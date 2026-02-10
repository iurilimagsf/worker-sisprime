"""
Módulo de handlers para processamento de mensagens do RabbitMQ.

Este módulo contém toda a lógica de processamento das ações:
- Enviar: Assina, gera QR e envia lote ao SIFEN
- Consultar: Consulta status de lote já enviado
- Cancelar: Cancela uma nota já aprovada

A estrutura modular facilita a adição de novos tipos de ação ou
novos tipos de nota fiscal.
"""
import json
import logging

import pika
from lxml import etree

from .database import (
    get_tb_de_emissao,
    get_tb_de_documento,
    update_tb_de_emissao,
    update_tb_de_documento,
)
from .sifen_api import (
    consultar_lote_sifen,
    enviar_evento_cancelamento,
    enviar_lote_sifen,
)
from .sifen_xml import (
    assinar_e_gerar_qr,
    extrair_cdc_do_xml,
    gerar_evento_assinado_wsdl,
    preparar_payload_sifen,
)
from .config import (
    CODIGO_STATUS_APROVADO,
    CODIGO_STATUS_CANCELADO,
    CODIGO_STATUS_ENVIADO,
    CODIGO_STATUS_EXCEDEU_TENTATIVAS,
    CODIGO_STATUS_REJEITADO,
    CODIGOS_SUCESSO_CANCELAMENTO,
    DELAY_QUEUE,
    DELAY_ROUTING_KEY,
    DLX_EXCHANGE,
    MAX_TENTATIVAS_CONSULTA,
    SIFEN_ENCODING,
)

logger = logging.getLogger(__name__)


# ==============================================================================
# FUNÇÕES AUXILIARES DE AGENDAMENTO
# ==============================================================================


def agendar_consulta(channel: pika.channel.Channel, id_fatura: int, dados_originais: dict):
    """
    Agenda uma consulta de status para ser executada após delay.
    
    Publica uma mensagem na fila de espera (delay queue) que expira
    após o TTL (30 segundos) e é redirecionada para a fila principal.
    Isso garante que o SIFEN tenha tempo de processar o lote antes
    da consulta.
    
    Args:
        channel: Canal RabbitMQ ativo
        id_fatura: ID da fatura a ser consultada
        dados_originais: Dados originais da mensagem (incluindo tentativas)
    """
    tentativas = dados_originais.get('tentativas', 1)
    mensagem_consulta = {
        "id_fatura": id_fatura,
        "acao": "consultar",
        "tentativas": tentativas
    }
    
    channel.basic_publish(
        exchange='',
        routing_key=DELAY_QUEUE,
        body=json.dumps(mensagem_consulta),
        properties=pika.BasicProperties(delivery_mode=2)  # Persistente
    )
    
    logger.info(
        f"[*] Consulta para fatura ID {id_fatura} agendada via DLX "
        f"(Tentativa {tentativas}). A mensagem reaparecerá em 30s."
    )


# ==============================================================================
# HANDLERS DE AÇÕES
# ==============================================================================


def handle_enviar(
    ch: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    emissao: dict,
    documento: dict
):
    """
    Processa o envio de uma nova fatura ao SIFEN.
    
    Fluxo completo:
    1. Assina o XML e gera QR Code
    2. Remove header XML e cria rLoteDE
    3. Comprime em ZIP/Base64
    4. Envia para SIFEN
    5. Processa retorno e agenda consulta se sucesso
    
    Args:
        ch: Canal RabbitMQ
        method: Método de entrega da mensagem
        emissao: Dicionário com dados da fatura (TbDeEmissao)
        documento: Dicionário com dados do documento (TbDeDocumento)
    """
    id_docfis = emissao['id_docfis']
    logger.info(f"[*] Iniciando envio de fatura ID {id_docfis}")
    
    try:
        # 1. Assina e gera QR
        xml_assinado_com_qr = assinar_e_gerar_qr(
            emissao['xml'],
            emissao['caminho_certificado'],
            emissao['senha'],
            emissao['csc'],
            emissao['id_csc']
        )
        
        # 2. Remove header XML de forma consistente
        xml_assinado_sem_header = xml_assinado_com_qr.replace(
            '<?xml version=\'1.0\' encoding=\'utf-8\'?>', ''
        ).strip()
        
        # 3. Cria rLoteDE (wrapper necessário para envio)
        xml_final = f"<rLoteDE>{xml_assinado_sem_header}</rLoteDE>"
        
        # 4. Comprime em ZIP/Base64
        payload_b64 = preparar_payload_sifen(xml_final)
        
        logger.info(f"[*] Enviando payload para fatura ID {id_docfis}")
        
        # 5. Envia para SIFEN
        retorno_sifen = enviar_lote_sifen(
            payload_b64,
            emissao['caminho_certificado'],
            emissao['senha']
        )
        retorno_root = etree.fromstring(retorno_sifen.encode(SIFEN_ENCODING))
        logger.info(f"Retorno SIFEN (Envio): {retorno_sifen}")
        
        # 6. Extrai protocolo do retorno - tenta múltiplos caminhos
        protocolo = (
            retorno_root.findtext('.//{*}dProtConsLote') or
            retorno_root.findtext('.//dProtConsLote') or
            ""
        )
        
        if not protocolo or protocolo == '0':
            # Falha no envio - tenta extrair informações de erro
            msg_res = (
                retorno_root.findtext('.//{*}dMsgRes') or
                retorno_root.findtext('.//dMsgRes') or
                'Erro não especificado'
            )
            
            codigo_res = (
                retorno_root.findtext('.//{*}dCodRes') or
                retorno_root.findtext('.//dCodRes') or
                '999'
            )
            
            logger.error(
                f"Falha ao enviar lote para fatura ID {id_docfis}. "
                f"Protocolo inválido ('{protocolo}'). "
                f"Código: {codigo_res}, Motivo: {msg_res}"
            )
            
            # Atualiza no banco
            update_tb_de_emissao(
                id_docfis,
                xml_assinado=xml_final,
                xml_retorno=retorno_sifen,
                cod_status=codigo_res,
                desc_status=f"Falha no envio: {msg_res}"
            )
            update_tb_de_documento(
                id_docfis,
                cod_status=int(codigo_res) if codigo_res.isdigit() else None,
                desc_status=f"Falha no envio: {msg_res}"
            )
        
        else:
            # Sucesso - agenda consulta
            logger.info(
                f"Fatura ID {id_docfis} enviada. Protocolo: {protocolo}. "
                f"Agendando consulta."
            )
            
            # Atualiza no banco
            update_tb_de_emissao(
                id_docfis,
                xml_assinado=xml_final,
                xml_retorno=retorno_sifen,
                protocolo=protocolo,
                cod_status=CODIGO_STATUS_ENVIADO,
                desc_status="Lote recebido. Aguardando consulta de status."
            )
            update_tb_de_documento(
                id_docfis,
                cod_status=int(CODIGO_STATUS_ENVIADO),
                desc_status="Lote recebido. Aguardando consulta de status."
            )
            
            agendar_consulta(ch, id_docfis, {"tentativas": 1})
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        logger.error(
            f"Erro ao processar envio da fatura ID {id_docfis}: {e}",
            exc_info=True
        )
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_consultar(
    ch: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    emissao: dict,
    documento: dict,
    original_body: bytes
):
    """
    Processa a consulta de status de um lote já enviado.
    
    Verifica se o lote foi aprovado, rejeitado ou ainda está em processamento.
    Se ainda estiver processando, agenda nova consulta (até limite de tentativas).
    
    Args:
        ch: Canal RabbitMQ
        method: Método de entrega da mensagem
        emissao: Dicionário com dados da fatura (TbDeEmissao)
        documento: Dicionário com dados do documento (TbDeDocumento)
        original_body: Corpo original da mensagem (para manter tentativas)
    """
    id_docfis = emissao['id_docfis']
    logger.info(f"[*] Consultando status da fatura ID {id_docfis}")
    
    try:
        # Consulta o status no SIFEN
        retorno_consulta = consultar_lote_sifen(
            emissao['protocolo'],
            emissao['caminho_certificado'],
            emissao['senha']
        )
        retorno_root = etree.fromstring(retorno_consulta.encode(SIFEN_ENCODING))
        logger.info(f"Retorno SIFEN (Consulta): {retorno_consulta}")
        
        # Extrai informações do retorno - tenta múltiplos caminhos possíveis
        status_documento = (
            retorno_root.findtext('.//{*}dEstRes') or
            retorno_root.findtext('.//dEstRes') or
            ""
        ).strip()
        
        msg_lote = (
            retorno_root.findtext('.//{*}dMsgResLot') or
            retorno_root.findtext('.//dMsgResLot') or
            ""
        ).strip()
        
        msg_res = (
            retorno_root.findtext('.//{*}dMsgRes') or
            retorno_root.findtext('.//dMsgRes') or
            ""
        ).strip()
        
        if not msg_lote and msg_res:
            msg_lote = msg_res
        
        # Extrai código de resposta para verificar erro 0160
        codigo_resposta = (
            retorno_root.findtext('.//{*}dCodRes') or
            retorno_root.findtext('.//{*}dCodResLot') or
            retorno_root.findtext('.//dCodRes') or
            retorno_root.findtext('.//dCodResLot') or
            ""
        ).strip()
        
        # Verifica se é erro 0160 "XML Mal Formado." - tenta reconsultar
        if codigo_resposta == "0160" and msg_lote.strip() == "XML Mal Formado.":
            logger.warning(
                f"Fatura ID {id_docfis} retornou erro 0160 (XML Mal Formado). "
                f"Reagendando consulta para evitar erro do SIFEN."
            )
            
            dados = json.loads(original_body.decode(SIFEN_ENCODING))
            tentativas = dados.get('tentativas', 1)
            
            if tentativas < MAX_TENTATIVAS_CONSULTA:
                dados['tentativas'] = tentativas + 1
                agendar_consulta(ch, id_docfis, dados)
                logger.info(
                    f"Consulta reagendada para fatura ID {id_docfis} "
                    f"(tentativa {tentativas + 1})."
                )
                
                update_tb_de_emissao(
                    id_docfis,
                    xml_retorno=retorno_consulta,
                    cod_status="900",
                    desc_status="Reprocessando consulta"
                )
                update_tb_de_documento(
                    id_docfis,
                    cod_status=900,
                    desc_status="Reprocessando consulta"
                )
            else:
                logger.error(
                    f"Fatura ID {id_docfis} excedeu o limite de "
                    f"tentativas mesmo com erro 0160 ({MAX_TENTATIVAS_CONSULTA})."
                )
                update_tb_de_emissao(
                    id_docfis,
                    xml_retorno=retorno_consulta,
                    cod_status=CODIGO_STATUS_EXCEDEU_TENTATIVAS,
                    desc_status="Excedeu o limite de tentativas de consulta (erro 0160)."
                )
                update_tb_de_documento(
                    id_docfis,
                    cod_status=int(CODIGO_STATUS_EXCEDEU_TENTATIVAS),
                    desc_status="Excedeu o limite de tentativas de consulta (erro 0160)."
                )
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        
        # Determina status
        is_aprovado = status_documento == "Aprobado"
        is_rejeitado = (
            status_documento == "Rechazado" or
            "Cancelado" in msg_lote or
            "Rechazado" in msg_lote
        )
        
        if is_aprovado:
            # Documento aprovado com sucesso
            logger.info(f"Fatura ID {id_docfis} APROVADA.")
            cod_status = retorno_root.findtext(
                './/{*}dCodRes', CODIGO_STATUS_APROVADO
            )
            
            update_tb_de_emissao(
                id_docfis,
                xml_retorno=retorno_consulta,
                cod_status=cod_status,
                desc_status="Aprobado exitosamente."
            )
            update_tb_de_documento(
                id_docfis,
                cod_status=int(cod_status) if cod_status.isdigit() else int(CODIGO_STATUS_APROVADO),
                desc_status="Aprobado exitosamente."
            )
        
        elif is_rejeitado:
            # Documento rejeitado
            codigo_rejeicao = (
                retorno_root.findtext('.//{*}dCodRes') or
                retorno_root.findtext('.//{*}dCodResLot') or
                retorno_root.findtext('.//dCodRes') or
                retorno_root.findtext('.//dCodResLot') or
                CODIGO_STATUS_REJEITADO
            )
            
            motivo_rejeicao = (
                msg_lote or
                msg_res or
                retorno_root.findtext('.//{*}dMsgRes') or
                retorno_root.findtext('.//dMsgRes') or
                'Motivo não especificado.'
            )
            
            logger.warning(
                f"Fatura ID {id_docfis} REJEITADA/CANCELADA. "
                f"Código: {codigo_rejeicao}, Motivo: {motivo_rejeicao}"
            )
            
            update_tb_de_emissao(
                id_docfis,
                xml_retorno=retorno_consulta,
                cod_status=codigo_rejeicao,
                desc_status=f"Rejeitado: {motivo_rejeicao}"
            )
            update_tb_de_documento(
                id_docfis,
                cod_status=int(codigo_rejeicao) if codigo_rejeicao.isdigit() else int(CODIGO_STATUS_REJEITADO),
                desc_status=f"Rejeitado: {motivo_rejeicao}"
            )
        
        else:
            # Lote ainda em processamento ou status desconhecido
            dados = json.loads(original_body.decode(SIFEN_ENCODING))
            tentativas = dados.get('tentativas', 1)
            
            if tentativas < MAX_TENTATIVAS_CONSULTA:
                logger.info(
                    f"Fatura ID {id_docfis} ainda em processamento. "
                    f"Reagendando (tentativa {tentativas + 1})."
                )
                dados['tentativas'] = tentativas + 1
                agendar_consulta(ch, id_docfis, dados)
            else:
                logger.error(
                    f"Fatura ID {id_docfis} excedeu o limite de "
                    f"tentativas de consulta ({MAX_TENTATIVAS_CONSULTA})."
                )
                update_tb_de_emissao(
                    id_docfis,
                    xml_retorno=retorno_consulta,
                    cod_status=CODIGO_STATUS_EXCEDEU_TENTATIVAS,
                    desc_status="Excedeu o limite de tentativas de consulta."
                )
                update_tb_de_documento(
                    id_docfis,
                    cod_status=int(CODIGO_STATUS_EXCEDEU_TENTATIVAS),
                    desc_status="Excedeu o limite de tentativas de consulta."
                )
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        logger.error(
            f"Erro ao consultar status da fatura ID {id_docfis}: {e}",
            exc_info=True
        )
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_cancelar(
    ch: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    emissao: dict,
    documento: dict,
    dados: dict
):
    """
    Processa o cancelamento de uma fatura já aprovada.
    
    Fluxo completo:
    1. Extrai CDC do XML assinado
    2. Gera XML de evento de cancelamento assinado
    3. Envia para SIFEN
    4. Processa retorno e atualiza status
    
    Args:
        ch: Canal RabbitMQ
        method: Método de entrega da mensagem
        emissao: Dicionário com dados da fatura (TbDeEmissao)
        documento: Dicionário com dados do documento (TbDeDocumento)
        dados: Dados da mensagem (incluindo motivo do cancelamento)
    """
    id_docfis = emissao['id_docfis']
    logger.info(f"--- Iniciando Cancelamento WSDL ID {id_docfis} ---")
    
    try:
        # 1. Recupera CDC do XML assinado
        cdc = extrair_cdc_do_xml(emissao['xml_assinado'])
        if not cdc:
            logger.error(
                f"CDC não encontrado para cancelar ID {id_docfis}"
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        
        motivo = dados.get('motivo', 'Solicitud de cancelacion')
        
        # 2. Gera XML de evento assinado
        try:
            xml_evento_assinado = gerar_evento_assinado_wsdl(
                cdc,
                motivo,
                emissao['caminho_certificado'],
                emissao['senha']
            )
        except Exception as e:
            logger.error(
                f"Erro ao gerar/assinar estrutura de cancelamento: {e}",
                exc_info=True
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        
        # 3. Envia para o SIFEN
        try:
            retorno = enviar_evento_cancelamento(
                xml_evento_assinado,
                "WSDL-GEN",
                emissao['caminho_certificado'],
                emissao['senha']
            )
        except Exception as e:
            logger.error(f"Erro envio evento: {e}", exc_info=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        
        # 4. Processa Retorno
        logger.info(f"Retorno Cancelamento: {retorno}")
        
        try:
            root_ret = etree.fromstring(retorno.encode(SIFEN_ENCODING))
            cod_res = root_ret.findtext('.//{*}dCodRes')
            msg_res = root_ret.findtext('.//{*}dMsgRes') or "Sem mensagem"
            est_res = root_ret.findtext('.//{*}dEstRes') or ""
        except Exception as e:
            logger.error(f"Erro ao parsear retorno: {e}", exc_info=True)
            cod_res = "ERRO_PARSE"
            msg_res = "Erro ao ler XML de retorno"
            est_res = ""
        
        # Verifica se cancelamento foi bem-sucedido
        if cod_res in CODIGOS_SUCESSO_CANCELAMENTO:
            logger.info(
                f"✅ Cancelamento Homologado! "
                f"Protocolo: {root_ret.findtext('.//{*}dProtAut')}"
            )
            update_tb_de_emissao(
                id_docfis,
                xml_cancelamento_envio=xml_evento_assinado,
                xml_cancelamento_retorno=retorno,
                cod_status=cod_res,
                desc_status=f"Cancelado: {msg_res}"
            )
            update_tb_de_documento(
                id_docfis,
                cod_status=int(CODIGO_STATUS_CANCELADO),
                desc_status="Nota Cancelada"
            )
        
        # Validação extra pelo Status Textual
        elif est_res == "Aprobado":
            logger.info(
                f"✅ Cancelamento Homologado (Via Status)! "
                f"Protocolo: {root_ret.findtext('.//{*}dProtAut')}"
            )
            update_tb_de_emissao(
                id_docfis,
                xml_cancelamento_envio=xml_evento_assinado,
                xml_cancelamento_retorno=retorno,
                cod_status=cod_res,
                desc_status=f"Cancelado: {msg_res}"
            )
            update_tb_de_documento(
                id_docfis,
                cod_status=int(CODIGO_STATUS_CANCELADO),
                desc_status="Nota Cancelada"
            )
        
        else:
            logger.warning(
                f"Cancelamento não foi aprovado. "
                f"Código: {cod_res}, Mensagem: {msg_res}"
            )
            update_tb_de_emissao(
                id_docfis,
                xml_cancelamento_envio=xml_evento_assinado,
                xml_cancelamento_retorno=retorno,
                cod_status=cod_res,
                desc_status=f"Erro no cancelamento: {msg_res}"
            )
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        logger.error(
            f"Erro não tratado no cancelamento ID {id_docfis}: {e}",
            exc_info=True
        )
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


# ==============================================================================
# ROTEADOR DE MENSAGENS
# ==============================================================================


def on_message_received(
    ch: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.spec.BasicProperties,
    body: bytes
):
    """
    Callback principal para processamento de mensagens do RabbitMQ.
    
    Esta função roteia as mensagens para os handlers apropriados baseado
    na ação especificada. Ela também faz validações básicas e tratamento
    de erros genérico.
    
    Args:
        ch: Canal RabbitMQ
        method: Método de entrega da mensagem
        properties: Propriedades da mensagem
        body: Corpo da mensagem (JSON)
    """
    id_fatura = None
    
    try:
        # Parse da mensagem
        dados = json.loads(body.decode(SIFEN_ENCODING))
        id_fatura = dados.get('id_fatura')
        acao = dados.get('acao', 'enviar').lower()  # Normaliza para minúsculo
        
        if not id_fatura:
            logger.warning("Mensagem recebida sem id_fatura. Ignorando.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        
        # Busca dados no banco
        emissao = get_tb_de_emissao(id_fatura)
        documento = get_tb_de_documento(id_fatura)
        
        if not emissao or not documento:
            logger.error(
                f"Dados não encontrados no banco ID {id_fatura}"
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        
        logger.info(f"[*] Processando ID {id_fatura} | Ação: {acao.upper()}")
        
        # Roteia para o handler apropriado
        if acao == 'enviar':
            handle_enviar(ch, method, emissao, documento)
        elif acao == 'consultar':
            handle_consultar(ch, method, emissao, documento, body)
        elif acao == 'cancelar':
            handle_cancelar(ch, method, emissao, documento, dados)
        else:
            logger.warning(f"Ação desconhecida: {acao}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    except Exception as e:
        logger.critical(
            f"Erro não tratado na fila (ID: {id_fatura}): {e}",
            exc_info=True
        )
        # Nack com false para não entrar em loop infinito de erro
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
