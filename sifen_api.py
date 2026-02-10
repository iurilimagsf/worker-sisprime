"""
Módulo de comunicação com a API do SIFEN.

Este módulo contém todas as funções de comunicação HTTP/SOAP com o SIFEN:
- Envio de lotes
- Consulta de status
- Envio de eventos (cancelamento)

Todas as requisições são feitas via SOAP com autenticação por certificado.
"""
import logging
import os
import tempfile
import time

import requests
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    pkcs12,
)
from lxml import etree

from .config import (
    SIFEN_ENCODING,
    URL_SIFEN_CONSULTA_LOTE,
    URL_SIFEN_EVENTO,
    URL_SIFEN_RECEBE_LOTE,
)

logger = logging.getLogger(__name__)


# ==============================================================================
# FUNÇÕES AUXILIARES DE REQUISIÇÃO
# ==============================================================================


def _make_sifen_request(
    url: str,
    data: str,
    cert_path: str,
    cert_pass: str
) -> str:
    """
    Faz uma requisição SOAP para o SIFEN usando certificado para autenticação.
    
    Esta função é usada internamente por todas as outras funções de API.
    Ela converte o certificado PFX para PEM temporário, faz a requisição
    e limpa os arquivos temporários.
    
    Args:
        url: URL do endpoint SIFEN
        data: Conteúdo SOAP a ser enviado
        cert_path: Caminho do certificado PFX
        cert_pass: Senha do certificado
        
    Returns:
        Resposta do servidor como string
        
    Raises:
        requests.HTTPError: Se a requisição falhar
        ValueError: Se não conseguir carregar o certificado
        
    Note:
        Os arquivos PEM temporários são sempre removidos, mesmo em caso de erro.
    """
    headers = {'Content-Type': 'application/soap+xml;charset=UTF-8'}
    cert_file_path, key_file_path = None, None
    
    try:
        # Carrega e converte o certificado PFX
        with open(cert_path, 'rb') as f_pfx:
            pfx_data = f_pfx.read()
        
        private_key, certificate, _ = pkcs12.load_key_and_certificates(
            pfx_data, cert_pass.encode(SIFEN_ENCODING)
        )
        
        key_pem = private_key.private_bytes(
            encoding=Encoding.PEM,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption()
        )
        cert_pem = certificate.public_bytes(Encoding.PEM)
        
        # Cria arquivos temporários PEM
        with tempfile.NamedTemporaryFile(
            delete=False, suffix='.pem', mode='wb'
        ) as cert_file, tempfile.NamedTemporaryFile(
            delete=False, suffix='.pem', mode='wb'
        ) as key_file:
            cert_file.write(cert_pem)
            key_file.write(key_pem)
            cert_file_path = cert_file.name
            key_file_path = key_file.name
        
        logger.info(f"Enviando para SIFEN (URL: {url})")
        response = requests.post(
            url,
            data=data.encode(SIFEN_ENCODING),
            headers=headers,
            cert=(cert_file_path, key_file_path)
        )
        
        # O SIFEN pode retornar 400 (Bad Request) mas ainda assim incluir
        # um XML válido no corpo com informações sobre o erro ou status.
        # Por isso, verificamos se há conteúdo XML válido antes de lançar exceção.
        response_text = response.text
        
        # Verifica se a resposta contém XML válido (mesmo com status 400)
        tem_xml_valido = (
            response_text and
            ('<?xml' in response_text or '<env:Envelope' in response_text or
             '<soap:Envelope' in response_text or '<Envelope' in response_text)
        )
        
        if not response.ok:
            if tem_xml_valido:
                # Status 400 mas com XML válido - retorna o XML para processamento
                # O handler decidirá o que fazer com base no conteúdo do XML
                logger.warning(
                    f"SIFEN retornou status {response.status_code} mas com XML válido. "
                    f"Processando resposta: {response_text[:200]}..."
                )
                return response_text
            else:
                # Status de erro sem XML válido - loga e lança exceção
                logger.error(
                    f"Erro na requisição para SIFEN. "
                    f"Status Code: {response.status_code}, "
                    f"Response: {response_text}"
                )
                response.raise_for_status()
        
        return response_text
    
    finally:
        # Sempre limpa os arquivos temporários
        if cert_file_path and os.path.exists(cert_file_path):
            os.unlink(cert_file_path)
        if key_file_path and os.path.exists(key_file_path):
            os.unlink(key_file_path)


# ==============================================================================
# FUNÇÕES DE ENVIO E CONSULTA
# ==============================================================================


def enviar_lote_sifen(
    payload_base64: str,
    cert_path: str,
    cert_pass: str
) -> str:
    """
    Envia um lote de documentos para o SIFEN.
    
    O lote deve estar comprimido em ZIP e codificado em Base64.
    Esta função cria o envelope SOAP apropriado e envia para o endpoint.
    
    Args:
        payload_base64: ZIP comprimido em Base64 contendo o(s) XML(s)
        cert_path: Caminho do certificado PFX
        cert_pass: Senha do certificado
        
    Returns:
        Resposta SOAP do SIFEN contendo o protocolo de recebimento
    """
    id_requisicao = str(int(time.time() * 1000))
    envelope_soap = (
        f'<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" '
        f'xmlns:xsd="http://ekuatia.set.gov.py/sifen/xsd">'
        f'<soap:Header/>'
        f'<soap:Body>'
        f'<xsd:rEnvioLote>'
        f'<xsd:dId>{id_requisicao}</xsd:dId>'
        f'<xsd:xDE>{payload_base64}</xsd:xDE>'
        f'</xsd:rEnvioLote>'
        f'</soap:Body>'
        f'</soap:Envelope>'
    )
    
    return _make_sifen_request(
        URL_SIFEN_RECEBE_LOTE, envelope_soap, cert_path, cert_pass
    )


def consultar_lote_sifen(
    protocolo: str,
    cert_path: str,
    cert_pass: str
) -> str:
    """
    Consulta o status de um lote enviado anteriormente.
    
    Usa o protocolo retornado pelo envio para consultar o status.
    Esta função deve ser chamada após um delay (normalmente 30 segundos)
    para dar tempo ao SIFEN processar o lote.
    
    Args:
        protocolo: Protocolo retornado pelo envio do lote
        cert_path: Caminho do certificado PFX
        cert_pass: Senha do certificado
        
    Returns:
        Resposta SOAP do SIFEN contendo o status do lote
    """
    id_requisicao = str(int(time.time() * 1000))
    envelope_soap = (
        f'<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" '
        f'xmlns:xsd="http://ekuatia.set.gov.py/sifen/xsd">'
        f'<soap:Header/>'
        f'<soap:Body>'
        f'<xsd:rEnviConsLoteDe>'
        f'<xsd:dId>{id_requisicao}</xsd:dId>'
        f'<xsd:dProtConsLote>{protocolo}</xsd:dProtConsLote>'
        f'</xsd:rEnviConsLoteDe>'
        f'</soap:Body>'
        f'</soap:Envelope>'
    )
    
    logger.debug(f"Envelope SOAP (Consulta Lote): {envelope_soap}")
    
    return _make_sifen_request(
        URL_SIFEN_CONSULTA_LOTE, envelope_soap, cert_path, cert_pass
    )


# ==============================================================================
# FUNÇÕES DE EVENTOS
# ==============================================================================


def enviar_evento_cancelamento(
    xml_conteudo: str,
    id_evento: str,
    cert_path: str,
    cert_pass: str
) -> str:
    """
    Envia um evento de cancelamento para o SIFEN.
    
    O XML do evento deve estar já assinado e formatado conforme
    especificação WSDL. Esta função apenas cria o envelope SOAP
    e envia para o endpoint de eventos.
    
    Args:
        xml_conteudo: XML do evento de cancelamento já assinado
        id_evento: ID do evento (usado apenas para logs)
        cert_path: Caminho do certificado PFX
        cert_pass: Senha do certificado
        
    Returns:
        Resposta SOAP do SIFEN contendo o resultado do cancelamento
        
    Note:
        O XML deve estar no formato gGroupGesEve conforme especificação
        SIFEN v1.50. Use gerar_evento_assinado_wsdl() para gerar o XML.
    """
    id_requisicao = str(int(time.time() * 1000))
    
    # Limpeza de segurança - remove header XML se existir
    xml_limpo = xml_conteudo.replace(
        "<?xml version='1.0' encoding='utf-8'?>", ""
    ).strip()
    
    # Envelope Montado Manualmente (String Interpolation) para precisão absoluta
    # Note o xmlns na tag rEnviEventoDe - isso define o namespace padrão
    envelope_soap = f"""<?xml version="1.0" encoding="utf-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
    <env:Body>
        <rEnviEventoDe xmlns="http://ekuatia.set.gov.py/sifen/xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <dId>{id_requisicao}</dId>
            <dEvReg>
                {xml_limpo}
            </dEvReg>
        </rEnviEventoDe>
    </env:Body>
</env:Envelope>"""
    
    logger.debug(f"Envelope SOAP (Strict Match): {envelope_soap}")
    
    # IMPORTANTE: URL sem ?WSDL no final
    return _make_sifen_request(
        URL_SIFEN_EVENTO, envelope_soap, cert_path, cert_pass
    )
