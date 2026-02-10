"""
Configurações e constantes para a aplicação standalone de mensageria SIFEN.

Todas as configurações são carregadas de variáveis de ambiente.
O arquivo .env é carregado automaticamente se existir.

Em ambiente Docker, as variáveis são injetadas via docker-compose (env_file ou environment).
O load_dotenv() não sobrescreve variáveis já definidas no ambiente.
"""
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env (se existir)
# 1. Tenta o caminho padrão (raiz do projeto, um nível acima deste pacote)
_env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=_env_path)

# 2. Fallback: busca automática no diretório atual e diretórios pais
#    Útil quando o .env está em outro local (ex: dentro do pacote em dev local)
load_dotenv()

# ==============================================================================
# CONFIGURAÇÕES DO BANCO DE DADOS ORACLE
# ==============================================================================

ORACLE_USER: Optional[str] = os.getenv('ORACLE_USER')
ORACLE_PASSWORD: Optional[str] = os.getenv('ORACLE_PASSWORD')
ORACLE_DSN: Optional[str] = os.getenv('ORACLE_DSN')  # Formato: host:port/service_name
ORACLE_CONNECTION_STRING: Optional[str] = os.getenv('ORACLE_CONNECTION_STRING')  # Alternativa: string completa

# ==============================================================================
# CONFIGURAÇÕES DO RABBITMQ
# ==============================================================================

RABBITMQ_HOST: str = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT: int = int(os.getenv('RABBITMQ_PORT', '5672'))
RABBITMQ_USER: Optional[str] = os.getenv('RABBITMQ_USER')
RABBITMQ_PASS: Optional[str] = os.getenv('RABBITMQ_PASS')
RABBITMQ_VHOST: str = os.getenv('RABBITMQ_VHOST', '/')

# ==============================================================================
# CONFIGURAÇÕES DO SIFEN
# ==============================================================================

URL_SIFEN_CONSULTA_LOTE: Optional[str] = os.getenv('URL_SIFEN_CONSULTA_LOTE')
URL_SIFEN_RECEBE_LOTE: Optional[str] = os.getenv('URL_SIFEN_RECEBE_LOTE')
URL_SIFEN_QR: Optional[str] = os.getenv('URL_SIFEN_QR')
URL_SIFEN_EVENTO: Optional[str] = os.getenv('URL_SIFEN_EVENTO')

# ==============================================================================
# CONSTANTES DE ENCODING E NAMESPACES
# ==============================================================================

SIFEN_ENCODING = 'utf-8'
"""Encoding padrão usado nas comunicações com SIFEN."""

SIFEN_NAMESPACES = {
    's': 'http://ekuatia.set.gov.py/sifen/xsd',
    'ds': 'http://www.w3.org/2000/09/xmldsig#'
}
"""Namespaces XML utilizados nos documentos SIFEN."""

# ==============================================================================
# CONFIGURAÇÕES DO RABBITMQ (FILAS E EXCHANGES)
# ==============================================================================

MAIN_QUEUE = 'faturas_para_processar'
"""Fila principal onde as mensagens são processadas."""

DELAY_QUEUE = 'faturas_wait_30s'
"""Fila de espera com TTL para agendar consultas após envio."""

DLX_EXCHANGE = 'faturas_dlx'
"""Exchange Dead Letter para redirecionar mensagens expiradas."""

DELAY_ROUTING_KEY = 'faturas_routing_key'
"""Routing key para redirecionamento de mensagens expiradas."""

DELAY_TTL_MS = int(os.getenv('DELAY_TTL_MS', '30000'))  # 30 segundos por padrão
"""Tempo de espera em milissegundos antes de consultar o status."""

# ==============================================================================
# CONFIGURAÇÕES DE PROCESSAMENTO
# ==============================================================================

MAX_TENTATIVAS_CONSULTA = 10
"""Número máximo de tentativas de consulta antes de desistir."""

PREFETCH_COUNT = 1
"""Número de mensagens não confirmadas que o worker pode receber simultaneamente."""

# ==============================================================================
# CÓDIGOS DE STATUS SIFEN
# ==============================================================================

CODIGO_STATUS_APROVADO = '0201'
"""Código padrão retornado quando documento é aprovado."""

CODIGO_STATUS_REJEITADO = '0300'
"""Código padrão retornado quando lote é rejeitado."""

CODIGO_STATUS_EXCEDEU_TENTATIVAS = '998'
"""Código usado quando excede o limite de tentativas de consulta."""

CODIGO_STATUS_ENVIADO = '900'
"""Código usado quando lote foi recebido e está aguardando consulta."""

CODIGO_STATUS_CANCELADO = '600'
"""Código usado quando nota é cancelada com sucesso."""

CODIGOS_SUCESSO_CANCELAMENTO = ['0500', '0501', '0600']
"""Códigos que indicam sucesso no cancelamento."""

# ==============================================================================
# CONFIGURAÇÕES DE VERSÃO SIFEN
# ==============================================================================

SIFEN_VERSION = '150'
"""Versão do formato SIFEN utilizado."""

# ==============================================================================
# VALIDAÇÃO DE CONFIGURAÇÕES
# ==============================================================================


def validar_configuracoes():
    """
    Valida se todas as configurações necessárias estão definidas.
    
    Raises:
        ValueError: Se alguma configuração obrigatória não estiver definida.
    """
    erros = []
    
    # Validação Oracle
    if not ORACLE_CONNECTION_STRING and not all([ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN]):
        erros.append("Configuração Oracle: forneça ORACLE_CONNECTION_STRING ou (ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN)")
    
    # Validação RabbitMQ
    if not RABBITMQ_USER or not RABBITMQ_PASS:
        erros.append("Configuração RabbitMQ: RABBITMQ_USER e RABBITMQ_PASS são obrigatórios")
    
    # Validação SIFEN
    urls_obrigatorias = {
        'URL_SIFEN_CONSULTA_LOTE': URL_SIFEN_CONSULTA_LOTE,
        'URL_SIFEN_RECEBE_LOTE': URL_SIFEN_RECEBE_LOTE,
        'URL_SIFEN_QR': URL_SIFEN_QR,
        'URL_SIFEN_EVENTO': URL_SIFEN_EVENTO,
    }
    
    urls_faltando = [
        nome for nome, url in urls_obrigatorias.items() if not url
    ]
    
    if urls_faltando:
        erros.append(
            f"URLs SIFEN não configuradas (variáveis de ambiente faltando): "
            f"{', '.join(urls_faltando)}"
        )
    
    if erros:
        raise ValueError("Erros de configuração:\n" + "\n".join(f"  - {erro}" for erro in erros))
