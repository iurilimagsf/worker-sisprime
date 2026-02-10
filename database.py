"""
Módulo de acesso ao banco de dados Oracle.

Fornece funções para conectar e executar queries diretamente no Oracle,
sem dependências do Django ORM.

Utiliza python-oracledb em modo thin (sem necessidade de Oracle Instant Client).
Para usar modo thick (com Oracle Client), descomente a linha oracledb.init_oracle_client()
na função connect().
"""
import logging
import oracledb
from typing import Optional, Dict, Any, List, Tuple
from contextlib import contextmanager

from .config import (
    ORACLE_USER,
    ORACLE_PASSWORD,
    ORACLE_DSN,
    ORACLE_CONNECTION_STRING
)

logger = logging.getLogger(__name__)


class OracleConnection:
    """Gerenciador de conexão Oracle."""
    
    def __init__(self):
        self._connection: Optional[oracledb.Connection] = None
    
    def connect(self):
        """Estabelece conexão com o Oracle usando thick mode (requer Oracle Instant Client)."""
        try:
            # Inicializa modo thick (necessário para password verifier SHA512)
            oracledb.init_oracle_client()
            
            if ORACLE_CONNECTION_STRING:
                # Usa string de conexão completa se fornecida
                # Formato: user/password@host:port/service_name
                self._connection = oracledb.connect(ORACLE_CONNECTION_STRING)
            elif ORACLE_USER and ORACLE_PASSWORD and ORACLE_DSN:
                # Monta string de conexão a partir dos componentes
                host = ORACLE_DSN.split(':')[0] if ':' in ORACLE_DSN else ORACLE_DSN
                port_str = ORACLE_DSN.split(':')[1].split('/')[0] if ':' in ORACLE_DSN else '1521'
                port = int(port_str) if port_str.isdigit() else 1521
                service = ORACLE_DSN.split('/')[-1] if '/' in ORACLE_DSN else ORACLE_DSN
                
                dsn = oracledb.makedsn(host, port, service_name=service)
                self._connection = oracledb.connect(
                    user=ORACLE_USER,
                    password=ORACLE_PASSWORD,
                    dsn=dsn
                )
            else:
                raise ValueError("Configuração Oracle não encontrada")
            
            self._connection.autocommit = True
            logger.info("Conexão Oracle estabelecida com sucesso (thick mode)")
            
        except Exception as e:
            logger.error(f"Erro ao conectar ao Oracle: {e}")
            raise
    
    def disconnect(self):
        """Fecha a conexão com o Oracle."""
        if self._connection:
            try:
                self._connection.close()
                logger.info("Conexão Oracle fechada")
            except Exception as e:
                logger.error(f"Erro ao fechar conexão Oracle: {e}")
            finally:
                self._connection = None
    
    @contextmanager
    def cursor(self):
        """Context manager para obter um cursor."""
        if not self._connection:
            self.connect()
        
        cursor = self._connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Tuple]:
        """
        Executa uma query SELECT e retorna os resultados.
        
        Args:
            query: Query SQL a ser executada
            params: Parâmetros para a query (opcional)
            
        Returns:
            Lista de tuplas com os resultados
        """
        with self.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
    
    def execute_one(self, query: str, params: Optional[Dict[str, Any]] = None) -> Optional[Tuple]:
        """
        Executa uma query SELECT e retorna apenas o primeiro resultado.
        
        Args:
            query: Query SQL a ser executada
            params: Parâmetros para a query (opcional)
            
        Returns:
            Tupla com o primeiro resultado ou None
        """
        results = self.execute_query(query, params)
        return results[0] if results else None
    
    def execute_update(self, query: str, params: Optional[Dict[str, Any]] = None) -> int:
        """
        Executa uma query UPDATE/INSERT/DELETE e retorna número de linhas afetadas.
        
        Args:
            query: Query SQL a ser executada
            params: Parâmetros para a query (opcional)
            
        Returns:
            Número de linhas afetadas
        """
        with self.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.rowcount
    
    def commit(self):
        """Faz commit da transação atual."""
        if self._connection:
            self._connection.commit()


# Instância global da conexão
_db_connection = OracleConnection()


def get_connection() -> OracleConnection:
    """Retorna a instância global da conexão Oracle."""
    return _db_connection


def get_tb_de_emissao(id_docfis: int) -> Optional[Dict[str, Any]]:
    """
    Busca um registro de TbDeEmissao pelo id_docfis.
    
    Retorna o último registro criado se houver múltiplos.
    
    Args:
        id_docfis: ID do documento fiscal
        
    Returns:
        Dicionário com os dados do registro ou None se não encontrado
    """
    query = """
        SELECT 
            id, id_docfis, XML, XML_RETORNO, tipo, cod_status, desc_status,
            caminho_certificado, senha, id_csc, csc, protocolo,
            XML_ASSINADO, XML_CANCELAMENTO_ENVIO, XML_CANCELAMENTO_RETORNO,
            TIPO_DOCTO
        FROM tb_de_emissao
        WHERE id_docfis = :id_docfis
        ORDER BY id DESC
    """
    
    result = get_connection().execute_one(query, {'id_docfis': id_docfis})
    
    if not result:
        return None
    
    return {
        'id': result[0],
        'id_docfis': result[1],
        'xml': result[2] or '',
        'xml_retorno': result[3] or '',
        'tipo': result[4] or '',
        'cod_status': result[5] or '',
        'desc_status': result[6] or '',
        'caminho_certificado': result[7] or '',
        'senha': result[8] or '',
        'id_csc': result[9] or '',
        'csc': result[10] or '',
        'protocolo': result[11] or '',
        'xml_assinado': result[12] or '',
        'xml_cancelamento_envio': result[13] or '',
        'xml_cancelamento_retorno': result[14] or '',
        'tipo_docto': result[15]
    }


def get_tb_de_documento(id_docfis: int) -> Optional[Dict[str, Any]]:
    """
    Busca um registro de TbDeDocumento pelo id.
    
    Args:
        id_docfis: ID do documento fiscal
        
    Returns:
        Dicionário com os dados do registro ou None se não encontrado
    """
    query = """
        SELECT 
            id_doc, cod_status, desc_status
        FROM tb_de_documento
        WHERE id_doc = :id_docfis
    """
    
    result = get_connection().execute_one(query, {'id_docfis': id_docfis})
    
    if not result:
        return None
    
    return {
        'id_doc': result[0],
        'cod_status': result[1],
        'desc_status': result[2]
    }


def update_tb_de_emissao(id_docfis: int, **kwargs) -> int:
    """
    Atualiza campos de TbDeEmissao.
    
    Args:
        id_docfis: ID do documento fiscal
        **kwargs: Campos a serem atualizados
        
    Returns:
        Número de linhas afetadas
    """
    if not kwargs:
        return 0
    
    # Monta a query dinamicamente
    set_clauses = []
    params = {'id_docfis': id_docfis}
    
    for key, value in kwargs.items():
        # Mapeia nomes de campos Python para nomes de colunas Oracle (maiúsculas)
        column_map = {
            'xml_assinado': 'XML_ASSINADO',
            'xml_retorno': 'XML_RETORNO',
            'xml_cancelamento_envio': 'XML_CANCELAMENTO_ENVIO',
            'xml_cancelamento_retorno': 'XML_CANCELAMENTO_RETORNO',
            'cod_status': 'cod_status',
            'desc_status': 'desc_status',
            'protocolo': 'protocolo'
        }
        
        column_name = column_map.get(key, key.upper())
        param_name = f"param_{key}"
        set_clauses.append(f"{column_name} = :{param_name}")
        params[param_name] = value
    
    query = f"""
        UPDATE tb_de_emissao
        SET {', '.join(set_clauses)}
        WHERE id_docfis = :id_docfis
    """
    
    return get_connection().execute_update(query, params)


def update_tb_de_documento(id_docfis: int, cod_status: Optional[int] = None, 
                          desc_status: Optional[str] = None) -> int:
    """
    Atualiza campos de TbDeDocumento.
    
    Args:
        id_docfis: ID do documento fiscal
        cod_status: Código de status (opcional)
        desc_status: Descrição de status (opcional)
        
    Returns:
        Número de linhas afetadas
    """
    updates = []
    params = {'id_docfis': id_docfis}
    
    if cod_status is not None:
        updates.append("cod_status = :cod_status")
        params['cod_status'] = cod_status
    
    if desc_status is not None:
        updates.append("desc_status = :desc_status")
        params['desc_status'] = desc_status
    
    if not updates:
        return 0
    
    query = f"""
        UPDATE tb_de_documento
        SET {', '.join(updates)}
        WHERE id_doc = :id_docfis
    """
    
    return get_connection().execute_update(query, params)
