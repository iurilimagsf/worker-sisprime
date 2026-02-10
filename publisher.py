"""
Módulo para publicação de mensagens no RabbitMQ.

Este módulo fornece funções para publicar mensagens na fila de processamento
de faturas SIFEN, permitindo que outras aplicações solicitem o processamento
de faturas sem depender do Django.
"""
import json
import logging

import pika

from .config import (
    MAIN_QUEUE,
    RABBITMQ_HOST,
    RABBITMQ_PASS,
    RABBITMQ_PORT,
    RABBITMQ_USER,
    SIFEN_ENCODING,
)

logger = logging.getLogger(__name__)


def _get_connection():
    """
    Cria uma conexão com o RabbitMQ.
    
    Returns:
        Conexão RabbitMQ
        
    Raises:
        pika.exceptions.AMQPConnectionError: Se não conseguir conectar
    """
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials
        )
    )


def processa_fatura(id_fatura: int):
    """
    Publica uma mensagem no RabbitMQ para agendar o processamento de uma fatura.
    
    Esta função se conecta ao RabbitMQ, garante que a fila de processamento
    exista e envia uma mensagem persistente contendo o ID da fatura
    que precisa ser processada pelo consumidor.
    
    Args:
        id_fatura: ID da fatura a ser processada
        
    Raises:
        pika.exceptions.AMQPConnectionError: Se não conseguir conectar ao RabbitMQ
        Exception: Se ocorrer erro inesperado
    """
    connection = None
    try:
        connection = _get_connection()
        
        with connection.channel() as channel:
            # Garante que a fila exista e seja durável
            channel.queue_declare(queue=MAIN_QUEUE, durable=True)
            
            # Monta a mensagem como um dicionário e converte para JSON
            mensagem_dict = {"id_fatura": id_fatura}
            mensagem_body = json.dumps(mensagem_dict)
            
            channel.basic_publish(
                exchange='',
                routing_key=MAIN_QUEUE,
                body=mensagem_body,
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                )
            )
            
            logger.info(f"[*] Fatura com ID {id_fatura} agendada para processamento.")
    
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"[!] Erro de conexão com o RabbitMQ: {e}")
        raise
    except Exception as e:
        logger.error(f"[!] Ocorreu um erro inesperado ao publicar a mensagem: {e}")
        raise
    finally:
        if connection and connection.is_open:
            connection.close()


def processa_cancelamento(id_fatura: int, motivo: str):
    """
    Publica uma mensagem no RabbitMQ solicitando o cancelamento de uma fatura.
    
    Args:
        id_fatura: ID da nota no banco de dados
        motivo: A justificativa do cancelamento (Obrigatório para o SIFEN)
        
    Raises:
        ValueError: Se o motivo não for válido
        pika.exceptions.AMQPConnectionError: Se não conseguir conectar ao RabbitMQ
        Exception: Se ocorrer erro inesperado
    """
    if not motivo or len(motivo) < 5:
        raise ValueError("O motivo do cancelamento é obrigatório e deve ter pelo menos 5 caracteres.")
    
    connection = None
    try:
        connection = _get_connection()
        
        with connection.channel() as channel:
            # A fila é A MESMA da emissão. O worker decide o que fazer baseada na 'acao'
            channel.queue_declare(queue=MAIN_QUEUE, durable=True)
            
            # Mensagem com ação de cancelamento
            mensagem_dict = {
                "id_fatura": id_fatura,
                "acao": "cancelar",  # Isso avisa o worker para ir pro handle_cancelar
                "motivo": motivo     # O SIFEN exige isso no XML de evento
            }
            
            mensagem_body = json.dumps(mensagem_dict)
            
            channel.basic_publish(
                exchange='',
                routing_key=MAIN_QUEUE,
                body=mensagem_body,
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                )
            )
            
            logger.info(f"[*] Solicitação de CANCELAMENTO para fatura ID {id_fatura} enviada.")
    
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"[!] Erro de conexão RabbitMQ no cancelamento: {e}")
        raise
    except Exception as e:
        logger.error(f"[!] Erro inesperado ao solicitar cancelamento: {e}")
        raise
    finally:
        if connection and connection.is_open:
            connection.close()


def processa_consulta(id_fatura: int):
    """
    Publica uma mensagem no RabbitMQ para reconsultar o status de uma fatura.
    
    Args:
        id_fatura: ID da nota no banco de dados
        
    Raises:
        pika.exceptions.AMQPConnectionError: Se não conseguir conectar ao RabbitMQ
        Exception: Se ocorrer erro inesperado
    """
    connection = None
    try:
        connection = _get_connection()
        
        with connection.channel() as channel:
            # A fila é A MESMA da emissão. O worker decide o que fazer baseada na 'acao'
            channel.queue_declare(queue=MAIN_QUEUE, durable=True)
            
            # Mensagem com ação de consulta
            mensagem_dict = {
                "id_fatura": id_fatura,
                "acao": "consultar",  # Isso avisa o worker para ir pro handle_consultar
                "tentativas": 1       # Reinicia contador de tentativas
            }
            
            mensagem_body = json.dumps(mensagem_dict)
            
            channel.basic_publish(
                exchange='',
                routing_key=MAIN_QUEUE,
                body=mensagem_body,
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                )
            )
            
            logger.info(f"[*] Solicitação de RECONSULTA para fatura ID {id_fatura} enviada.")
    
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"[!] Erro de conexão RabbitMQ na reconsulta: {e}")
        raise
    except Exception as e:
        logger.error(f"[!] Erro inesperado ao solicitar reconsulta: {e}")
        raise
    finally:
        if connection and connection.is_open:
            connection.close()
