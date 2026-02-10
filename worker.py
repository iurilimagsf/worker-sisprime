"""
Worker principal para processamento de mensagens SIFEN via RabbitMQ.

Este worker consome mensagens do RabbitMQ e processa faturas eletrônicas
conforme especificação SIFEN (Sistema de Facturación Electrónica Nacional).

Uso:
    python -m messaging_standalone.worker

Para adicionar novos tipos de nota:
1. Adicione a lógica específica em sifen_xml.py ou sifen_api.py
2. Crie um novo handler em handlers.py se necessário
3. Adicione a nova ação no roteador on_message_received()

Exemplo de mensagem RabbitMQ:
{
    "id_fatura": 123,
    "acao": "enviar",  # ou "consultar", "cancelar"
    "motivo": "Motivo do cancelamento"  # apenas para cancelar
}
"""
import logging
import signal
import sys

import pika

from .config import (
    DELAY_QUEUE,
    DELAY_ROUTING_KEY,
    DELAY_TTL_MS,
    DLX_EXCHANGE,
    MAIN_QUEUE,
    PREFETCH_COUNT,
    RABBITMQ_HOST,
    RABBITMQ_PASS,
    RABBITMQ_PORT,
    RABBITMQ_USER,
    validar_configuracoes,
)
from .database import get_connection
from .handlers import on_message_received

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Worker:
    """Worker para processamento de mensagens RabbitMQ."""
    
    def __init__(self):
        self.connection = None
        self.channel = None
        self.running = True
    
    def setup_signal_handlers(self):
        """Configura handlers para sinais de interrupção."""
        def signal_handler(sig, frame):
            logger.info("Sinal de interrupção recebido. Encerrando worker...")
            self.running = False
            if self.connection and self.connection.is_open:
                self.connection.close()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def connect_rabbitmq(self):
        """Conecta ao RabbitMQ e configura filas."""
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=credentials
                )
            )
            self.channel = self.connection.channel()
            
            # Configura exchange Dead Letter
            self.channel.exchange_declare(
                exchange=DLX_EXCHANGE,
                exchange_type='direct',
                durable=True
            )
            
            # Configura fila principal
            self.channel.queue_declare(queue=MAIN_QUEUE, durable=True)
            self.channel.queue_bind(
                queue=MAIN_QUEUE,
                exchange=DLX_EXCHANGE,
                routing_key=DELAY_ROUTING_KEY
            )
            
            # Configura fila de delay (TTL de 30 segundos)
            self.channel.queue_declare(
                queue=DELAY_QUEUE,
                durable=True,
                arguments={
                    'x-message-ttl': DELAY_TTL_MS,  # 30 segundos
                    'x-dead-letter-exchange': DLX_EXCHANGE,
                    'x-dead-letter-routing-key': DELAY_ROUTING_KEY
                }
            )
            
            # Configura QoS para processar uma mensagem por vez
            self.channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            
            logger.info("Conexão RabbitMQ estabelecida com sucesso")
            
        except Exception as e:
            logger.error(f"Erro ao conectar ao RabbitMQ: {e}")
            raise
    
    def start_consuming(self):
        """Inicia o consumo de mensagens."""
        logger.info(
            f'[*] Worker aguardando por faturas. '
            f'Para sair, pressione CTRL+C'
        )
        logger.info(
            f'[*] Delay configurado: {DELAY_TTL_MS / 1000}s '
            f'(TTL: {DELAY_TTL_MS}ms)'
        )
        
        # Inicia consumo
        self.channel.basic_consume(
            queue=MAIN_QUEUE,
            on_message_callback=on_message_received
        )
        
        # Inicia consumo (bloqueia até CTRL+C)
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Worker interrompido pelo usuário")
            self.stop()
    
    def stop(self):
        """Para o worker e fecha conexões."""
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()
        
        if self.connection and self.connection.is_open:
            self.connection.close()
            logger.info("Conexão com RabbitMQ fechada")
        
        # Fecha conexão Oracle
        db_conn = get_connection()
        db_conn.disconnect()
    
    def run(self):
        """Método principal que inicia o worker."""
        try:
            # Valida configurações
            validar_configuracoes()
            
            # Conecta ao banco de dados
            db_conn = get_connection()
            db_conn.connect()
            
            # Configura handlers de sinal
            self.setup_signal_handlers()
            
            # Conecta ao RabbitMQ
            self.connect_rabbitmq()
            
            # Inicia consumo
            self.start_consuming()
            
        except ValueError as e:
            logger.error(f'Erro de configuração: {e}')
            sys.exit(1)
        
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f'Não foi possível conectar ao RabbitMQ: {e}')
            sys.exit(1)
        
        except Exception as e:
            logger.critical(f'Erro inesperado: {e}', exc_info=True)
            sys.exit(1)
        
        finally:
            self.stop()


def main():
    """Função principal para iniciar o worker."""
    worker = Worker()
    worker.run()


if __name__ == '__main__':
    main()
