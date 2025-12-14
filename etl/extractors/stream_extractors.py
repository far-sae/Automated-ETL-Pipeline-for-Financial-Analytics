"""
Stream-based extractors for Kafka, RabbitMQ, and WebSocket
"""
from typing import Dict, Any, Optional, List
import json
from datetime import datetime, timedelta
import pandas as pd
from kafka import KafkaConsumer
import pika
import asyncio
import websockets
from etl.extractors.base import BaseExtractor
from etl.logger import get_logger

logger = get_logger(__name__)


class KafkaExtractor(BaseExtractor):
    """Extract data from Kafka topics"""
    
    def __init__(self, bootstrap_servers: List[str], config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("kafka_stream", config_dict)
        self.bootstrap_servers = bootstrap_servers
    
    def extract(self, topic: str, consumer_group: str = "etl_consumer",
                max_messages: int = 1000, timeout_seconds: int = 60, **kwargs) -> pd.DataFrame:
        """
        Extract messages from Kafka topic
        
        Args:
            topic: Kafka topic name
            consumer_group: Consumer group ID
            max_messages: Maximum number of messages to consume
            timeout_seconds: Timeout for message consumption
        """
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=consumer_group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        
        messages = []
        start_time = datetime.now()
        
        try:
            for message in consumer:
                messages.append(message.value)
                
                if len(messages) >= max_messages:
                    break
                
                if (datetime.now() - start_time).seconds > timeout_seconds:
                    break
            
            df = pd.DataFrame(messages)
            logger.info("kafka_extraction_successful", topic=topic, messages=len(df))
            return df
            
        except Exception as e:
            logger.error("kafka_extraction_failed", topic=topic, error=str(e))
            raise
        finally:
            consumer.close()


class RabbitMQExtractor(BaseExtractor):
    """Extract data from RabbitMQ queues"""
    
    def __init__(self, host: str, port: int = 5672, config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("rabbitmq_queue", config_dict)
        self.host = host
        self.port = port
    
    def extract(self, queue_name: str, max_messages: int = 1000,
                username: str = 'guest', password: str = 'guest', **kwargs) -> pd.DataFrame:
        """
        Extract messages from RabbitMQ queue
        
        Args:
            queue_name: Queue name
            max_messages: Maximum messages to consume
            username: RabbitMQ username
            password: RabbitMQ password
        """
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.port, credentials=credentials)
        )
        channel = connection.channel()
        
        messages = []
        
        try:
            for _ in range(max_messages):
                method_frame, header_frame, body = channel.basic_get(queue_name)
                
                if method_frame:
                    try:
                        message_data = json.loads(body)
                        messages.append(message_data)
                        channel.basic_ack(method_frame.delivery_tag)
                    except:
                        messages.append({'raw_message': body.decode('utf-8')})
                        channel.basic_ack(method_frame.delivery_tag)
                else:
                    break
            
            df = pd.DataFrame(messages)
            logger.info("rabbitmq_extraction_successful", queue=queue_name, messages=len(df))
            return df
            
        except Exception as e:
            logger.error("rabbitmq_extraction_failed", queue=queue_name, error=str(e))
            raise
        finally:
            connection.close()


class WebSocketExtractor(BaseExtractor):
    """Extract data from WebSocket streams"""
    
    def __init__(self, ws_url: str, config_dict: Optional[Dict[str, Any]] = None):
        super().__init__("websocket_stream", config_dict)
        self.ws_url = ws_url
    
    def extract(self, max_messages: int = 100, timeout_seconds: int = 60, **kwargs) -> pd.DataFrame:
        """
        Extract messages from WebSocket
        
        Args:
            max_messages: Maximum messages to receive
            timeout_seconds: Connection timeout
        """
        messages = []
        
        async def receive_messages():
            async with websockets.connect(self.ws_url) as websocket:
                start_time = datetime.now()
                
                while len(messages) < max_messages:
                    try:
                        message = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=5.0
                        )
                        messages.append(json.loads(message))
                        
                        if (datetime.now() - start_time).seconds > timeout_seconds:
                            break
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        logger.error("websocket_message_error", error=str(e))
                        break
        
        try:
            asyncio.run(receive_messages())
            df = pd.DataFrame(messages)
            logger.info("websocket_extraction_successful", messages=len(df))
            return df
            
        except Exception as e:
            logger.error("websocket_extraction_failed", error=str(e))
            raise
