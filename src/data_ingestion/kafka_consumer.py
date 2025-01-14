from kafka import KafkaConsumer
from typing import Dict, Any
import json
import logging

class TransactionConsumer:
    """
    Kafka consumer for ingesting real-time transaction data
    """
    
    def __init__(self, bootstrap_servers: str, topic: str):
        """
        Initialize the Kafka consumer
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Topic to consume messages from
        """
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='fraud_detection_group'
        )
        self.logger = logging.getLogger(__name__)
    
    def consume_transactions(self) -> Dict[str, Any]:
        """
        Consume and process transaction messages
        
        Returns:
            Dict containing transaction details
        """
        try:
            for message in self.consumer:
                transaction = message.value
                self.logger.info(f'Received transaction: {transaction["transaction_id"]}')
                yield transaction
                
        except Exception as e:
            self.logger.error(f'Error consuming message: {str(e)}')
            raise
    
    def close(self):
        """Close the Kafka consumer"""
        self.consumer.close()