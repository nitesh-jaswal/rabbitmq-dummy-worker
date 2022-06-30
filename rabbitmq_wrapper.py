from typing import Callable
import pika
from config import get_config

cfg = get_config()

class RabbitMQPublisher:

    def __init__(
        self,
        queue_name: str,
        **kwargs
    ):
        self.queue_name = queue_name
        # self._host_url = f"amqp://{cfg.RABBITMQ_UNAME}:{cfg.RABBITMQ_PASS}@{cfg.RABBITMQ_BROKER}:{cfg.RABBITMQ_PORT}"
        self._host_url = "127.0.0.1"
        self.connection = self._get_connection()
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name, durable=True)

    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()

    def close(self):
        self.connection.close()

    def _get_connection(self):
        return pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host_url))    

    def publish_message(self, msg: str):
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body=msg,
            properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )

class RabbitMQConsumer:
    def __init__(
        self,
        queue_name: str,
        **kwargs
    ):
        self.queue_name = queue_name
        # self._host_url = f"amqp://{cfg.RABBITMQ_UNAME}:{cfg.RABBITMQ_PASS}@{cfg.RABBITMQ_BROKER}:{cfg.RABBITMQ_PORT}"
        self._host_url = "127.0.0.1"
        self.connection = self._get_connection()
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name, durable=True)

    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()

    def close(self):
        self.connection.close()

    def _get_connection(self):
        return pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host_url))

    def consume(
        self,
        callback_fn: Callable[..., None],
        prefetch_count: int = 1
    ):
        self.channel.basic_qos(prefetch_count=prefetch_count)
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback_fn
        )
        self.channel.start_consuming()