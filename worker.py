from json import loads
from config import get_config
from rabbitmq_wrapper import RabbitMQConsumer
from pathlib import Path
import time

cfg = get_config()

def _dummy_process(
    channel,
    method,
    properties,
    body
):
    task = loads(body.decode('utf-8'))
    print(f"Processing task number: {task['serial_number']}...Sleeping for {task['delay']}")
    time.sleep(task['delay'])
    channel.basic_ack(delivery_tag=method.delivery_tag)

def main():
    wf = Path('./worker_number')
    with wf.open('r') as f:
        worker_number = int(f.read())
    with wf.open('w') as f:
        f.write(f"{worker_number+1}")
    
    print(f"Started Worker {worker_number}")
    with RabbitMQConsumer(cfg.TASK_QUEUE_NAME) as rqconsumer:
        rqconsumer.consume(_dummy_process)

if __name__ == "__main__":
    main()