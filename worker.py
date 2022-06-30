from json import loads, dumps
from config import get_config
from rabbitmq_wrapper import RabbitMQConsumer
from redis_wrapper import WorkerRedisHandler
from pathlib import Path
import time
import sys

cfg = get_config()

# Make this class a BaseClass from whihc the worker and main handler's inherit
# Explore factory pattern

redishandler = WorkerRedisHandler(
    host=cfg.REDIS_BROKER,
    port=cfg.REDIS_PORT,
    password=cfg.REDIS_PASS
)

def _dummy_process(
    channel,
    method,
    properties,
    body
):
    try:
        task = loads(body.decode('utf-8'))
        print(f"Processing task number: {task['serial_number']}...Sleeping for {task['delay']}")
        time.sleep(task['delay'])

        msg = dict(serial_number=task['serial_number'], success=True)
        data_pushed_flag = redishandler.publish_data(task_id=task['task_id'], msg=msg)
        if not data_pushed_flag:
            print(f"Failed to publish data {msg} to Redis Broker. Exiting worker to prevent further data loss...")
            sys.exit(0)
        print(f"Succesfully pushed result for Task: {task['task_id']}, Serial Number: {task['serial_number']}")

        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Ran into error: {e}")
    

def main():
    wf = Path('./worker_number')
    with wf.open('r') as f:
        worker_number = int(f.read())
    with wf.open('w') as f:
        f.write(f"{worker_number + 1}")
    
    print(f"Started Worker {worker_number}")
    with RabbitMQConsumer(cfg.TASK_QUEUE_NAME) as rqconsumer:
        rqconsumer.consume(_dummy_process)

if __name__ == "__main__":
    main()