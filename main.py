import sys
import argparse
import random
import time
from uuid import uuid4
from json import dumps, loads
from typing import Dict, Any
from config import get_config
from rabbitmq_wrapper import RabbitMQPublisher
from redis_wrapper import MainRedisHandler

_MAX_TASKS: int = 100
cfg = get_config()
redishandler = MainRedisHandler(
    host=cfg.REDIS_BROKER,
    port=cfg.REDIS_PORT,
    password=cfg.REDIS_PASS
)

def _create_dummy_task(task_id: str, serial_number: int):
    delay = random.randint(1, 5)
    return {
        'serial_number': serial_number,
        'delay': delay,
        'task_id': task_id
    }

def _execute_tasks(number_of_tasks: int) -> Dict[str, Any]:
    """
    params:
        number_of_tasks => int: The number of tasks that need to be scheduled
    returns: A dictionary with the following keys
        total_tasks => int : Total number of scheduled tasks
        susccess_tasks => int : Total number of successfull tasks
        failed_tasks => int : Total number of failed tasks
    """
    task_id = str(uuid4())    
    result = dict(
        total_tasks=number_of_tasks, 
        success_tasks=0, 
        failed_tasks=0
    )
    task_list = list(_create_dummy_task(task_id, i) for i in range(number_of_tasks))
    print(f"Debug: Combined delay time for all tasks={sum(x['delay'] for x in task_list)}")
    with RabbitMQPublisher(cfg.TASK_QUEUE_NAME) as rqpublisher:
        for task in task_list:
            # print("Queing Task: ", task)
            msg = dumps(task)
            rqpublisher.publish_message(msg)
        completed = 0
        while completed < number_of_tasks:
            print(f"Polling for completion. Completed {completed}/{number_of_tasks}")
            print(f"Sleeping for 10 seconds")
            time.sleep(10)
            completed = redishandler.completed(task_id)
        print(f"Response received from all tasks. Compiling result")
        for res in map(lambda x: loads(x), redishandler.get_result(task_id)):
            if res["success"]:
                result["success_tasks"] += 1
            else:
                result["failed_tasks"] += 1    
    # Poll for completion with status
    return result

if __name__ == "__main__":
    try:
        number_of_tasks: int = int(sys.argv[1])
        if number_of_tasks <= 0:
            raise ValueError
    except TypeError as e:
        print(f"{e}: Type of argument must be and integer greater than 0")
    except ValueError as e:
        print(f"{e}: Type of argument must be and integer greater than 0")
    number_of_tasks = min(number_of_tasks, _MAX_TASKS)
    
    start = time.perf_counter()
    result = _execute_tasks(number_of_tasks)
    stop = time.perf_counter()
    
    redishandler.close()
    print(f"Result: {result}")
    print(f"Time Taken: {stop - start}s")


