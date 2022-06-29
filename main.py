import sys
import argparse
import random
import time
from uuid import uuid4
from json import dumps
from typing import Dict, Any
from config import get_config
from rabbitmq_wrapper import RabbitMQPublisher

_MAX_TASKS: int = 100
cfg = get_config()

def _create_dummy_task(serial_number: int):
    delay = random.randint(1, 60)
    task_id = str(uuid4())    
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
    result = dict(
        total_tasks=number_of_tasks, 
        success_tasks=0, 
        failed_tasks=0
    )
    task_generator = (_create_dummy_task(i) for i in range(number_of_tasks))
    with RabbitMQPublisher(cfg.TASK_QUEUE_NAME) as rqpublisher:
        for task in task_generator:
            print("Queing Task: ", task)
            msg = dumps(task)
            rqpublisher.publish_message(msg)
    
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

    print(f"Result: {result}")
    print(f"Time Taken: {stop - start}s")


