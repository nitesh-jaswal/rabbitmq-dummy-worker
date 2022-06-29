from os import environ
from dotenv import load_dotenv
from functools import lru_cache
from dataclasses import dataclass, asdict
load_dotenv('./.env', override=True)

@dataclass(frozen=True, slots=True)
class Config:
    REDIS_BROKER: str = environ['REDIS_BROKER']
    REDIS_PORT: str = environ['REDIS_PORT']
    REDIS_PASS: str = environ['REDIS_PASS']
    RABBITMQ_BROKER: str = environ['RABBITMQ_BROKER']
    RABBITMQ_PORT: str = environ['RABBITMQ_PORT']
    RABBITMQ_UNAME: str = environ['RABBITMQ_UNAME']
    RABBITMQ_PASS: str = environ['RABBITMQ_PASS']
    TASK_QUEUE_NAME: str = "worker_tasks"

@lru_cache(maxsize=1)
def get_config() -> Config:
    cfg = Config()
    return cfg
