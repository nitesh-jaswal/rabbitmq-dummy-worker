import redis
from json import dumps
from typing import Dict, List

class BaseRedisHandler:
    def __init__(
         self,
         host: str,
         port: str,
         password: str
    ):
        self.host = host
        self.port = port
        self.password = password
        self.client = self._get_client()
            
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
    
    def _get_client(self):
        return redis.Redis(
            host=self.host,
            port=self.port,
            password=self.password
        )

    def close(self):
        self.client.close()

class MainRedisHandler(BaseRedisHandler):

    def __init__(
        self,
        host: str,
        port: str,
        password: str
    ):
        super().__init__(host, port, password)
    
    def completed(self, task_id: str) -> int:
        return self.client.llen(task_id)
    
    def get_result(self, task_id: str) -> List:
        return self.client.lrange(task_id, 0, -1)
class WorkerRedisHandler(BaseRedisHandler):
    MAX_RETRY: int = 1

    def __init__(
         self,
         host: str,
         port: str,
         password: str
    ):
        super().__init__(host, port, password)
        self.retry_number = 0
    
    # Currently using a List data structure. A set could also be suitable
    def publish_data(
        self, 
        task_id: str, 
        msg: Dict[str, str]
    ) -> bool:
        try:
            msg_str = dumps(msg)
            retval = self.client.lpush(task_id, msg_str)
            if retval < 1:
                return False
            return True
        except redis.exceptions.ConnectionError as e:
            if self.retry_number <= WorkerRedisHandler.MAX_RETRY:
                print(f"ConnectionError: Re-establishing connection and retrying. Retry number {self.retry_number}")
                self.retry_number += 1
                self.close()
                self.client = self._get_client()
                return self.publish_data(msg)
            return False