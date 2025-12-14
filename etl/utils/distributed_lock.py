"""
Distributed locking mechanism using Redis
"""
import time
import uuid
from typing import Optional
from contextlib import contextmanager
import redis
from etl.config import config
from etl.logger import get_logger

logger = get_logger(__name__)


class DistributedLock:
    """Redis-based distributed lock for coordinating ETL operations"""
    
    def __init__(self, lock_name: str, timeout: int = 300, retry_interval: int = 1):
        """
        Initialize distributed lock
        
        Args:
            lock_name: Unique identifier for the lock
            timeout: Lock expiration time in seconds
            retry_interval: Time to wait between lock acquisition attempts
        """
        self.lock_name = f"etl:lock:{lock_name}"
        self.timeout = timeout
        self.retry_interval = retry_interval
        self.lock_id = str(uuid.uuid4())
        
        self.redis_client = redis.Redis(
            host=config.redis.host,
            port=config.redis.port,
            password=config.redis.password,
            db=config.redis.db,
            decode_responses=True
        )
    
    def acquire(self, blocking: bool = True, timeout: Optional[int] = None) -> bool:
        """Acquire the distributed lock"""
        start_time = time.time()
        max_wait = timeout or self.timeout
        
        while True:
            acquired = self.redis_client.set(
                self.lock_name,
                self.lock_id,
                nx=True,
                ex=self.timeout
            )
            
            if acquired:
                logger.info("lock_acquired", lock_name=self.lock_name, lock_id=self.lock_id)
                return True
            
            if not blocking:
                return False
            
            if time.time() - start_time >= max_wait:
                logger.warning("lock_acquisition_timeout", lock_name=self.lock_name, timeout=max_wait)
                return False
            
            time.sleep(self.retry_interval)
    
    def release(self) -> bool:
        """Release the distributed lock"""
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        
        released = self.redis_client.eval(lua_script, 1, self.lock_name, self.lock_id)
        
        if released:
            logger.info("lock_released", lock_name=self.lock_name, lock_id=self.lock_id)
        
        return bool(released)
    
    @contextmanager
    def context(self, blocking: bool = True, timeout: Optional[int] = None):
        """Context manager for lock acquisition and release"""
        acquired = self.acquire(blocking=blocking, timeout=timeout)
        
        if not acquired:
            raise RuntimeError(f"Failed to acquire lock: {self.lock_name}")
        
        try:
            yield self
        finally:
            self.release()
    
    def __enter__(self):
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False
