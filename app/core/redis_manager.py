import redis
from typing import Dict, List, Optional, Any
import logging
from .consistent_hash import ConsistentHash
from .config import settings


class RedisManager:
    def __init__(self):
        """Initialize Redis connection pools and consistent hashing"""
        self.connection_pools: Dict[str, redis.ConnectionPool] = {}
        self.redis_clients: Dict[str, redis.Redis] = {}

        # Parse Redis nodes from comma-separated string
        redis_nodes = [
            node.strip() for node in settings.REDIS_NODES.split(",") if node.strip()
        ]
        # self.consistent_hash = ConsistentHash(redis_nodes, settings.VIRTUAL_NODES)

        # TODO: Initialize connection pools for each Redis node
        # 1. Create connection pools for each Redis node
        # 2. Initialize Redis clients
        for node in redis_nodes:
            self.connection_pools[node] = redis.ConnectionPool.from_url(node)
            self.redis_clients[node] = redis.Redis(
                connection_pool=self.connection_pools[node]
            )

    async def get_connection(self, key: str) -> redis.Redis:
        """
        Get Redis connection for the given key using consistent hashing

        Args:
            key: The key to determine which Redis node to use

        Returns:
            Redis client for the appropriate node
        """
        # TODO: Implement getting the appropriate Redis connection
        # 1. Use consistent hashing to determine which node should handle this key
        # 2. Return the Redis client for that node
        node = list(self.redis_clients.keys())[0]
        return self.redis_clients[node]

    async def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment a counter in Redis

        Args:
            key: The key to increment
            amount: Amount to increment by

        Returns:
            New value of the counter
        """
        # TODO: Implement incrementing a counter
        # 1. Get the appropriate Redis connection
        # 2. Increment the counter
        # 3. Handle potential failures and retries
        try:
            client = await self.get_connection(key)
            return client.incrby(key, amount)
        except Exception as e:
            logging.error(f"Redis increment error: {str(e)}")
            raise

    async def get(self, key: str) -> Optional[int]:
        """
        Get value for a key from Redis

        Args:
            key: The key to get

        Returns:
            Value of the key or None if not found
        """
        # TODO: Implement getting a value
        # 1. Get the appropriate Redis connection
        # 2. Retrieve the value
        # 3. Handle potential failures and retries
        try:
            client = await self.get_connection(key)
            value = client.get(key)
            return int(value) if value else None
        except Exception as e:
            logging.error(f"Redis get error: {str(e)}")
            raise
