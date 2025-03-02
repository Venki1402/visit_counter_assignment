import redis
from typing import Dict, List, Optional, Any
import hashlib
from bisect import bisect
from app.logger import logger
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

        # Initialize connection pools for each Redis node
        for node in redis_nodes:
            self.connection_pools[node] = redis.ConnectionPool.from_url(node)
            self.redis_clients[node] = redis.Redis(
                connection_pool=self.connection_pools[node]
            )
            logger.info(f"Initialized Redis client for node: {node}")

        # Initialize consistent hash ring
        self.consistent_hash = ConsistentHash(redis_nodes, settings.VIRTUAL_NODES)
        logger.info(
            f"Initialized consistent hash ring with {len(redis_nodes)} nodes and {settings.VIRTUAL_NODES} virtual nodes per node"
        )

    async def get_connection(self, key: str) -> redis.Redis:
        """
        Get Redis connection for the given key using consistent hashing

        Args:
            key: The key to determine which Redis node to use

        Returns:
            Redis client for the appropriate node
        """
        # Use consistent hashing to determine which node should handle this key
        node = self.consistent_hash.get_node(key)
        logger.info(f"Key {key} mapped to Redis node: {node}")
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
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Get the appropriate Redis connection
                client = await self.get_connection(key)
                # Extract node port from connection URL for logging
                node_url = next(
                    node
                    for node, redis_client in self.redis_clients.items()
                    if redis_client == client
                )
                node_port = node_url.split(":")[-2]

                # Increment the counter
                result = client.incrby(key, amount)
                logger.info(f"Incremented {key} by {amount} on Redis {node_port}")
                return result
            except redis.RedisError as e:
                retry_count += 1
                logger.warning(
                    f"Redis increment error (attempt {retry_count}/{max_retries}): {str(e)}"
                )
                if retry_count >= max_retries:
                    logger.error(
                        f"Failed to increment {key} after {max_retries} attempts"
                    )
                    raise
            except Exception as e:
                logger.error(f"Unexpected error incrementing {key}: {str(e)}")
                raise

    async def get(self, key: str) -> Optional[int]:
        """
        Get value for a key from Redis

        Args:
            key: The key to get

        Returns:
            Value of the key or None if not found
        """
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Get the appropriate Redis connection
                client = await self.get_connection(key)
                # Extract node port from connection URL for logging
                node_url = next(
                    node
                    for node, redis_client in self.redis_clients.items()
                    if redis_client == client
                )
                node_port = node_url.split(":")[-2]

                # Retrieve the value
                value = client.get(key)
                result = int(value) if value else None
                logger.info(f"Retrieved {key} from Redis {node_port}: {result}")
                return result
            except redis.RedisError as e:
                retry_count += 1
                logger.warning(
                    f"Redis get error (attempt {retry_count}/{max_retries}): {str(e)}"
                )
                if retry_count >= max_retries:
                    logger.error(f"Failed to get {key} after {max_retries} attempts")
                    raise
            except Exception as e:
                logger.error(f"Unexpected error getting {key}: {str(e)}")
                raise

    async def add_shard(self, node_url: str) -> None:
        """
        Add a new Redis shard dynamically

        Args:
            node_url: URL of the new Redis node
        """
        if node_url in self.redis_clients:
            logger.warning(f"Node {node_url} already exists in the cluster")
            return

        try:
            # Add to connection pools and clients
            self.connection_pools[node_url] = redis.ConnectionPool.from_url(node_url)
            self.redis_clients[node_url] = redis.Redis(
                connection_pool=self.connection_pools[node_url]
            )

            # Add to consistent hash ring
            self.consistent_hash.add_node(node_url)
            logger.info(f"Successfully added new Redis shard: {node_url}")
        except Exception as e:
            logger.error(f"Failed to add new Redis shard {node_url}: {str(e)}")
            raise

    async def remove_shard(self, node_url: str) -> None:
        """
        Remove a Redis shard dynamically

        Args:
            node_url: URL of the Redis node to remove
        """
        if node_url not in self.redis_clients:
            logger.warning(f"Node {node_url} does not exist in the cluster")
            return

        try:
            # Remove from consistent hash ring first
            self.consistent_hash.remove_node(node_url)

            # Close connections and remove from dicts
            self.redis_clients[node_url].close()
            self.connection_pools[node_url].disconnect()
            del self.redis_clients[node_url]
            del self.connection_pools[node_url]

            logger.info(f"Successfully removed Redis shard: {node_url}")
        except Exception as e:
            logger.error(f"Failed to remove Redis shard {node_url}: {str(e)}")
            raise

    async def get_shard_info(self) -> Dict[str, Any]:
        """
        Get information about current shards

        Returns:
            Dictionary with shard information
        """
        shard_info = {
            "total_shards": len(self.redis_clients),
            "shards": list(self.redis_clients.keys()),
            "virtual_nodes": settings.VIRTUAL_NODES,
        }
        return shard_info
