from typing import Dict, List, Any
import asyncio
from app.logger import logger
from datetime import datetime
from ..core.redis_manager import RedisManager
import time
from ..schemas.counter import VisitCount


class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with Redis manager"""
        # self.visit_counts: Dict[str, int] = {}

        self.redis_manager = RedisManager()
        
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.cache_ttl = 5

        self.buffer: Dict[str, int] = {}
        self.lock = asyncio.Lock()
        self.flush_interval = 30
        self.flush_task = asyncio.create_task(self.periodic_flush())
        logger.info("Visit counter service initialized with write batching")

    async def periodic_flush(self):
        """Background task that periodically flushes the buffer to Redis"""
        while True:
            try:
                logger.info("✅ start")
                await asyncio.sleep(self.flush_interval)
                logger.info("✅ end")
                await self.flush_buffer()
            except Exception as e:
                logger.error(f"Error in periodic flush: {str(e)}")

    async def flush_buffer(self):
        """Flush all accumulated counts from buffer to Redis"""
        async with self.lock:
            if not self.buffer:
                logger.info("Buffer is empty, nothing to flush")
                return

            logger.info(f"Flushing buffer with {len(self.buffer)} keys to Redis")
            buffer_copy = self.buffer.copy()
            self.buffer.clear()

            try:
                for page_id, count in buffer_copy.items():
                    key = f"visit:{page_id}"
                    updated_count = await self.redis_manager.increment(key, count)
                    self.cache[page_id] = {
                        "value": updated_count,
                        "timestamp": time.time(),
                    }
                    logger.info(f"Flushed {count} visits for {page_id} to Redis")
            except Exception as e:
                logger.error(f"Error flushing buffer to Redis: {str(e)}")
                async with self.lock:
                    for page_id, count in buffer_copy.items():
                        if page_id in self.buffer:
                            self.buffer[page_id] += count
                        else:
                            self.buffer[page_id] = count
                raise

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page in memory buffer

        Args:
            page_id: Unique identifier for the page
        """
        try:
            async with self.lock:
                # Add to buffer instead of writing directly to Redis
                if page_id in self.buffer:
                    self.buffer[page_id] += 1
                else:
                    self.buffer[page_id] = 1

                # Update cache with combined count (persisted + pending)
                # First, get current persisted count from cache or Redis
                persisted_count = 0
                if page_id in self.cache:
                    cache_entry = self.cache[page_id]
                    if time.time() - cache_entry["timestamp"] <= self.cache_ttl:
                        persisted_count = cache_entry["value"]
                    else:
                        # Cache expired, get from Redis
                        persisted_count = (
                            await self.redis_manager.get(f"visit:{page_id}") or 0
                        )
                else:
                    # Not in cache, get from Redis
                    persisted_count = (
                        await self.redis_manager.get(f"visit:{page_id}") or 0
                    )

                # Calculate total count (Redis + pending buffer)
                total_count = persisted_count + self.buffer[page_id]

                # Update cache with total count
                self.cache[page_id] = {"value": total_count, "timestamp": time.time()}
                logger.info(
                    f"Incremented visit in buffer for {page_id}, total count: {total_count}"
                )

        except Exception as e:
            logger.error(f"Error incrementing visit count: {str(e)}")
            raise

    async def get_visit_count(self, page_id: str) -> VisitCount:
        """
        Get current visit count for a page, combining Redis count with buffer

        Args:
            page_id: Unique identifier for the page

        Returns:
            Current visit count
        """
        logger.info(f"Getting visit count for {page_id}")
        current_time = time.time()

        # Get pending count from buffer (if any)
        buffer_count = 0
        async with self.lock:
            buffer_count = self.buffer.get(page_id, 0)

        # Check if we have a valid cache entry
        if page_id in self.cache:
            cache_entry = self.cache[page_id]
            if current_time - cache_entry["timestamp"] <= self.cache_ttl:
                logger.info(
                    f"Found InMemory cache for {page_id}, u saved computation cost 🥳🥳🥳"
                )
                return VisitCount(visits=cache_entry["value"], served_via="using cache")

        try:
            logger.info(f"Cache miss for {page_id}, flushing buffer")
            await self.flush_buffer()

            count = await self.redis_manager.get(f"visit:{page_id}")
            count = count or 0

            async with self.lock:
                new_buffer_count = self.buffer.get(page_id, 0)

            total_count = count + new_buffer_count

            self.cache[page_id] = {"value": total_count, "timestamp": time.time()}
            logger.info(f"Cache updated for {page_id}")

            return VisitCount(visits=total_count, served_via="Redis")
        except Exception as e:
            logger.error(f"Error getting visit count: {str(e)}")
            raise

    async def cleanup(self):
        """Clean up resources and ensure all pending writes are flushed"""
        logger.info("Cleaning up visit counter service")
        if hasattr(self, "flush_task"):
            self.flush_task.cancel()
            try:
                await self.flush_task
            except asyncio.CancelledError:
                pass
        await self.flush_buffer()
        logger.info("Visit counter service cleanup completed")
