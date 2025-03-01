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

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page

        Args:
            page_id: Unique identifier for the page
        """
        # TODO: Implement visit count increment
        # if page_id in self.visit_counts:
        #     self.visit_counts[page_id] += 1
        # else:
        #     self.visit_counts[page_id] = 1

        try:
            updated_count = self.redis_manager.increment(f"visit:{page_id}")
            self.cache[page_id] = {"value": updated_count, "timestamp": time.time()}
            logger.info("successfully incremented visit count and updated cache")
        except Exception as e:
            logger.error(f"Error incrementing visit count: {str(e)}")
            raise

    async def get_visit_count(self, page_id: str) -> VisitCount:
        """
        Get current visit count for a page

        Args:
            page_id: Unique identifier for the page

        Returns:
            Current visit count
        """
        # TODO: Implement getting visit count
        # if page_id in self.visit_counts:
        #     return self.visit_counts[page_id]
        # else:
        #     return 0
        logger.info(f"cache {self.cache}")

        current_time = time.time()
        if page_id in self.cache:
            cache_entry = self.cache[page_id]
            if current_time - cache_entry["timestamp"] <= self.cache_ttl:
                logger.info(
                    f"Found InMemory cache for {page_id}, u saved computation cost 🥳🥳🥳"
                )
                return VisitCount(visits=cache_entry["value"], served_via="using cache")

        try:
            count = await self.redis_manager.get(f"visit:{page_id}")
            count = count or 0
            self.cache[page_id] = {"value": count, "timestamp": time.time()}
            logger.info(f"Cache updated for {page_id}")
            return VisitCount(visits=count, served_via="Redis")
        except Exception as e:
            logger.error(f"Error getting visit count: {str(e)}")
            raise
