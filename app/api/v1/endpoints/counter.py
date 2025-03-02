from fastapi import APIRouter, HTTPException, Depends, Body
from ....services.visit_counter import VisitCounterService
from ....schemas.counter import VisitCount
from typing import Dict, Any

router = APIRouter()
counter_service = VisitCounterService()


@router.post("/visit/{page_id}")
async def record_visit(page_id: str):
    """Record a visit for a website"""
    try:
        await counter_service.increment_visit(page_id)
        return {"status": "success", "message": f"Visit recorded for page {page_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/visits/{page_id}", response_model=VisitCount)
async def get_visits(page_id: str):
    """Get visit count for a website"""
    try:
        response = await counter_service.get_visit_count(page_id)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# New endpoints for managing Redis shards
@router.get("/shards", response_model=Dict[str, Any])
async def get_shard_info():
    """Get information about current Redis shards"""
    try:
        return await counter_service.get_shard_info()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shards", response_model=Dict[str, Any])
async def add_shard(node_url: str = Body(..., embed=True)):
    """Add a new Redis shard to the cluster"""
    try:
        return await counter_service.add_redis_shard(node_url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/shards", response_model=Dict[str, Any])
async def remove_shard(node_url: str = Body(..., embed=True)):
    """Remove a Redis shard from the cluster"""
    try:
        return await counter_service.remove_redis_shard(node_url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
