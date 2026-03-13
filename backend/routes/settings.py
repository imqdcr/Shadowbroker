import json as json_mod

from fastapi import APIRouter, Request, Response
from pydantic import BaseModel
from services.api_settings import get_api_keys, update_api_key
from services.modules.news.feed_config import (get_feeds, reset_feeds,
                                               save_feeds)

router = APIRouter(prefix="/api")


class ApiKeyUpdate(BaseModel):
    env_key: str
    value: str


@router.get("/settings/api-keys")
async def api_get_keys():
    return get_api_keys()


@router.put("/settings/api-keys")
async def api_update_key(body: ApiKeyUpdate):
    ok = update_api_key(body.env_key, body.value)
    if ok:
        return {"status": "updated", "env_key": body.env_key}
    return {"status": "error", "message": "Failed to update .env file"}


@router.get("/settings/news-feeds")
async def api_get_news_feeds():
    return get_feeds()


@router.put("/settings/news-feeds")
async def api_save_news_feeds(request: Request):
    body = await request.json()
    ok = save_feeds(body)
    if ok:
        return {"status": "updated", "count": len(body)}
    return Response(
        content=json_mod.dumps(
            {
                "status": "error",
                "message": "Validation failed (max 20 feeds, each needs name/url/weight 1-5)",
            }
        ),
        status_code=400,
        media_type="application/json",
    )


@router.post("/settings/news-feeds/reset")
async def api_reset_news_feeds():
    ok = reset_feeds()
    if ok:
        return {"status": "reset", "feeds": get_feeds()}
    return {"status": "error", "message": "Failed to reset feeds"}
