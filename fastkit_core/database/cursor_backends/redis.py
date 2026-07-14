import json
import secrets
from datetime import datetime, timezone
from typing import Any

from redis.asyncio import Redis

from fastkit_core.database.cursor_backends.base import BaseCursorBackend
from fastkit_core.http import InvalidCursorError

CURSOR_KEY_PREFIX = 'fastkit:cursor'

class RedisCursorBackend(BaseCursorBackend):
    """
    Server-side cursor backend — stores cursor metadata in Redis.

    The token returned to the client is a random opaque string.
    The underlying field value, direction, and filters are never exposed.

    Args:
        redis:      Connected redis.asyncio.Redis instance.
        ttl:        Cursor TTL in seconds. Default: 300 (5 minutes).
        key_prefix: Redis key prefix. Default: 'fastkit:cursor'.
    """

    def __init__(
            self,
            redis: Redis,
            ttl: int = 300,
            key_prefix: str = CURSOR_KEY_PREFIX,
    ) -> None:
        self._redis = redis
        self._ttl = ttl
        self._key_prefix = key_prefix

    def _key(self, token: str) -> str:
        return f'{self._key_prefix}:{token}'

    async def encode(
            self,
            field: str,
            value: Any,
            filters: dict | None = None,
            direction: str = 'asc',
    ) -> str:
        token = secrets.token_urlsafe(32)
        key = self._key(token)
        data = {
            'field': field,
            'value': value,
            'filters': filters or {},
            'direction': direction,
            'created_at': datetime.now(timezone.utc).isoformat(),
        }
        await self._redis.set(key, json.dumps(data, default=str), ex=self._ttl)
        return token
