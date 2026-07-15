"""
Tests for cursor backends and cursor_paginate integration.

Coverage targets:
- LocalCursorBackend: encode/decode roundtrip, InvalidCursorError (lines 35-36)
- RedisCursorBackend: encode/decode, expired token, corrupt data (0% → covered)
- RedisAsyncCursorBackend: async encode/decode, expired token, corrupt data (0% → covered)
- Repository.cursor_paginate: first page, second page, desc, filters, last page
- AsyncRepository.cursor_paginate: same cases + _encode_cursor_token/_decode_cursor_token
  with sync and async backend dispatch
"""

import base64
import json
import pytest
from unittest.mock import MagicMock, patch
from typing import Any

from fastkit_core.database.cursor_backends.local import LocalCursorBackend
from fastkit_core.database.cursor_backends.redis import RedisCursorBackend
from fastkit_core.database.cursor_backends.redis_async import RedisAsyncCursorBackend
from fastkit_core.database.exceptions import InvalidCursorError


# ============================================================================
# Helpers
# ============================================================================

def _make_sync_redis(store: dict | None = None) -> MagicMock:
    """Fake sync Redis client backed by an in-memory dict."""
    store = store if store is not None else {}
    client = MagicMock()

    def _set(key, value, ex=None):
        store[key] = value

    def _get(key):
        return store.get(key)

    client.set.side_effect = _set
    client.get.side_effect = _get
    return client, store


def _make_async_redis(store: dict | None = None) -> MagicMock:
    """Fake async Redis client backed by an in-memory dict."""
    store = store if store is not None else {}
    client = MagicMock()

    async def _set(key, value, ex=None):
        store[key] = value

    async def _get(key):
        return store.get(key)

    client.set = _set
    client.get = _get
    return client, store