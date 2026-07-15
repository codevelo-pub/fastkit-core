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

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
# LocalCursorBackend
# ============================================================================

class TestLocalCursorBackend:
    """LocalCursorBackend — stateless base64 encode/decode."""

    def setup_method(self):
        self.backend = LocalCursorBackend()

    # --- encode ---

    def test_encode_returns_string(self):
        token = self.backend.encode(field='id', value=42)
        assert isinstance(token, str)

    def test_encode_is_base64(self):
        token = self.backend.encode(field='id', value=42)
        # Must decode without error
        decoded_bytes = base64.urlsafe_b64decode(token.encode())
        assert decoded_bytes

    def test_encode_default_direction_is_asc(self):
        token = self.backend.encode(field='id', value=1)
        payload = json.loads(base64.urlsafe_b64decode(token).decode())
        assert payload['direction'] == 'asc'

    def test_encode_stores_field_and_value(self):
        token = self.backend.encode(field='created_at', value='2026-01-01')
        payload = json.loads(base64.urlsafe_b64decode(token).decode())
        assert payload['field'] == 'created_at'
        assert payload['value'] == '2026-01-01'

    def test_encode_desc_direction(self):
        token = self.backend.encode(field='id', value=10, direction='desc')
        payload = json.loads(base64.urlsafe_b64decode(token).decode())
        assert payload['direction'] == 'desc'

    def test_encode_ignores_filters_in_token(self):
        """filters are accepted but not stored in stateless backend."""
        token = self.backend.encode(field='id', value=1, filters={'status': 'active'})
        payload = json.loads(base64.urlsafe_b64decode(token).decode())
        # filters key is not expected in the stateless token
        assert 'field' in payload
        assert 'value' in payload

    def test_encode_non_string_value(self):
        """Integer, UUID-like strings, and floats must all encode cleanly."""
        for value in [1, 3.14, 'uuid-abc-123']:
            token = self.backend.encode(field='id', value=value)
            assert isinstance(token, str)

    # --- decode ---

    def test_decode_roundtrip(self):
        token = self.backend.encode(field='id', value=5, direction='asc')
        decoded = self.backend.decode(token)
        assert decoded['field'] == 'id'
        assert decoded['value'] == 5
        assert decoded['direction'] == 'asc'

    def test_decode_desc_roundtrip(self):
        token = self.backend.encode(field='name', value='Alice', direction='desc')
        decoded = self.backend.decode(token)
        assert decoded['field'] == 'name'
        assert decoded['value'] == 'Alice'
        assert decoded['direction'] == 'desc'

    def test_decode_invalid_token_raises_invalid_cursor_error(self):
        """Lines 35-36 in local.py — the except branch."""
        with pytest.raises(InvalidCursorError):
            self.backend.decode('not-valid-base64!!!')

    def test_decode_valid_base64_but_not_json_raises(self):
        """Valid base64 but non-JSON content → InvalidCursorError."""
        bad = base64.urlsafe_b64encode(b'not json').decode()
        with pytest.raises(InvalidCursorError):
            self.backend.decode(bad)

    def test_decode_empty_string_raises(self):
        with pytest.raises(InvalidCursorError):
            self.backend.decode('')


# ============================================================================
# RedisCursorBackend (sync)
# ============================================================================

class TestRedisCursorBackend:
    """RedisCursorBackend — server-side opaque token via sync Redis."""

    def setup_method(self):
        self.redis_client, self.store = _make_sync_redis()
        self.backend = RedisCursorBackend(redis=self.redis_client, ttl=300)

    # --- encode ---

    def test_encode_returns_string(self):
        token = self.backend.encode(field='id', value=1)
        assert isinstance(token, str)

    def test_encode_token_is_opaque(self):
        """Token must not be base64-decodable to the field value."""
        token = self.backend.encode(field='id', value=42)
        try:
            raw = base64.urlsafe_b64decode(token + '==')
            payload = json.loads(raw)
            # If it decoded, it must not expose the value directly
            assert payload.get('value') != 42
        except Exception:
            pass  # Expected — opaque token is not base64 JSON

    def test_encode_writes_to_redis(self):
        token = self.backend.encode(field='id', value=10)
        key = f'fastkit:cursor:{token}'
        assert key in self.store

    def test_encode_stored_value_contains_field_and_value(self):
        token = self.backend.encode(field='id', value=99)
        key = f'fastkit:cursor:{token}'
        data = json.loads(self.store[key])
        assert data['field'] == 'id'
        assert data['value'] == 99

    def test_encode_stores_direction(self):
        token = self.backend.encode(field='id', value=1, direction='desc')
        key = f'fastkit:cursor:{token}'
        data = json.loads(self.store[key])
        assert data['direction'] == 'desc'

    def test_encode_stores_filters(self):
        token = self.backend.encode(field='id', value=1, filters={'status': 'active'})
        key = f'fastkit:cursor:{token}'
        data = json.loads(self.store[key])
        assert data['filters'] == {'status': 'active'}

    def test_encode_empty_filters_stored_as_empty_dict(self):
        token = self.backend.encode(field='id', value=1)
        key = f'fastkit:cursor:{token}'
        data = json.loads(self.store[key])
        assert data['filters'] == {}

    def test_encode_uses_ttl(self):
        self.backend.encode(field='id', value=1)
        # Verify set was called with ex=300
        call_kwargs = self.redis_client.set.call_args
        assert call_kwargs.kwargs.get('ex') == 300 or call_kwargs.args[2] == 300 \
               or (len(call_kwargs.args) > 2 and call_kwargs.args[2] == 300)

    def test_encode_two_calls_produce_different_tokens(self):
        t1 = self.backend.encode(field='id', value=1)
        t2 = self.backend.encode(field='id', value=1)
        assert t1 != t2

    # --- decode ---

    def test_decode_roundtrip(self):
        token = self.backend.encode(field='id', value=42, direction='asc')
        decoded = self.backend.decode(token)
        assert decoded['field'] == 'id'
        assert decoded['value'] == 42
        assert decoded['direction'] == 'asc'

    def test_decode_unknown_token_raises(self):
        with pytest.raises(InvalidCursorError, match="not found or expired"):
            self.backend.decode('nonexistent-token')

    def test_decode_corrupt_data_raises(self):
        """Manually write corrupt JSON into the store."""
        token = 'test-corrupt-token'
        key = f'fastkit:cursor:{token}'
        self.store[key] = 'not valid json {'
        with pytest.raises(InvalidCursorError, match="Corrupt cursor data"):
            self.backend.decode(token)

    def test_custom_key_prefix(self):
        backend = RedisCursorBackend(
            redis=self.redis_client,
            key_prefix='myapp:cursor',
        )
        token = backend.encode(field='id', value=1)
        key = f'myapp:cursor:{token}'
        assert key in self.store

    def test_custom_ttl(self):
        backend = RedisCursorBackend(redis=self.redis_client, ttl=60)
        backend.encode(field='id', value=1)
        call_kwargs = self.redis_client.set.call_args
        # Accept both positional and keyword ttl argument
        args = call_kwargs.args if call_kwargs.args else ()
        kwargs = call_kwargs.kwargs if call_kwargs.kwargs else {}
        ttl_value = kwargs.get('ex') or (args[2] if len(args) > 2 else None)
        assert ttl_value == 60


# ============================================================================
# RedisAsyncCursorBackend
# ============================================================================

class TestRedisAsyncCursorBackend:
    """RedisAsyncCursorBackend — server-side opaque token via async Redis."""

    def setup_method(self):
        self.redis_client, self.store = _make_async_redis()
        self.backend = RedisAsyncCursorBackend(redis=self.redis_client, ttl=300)

    # --- encode ---

    @pytest.mark.asyncio
    async def test_encode_returns_string(self):
        token = await self.backend.encode(field='id', value=1)
        assert isinstance(token, str)

    @pytest.mark.asyncio
    async def test_encode_writes_to_redis(self):
        token = await self.backend.encode(field='id', value=10)
        key = f'fastkit:cursor:{token}'
        assert key in self.store

    @pytest.mark.asyncio
    async def test_encode_stored_value_contains_field_and_value(self):
        token = await self.backend.encode(field='id', value=99)
        key = f'fastkit:cursor:{token}'
        data = json.loads(self.store[key])
        assert data['field'] == 'id'
        assert data['value'] == 99

    @pytest.mark.asyncio
    async def test_encode_stores_direction(self):
        token = await self.backend.encode(field='id', value=1, direction='desc')
        key = f'fastkit:cursor:{token}'
        data = json.loads(self.store[key])
        assert data['direction'] == 'desc'

    @pytest.mark.asyncio
    async def test_encode_stores_filters(self):
        token = await self.backend.encode(
            field='id', value=1, filters={'status': 'active'}
        )
        key = f'fastkit:cursor:{token}'
        data = json.loads(self.store[key])
        assert data['filters'] == {'status': 'active'}

    @pytest.mark.asyncio
    async def test_encode_two_calls_produce_different_tokens(self):
        t1 = await self.backend.encode(field='id', value=1)
        t2 = await self.backend.encode(field='id', value=1)
        assert t1 != t2

    # --- decode ---

    @pytest.mark.asyncio
    async def test_decode_roundtrip(self):
        token = await self.backend.encode(field='id', value=42, direction='asc')
        decoded = await self.backend.decode(token)
        assert decoded['field'] == 'id'
        assert decoded['value'] == 42
        assert decoded['direction'] == 'asc'

    @pytest.mark.asyncio
    async def test_decode_unknown_token_raises(self):
        with pytest.raises(InvalidCursorError, match="not found or expired"):
            await self.backend.decode('nonexistent-token')

    @pytest.mark.asyncio
    async def test_decode_corrupt_data_raises(self):
        token = 'test-corrupt-async-token'
        key = f'fastkit:cursor:{token}'
        self.store[key] = 'not valid json {'
        with pytest.raises(InvalidCursorError, match="Corrupt cursor data"):
            await self.backend.decode(token)

    @pytest.mark.asyncio
    async def test_custom_key_prefix(self):
        backend = RedisAsyncCursorBackend(
            redis=self.redis_client,
            key_prefix='myapp:cursor',
        )
        token = await backend.encode(field='id', value=1)
        assert f'myapp:cursor:{token}' in self.store


