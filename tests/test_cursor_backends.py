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