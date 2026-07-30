"""
Tests for broker backends, consumers, and setup_signal_backend.

Coverage:
- setup_signal_backend — swaps global backend, Signal picks up new backend
- RabbitMQBackend — connect/disconnect/receivers, serialize/deserialize,
  send (success, not initialized, publish error), consumer factory
- RabbitMQConsumer — _connect, _ack, _nack, _handle via BaseConsumer
- RedisStreamsBackend — connect/disconnect/receivers, serialize/deserialize,
  send (success, xadd error), _move_to_dlq, consumer factory
- RedisStreamsConsumer — _connect (new group, BUSYGROUP), _ack, _nack
  (retry, max_retries → DLQ), _handle override
- BaseConsumer — _handle dispatches, nacks on deserialize error, nacks on dispatch error

All broker tests use mocks — no real RabbitMQ or Redis required.
"""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
from collections import defaultdict

import fastkit_core.events.signal as signal_module
from fastkit_core.events import Signal, setup_signal_backend
from fastkit_core.events.backends.inprocess import InProcessBackend
from fastkit_core.events.backends.rabbitmq import RabbitMQBackend
from fastkit_core.events.backends.redis_streams import RedisStreamsBackend
from fastkit_core.events.consumer import BaseConsumer
from fastkit_core.events.consumer_rabbitmq import RabbitMQConsumer
from fastkit_core.events.consumer_redis_streams import RedisStreamsConsumer


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture(autouse=True)
def reset_backend():
    """Reset global backend singleton before every test."""
    signal_module._backend_instance = None
    yield
    signal_module._backend_instance = None


def _make_rabbitmq_backend() -> RabbitMQBackend:
    """RabbitMQBackend with a fake initialized exchange."""
    backend = RabbitMQBackend(url='amqp://guest:guest@localhost/')
    backend._exchange = AsyncMock()
    backend._channel = AsyncMock()
    return backend


def _make_redis_backend() -> tuple[RedisStreamsBackend, MagicMock]:
    """RedisStreamsBackend with a fake async Redis client."""
    redis = AsyncMock()
    backend = RedisStreamsBackend(redis=redis)
    return backend, redis
