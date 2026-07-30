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


# ============================================================================
# setup_signal_backend
# ============================================================================

class TestSetupSignalBackend:
    """setup_signal_backend swaps the global backend for all Signal instances."""

    def test_setup_replaces_default_inprocess_backend(self):
        """After setup, Signal._backend must be the configured backend."""
        custom = InProcessBackend()
        setup_signal_backend(custom)
        s = Signal('evt')
        assert s._backend is custom

    def test_signal_created_before_setup_uses_new_backend(self):
        """
        Signal created before setup must pick up the new backend
        because _backend is a property that always reads the global.
        """
        s = Signal('evt')
        custom = InProcessBackend()
        setup_signal_backend(custom)
        assert s._backend is custom

    def test_setup_with_rabbitmq_backend(self):
        backend = _make_rabbitmq_backend()
        setup_signal_backend(backend)
        s = Signal('evt')
        assert isinstance(s._backend, RabbitMQBackend)

    def test_setup_with_redis_streams_backend(self):
        backend, _ = _make_redis_backend()
        setup_signal_backend(backend)
        s = Signal('evt')
        assert isinstance(s._backend, RedisStreamsBackend)

    def test_setup_called_twice_uses_last_backend(self):
        """Last call to setup_signal_backend wins."""
        first = InProcessBackend()
        second = InProcessBackend()
        setup_signal_backend(first)
        setup_signal_backend(second)
        s = Signal('evt')
        assert s._backend is second

    @pytest.mark.asyncio
    async def test_signal_send_uses_configured_backend(self):
        """Signal.send() must dispatch through the configured backend."""
        custom = InProcessBackend()
        setup_signal_backend(custom)

        s = Signal('evt')
        received = []

        @s.connect
        async def handler(payload, **kwargs):
            received.append(payload)

        await s.send({'key': 'value'})
        assert received == [{'key': 'value'}]


# ============================================================================
# RabbitMQBackend — receiver management
# ============================================================================

class TestRabbitMQBackendReceivers:
    """connect / disconnect / receivers mirror InProcessBackend behavior."""

    def test_connect_registers_receiver(self):
        backend = _make_rabbitmq_backend()

        async def handler(p): pass

        backend.connect('user.created', handler)
        assert handler in backend.receivers('user.created')

    def test_connect_does_not_add_duplicates(self):
        backend = _make_rabbitmq_backend()

        async def handler(p): pass

        backend.connect('evt', handler)
        backend.connect('evt', handler)
        assert backend.receivers('evt').count(handler) == 1

    def test_disconnect_removes_receiver(self):
        backend = _make_rabbitmq_backend()

        async def handler(p): pass

        backend.connect('evt', handler)
        backend.disconnect('evt', handler)
        assert handler not in backend.receivers('evt')

    def test_disconnect_nonexistent_does_not_raise(self):
        backend = _make_rabbitmq_backend()

        async def handler(p): pass

        backend.disconnect('evt', handler)

    def test_receivers_returns_empty_for_unknown_signal(self):
        backend = _make_rabbitmq_backend()
        assert backend.receivers('nonexistent') == []

    def test_receivers_different_signals_are_independent(self):
        backend = _make_rabbitmq_backend()

        async def h(p): pass

        backend.connect('a', h)
        assert h not in backend.receivers('b')


# ============================================================================
# RabbitMQBackend — serialization
# ============================================================================

class TestRabbitMQBackendSerialization:

    def test_serialize_message_returns_bytes(self):
        backend = _make_rabbitmq_backend()
        result = backend._serialize_message('user.created', {'id': 1})
        assert isinstance(result, bytes)

    def test_serialize_message_envelope_structure(self):
        backend = _make_rabbitmq_backend()
        result = backend._serialize_message('user.created', {'id': 1})
        envelope = json.loads(result)
        assert envelope['signal'] == 'user.created'
        assert envelope['payload'] == {'id': 1}

    def test_deserialize_message_roundtrip(self):
        backend = _make_rabbitmq_backend()
        body = backend._serialize_message('order.paid', {'amount': 99})
        # Simulate aio_pika.IncomingMessage
        raw = MagicMock()
        raw.body = body
        signal_name, payload = backend._deserialize_message(raw)
        assert signal_name == 'order.paid'
        assert payload == {'amount': 99}

    def test_serialize_non_serializable_uses_default_str(self):
        """Non-JSON-serializable values fall back to str() via default=str."""
        from datetime import datetime
        backend = _make_rabbitmq_backend()
        dt = datetime(2026, 1, 1)
        result = backend._serialize_message('evt', {'ts': dt})
        envelope = json.loads(result)
        assert '2026' in envelope['payload']['ts']


# ============================================================================
# RabbitMQBackend — send
# ============================================================================

class TestRabbitMQBackendSend:

    @pytest.mark.asyncio
    async def test_send_publishes_to_exchange(self):
        backend = _make_rabbitmq_backend()
        result = await backend.send('user.created', {'id': 1})
        assert result == []
        backend._exchange.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_uses_signal_name_as_routing_key(self):
        backend = _make_rabbitmq_backend()
        await backend.send('order.paid', {'amount': 50})
        _, kwargs = backend._exchange.publish.call_args
        assert kwargs.get('routing_key') == 'order.paid'

    @pytest.mark.asyncio
    async def test_send_raises_if_not_initialized(self):
        backend = RabbitMQBackend(url='amqp://localhost/')
        # _exchange is None — not initialized
        with pytest.raises(RuntimeError, match='not initialized'):
            await backend.send('evt', {})

    @pytest.mark.asyncio
    async def test_send_returns_exception_on_publish_failure(self):
        backend = _make_rabbitmq_backend()
        backend._exchange.publish.side_effect = ConnectionError("broker down")
        result = await backend.send('evt', {})
        assert len(result) == 1
        assert isinstance(result[0], ConnectionError)

    @pytest.mark.asyncio
    async def test_send_does_not_raise_on_publish_failure(self):
        """Publish errors are returned, never raised."""
        backend = _make_rabbitmq_backend()
        backend._exchange.publish.side_effect = RuntimeError("timeout")
        # Must not raise
        await backend.send('evt', {})

    @pytest.mark.asyncio
    async def test_send_message_is_persistent(self):
        """Messages must use PERSISTENT delivery mode to survive broker restart."""
        import aio_pika
        backend = _make_rabbitmq_backend()
        await backend.send('evt', {'x': 1})
        message = backend._exchange.publish.call_args[0][0]
        assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
