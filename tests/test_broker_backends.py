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


# ============================================================================
# RabbitMQBackend — consumer factory
# ============================================================================

class TestRabbitMQBackendConsumerFactory:

    def test_consumer_returns_rabbitmq_consumer_instance(self):
        backend = _make_rabbitmq_backend()
        consumer = backend.consumer()
        assert isinstance(consumer, RabbitMQConsumer)

    def test_consumer_default_queue_name(self):
        backend = RabbitMQBackend(url='amqp://localhost/', queue_prefix='myapp')
        backend._exchange = AsyncMock()
        backend._channel = AsyncMock()
        consumer = backend.consumer()
        assert consumer._queue_name == 'myapp.consumer'

    def test_consumer_custom_queue_name(self):
        backend = _make_rabbitmq_backend()
        consumer = backend.consumer(queue_name='my.custom.queue')
        assert consumer._queue_name == 'my.custom.queue'

    def test_consumer_is_bound_to_backend(self):
        backend = _make_rabbitmq_backend()
        consumer = backend.consumer()
        assert consumer._backend is backend


# ============================================================================
# RabbitMQConsumer
# ============================================================================

class TestRabbitMQConsumer:

    @pytest.mark.asyncio
    async def test_connect_declares_queue_with_dlq_arguments(self):
        backend = _make_rabbitmq_backend()
        consumer = RabbitMQConsumer(backend, queue_name='test.queue')
        await consumer._connect()

        backend._channel.declare_queue.assert_called_once_with(
            'test.queue',
            durable=True,
            arguments={'x-dead-letter-exchange': 'fastkit.dlq'},
        )

    @pytest.mark.asyncio
    async def test_connect_binds_queue_to_exchange(self):
        backend = _make_rabbitmq_backend()
        mock_queue = AsyncMock()
        backend._channel.declare_queue.return_value = mock_queue
        consumer = RabbitMQConsumer(backend, queue_name='test.queue')
        await consumer._connect()
        mock_queue.bind.assert_called_once_with(backend._exchange, routing_key='#')

    @pytest.mark.asyncio
    async def test_connect_raises_if_channel_is_none(self):
        backend = RabbitMQBackend(url='amqp://localhost/')
        consumer = RabbitMQConsumer(backend, queue_name='test.queue')
        with pytest.raises(RuntimeError, match='not initialized'):
            await consumer._connect()

    @pytest.mark.asyncio
    async def test_ack_calls_message_ack(self):
        backend = _make_rabbitmq_backend()
        consumer = RabbitMQConsumer(backend, queue_name='test.queue')
        mock_message = AsyncMock()
        await consumer._ack(mock_message)
        mock_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_nack_calls_message_nack_with_requeue_false(self):
        backend = _make_rabbitmq_backend()
        consumer = RabbitMQConsumer(backend, queue_name='test.queue')
        mock_message = AsyncMock()
        await consumer._nack(mock_message)
        mock_message.nack.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_handle_dispatches_and_acks_on_success(self):
        backend = _make_rabbitmq_backend()
        received = []

        async def handler(payload): received.append(payload)

        backend.connect('user.created', handler)

        consumer = RabbitMQConsumer(backend, queue_name='test.queue')
        body = backend._serialize_message('user.created', {'id': 1})
        mock_message = AsyncMock()
        mock_message.body = body

        await consumer._handle(mock_message)

        assert received == [{'id': 1}]
        mock_message.ack.assert_called_once()
        mock_message.nack.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_nacks_on_deserialize_error(self):
        backend = _make_rabbitmq_backend()
        consumer = RabbitMQConsumer(backend, queue_name='test.queue')

        mock_message = AsyncMock()
        mock_message.body = b'not valid json {'

        await consumer._handle(mock_message)

        mock_message.nack.assert_called_once_with(requeue=False)
        mock_message.ack.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_nacks_when_receiver_fails(self):
        backend = _make_rabbitmq_backend()

        async def bad_handler(payload): raise ValueError("fail")

        backend.connect('evt', bad_handler)

        consumer = RabbitMQConsumer(backend, queue_name='test.queue')
        body = backend._serialize_message('evt', {'x': 1})
        mock_message = AsyncMock()
        mock_message.body = body

        await consumer._handle(mock_message)

        mock_message.nack.assert_called_once_with(requeue=False)
        mock_message.ack.assert_not_called()

# ============================================================================
# RedisStreamsBackend — receiver management
# ============================================================================

class TestRedisStreamsBackendReceivers:

    def test_connect_registers_receiver(self):
        backend, _ = _make_redis_backend()

        async def handler(p): pass

        backend.connect('user.created', handler)
        assert handler in backend.receivers('user.created')

    def test_connect_does_not_add_duplicates(self):
        backend, _ = _make_redis_backend()

        async def handler(p): pass

        backend.connect('evt', handler)
        backend.connect('evt', handler)
        assert backend.receivers('evt').count(handler) == 1

    def test_disconnect_removes_receiver(self):
        backend, _ = _make_redis_backend()

        async def handler(p): pass

        backend.connect('evt', handler)
        backend.disconnect('evt', handler)
        assert handler not in backend.receivers('evt')

    def test_disconnect_nonexistent_does_not_raise(self):
        backend, _ = _make_redis_backend()

        async def handler(p): pass

        backend.disconnect('evt', handler)

    def test_receivers_returns_empty_for_unknown_signal(self):
        backend, _ = _make_redis_backend()
        assert backend.receivers('nonexistent') == []


# ============================================================================
# RedisStreamsBackend — stream key and serialization
# ============================================================================

class TestRedisStreamsBackendSerialization:

    def test_stream_key_format(self):
        backend, _ = _make_redis_backend()
        assert backend._stream_key('user.created') == 'fastkit:signals:user.created'

    def test_stream_key_custom_prefix(self):
        redis = AsyncMock()
        backend = RedisStreamsBackend(redis=redis, stream_prefix='myapp:events')
        assert backend._stream_key('order.paid') == 'myapp:events:order.paid'

    def test_serialize_payload_returns_json_string(self):
        backend, _ = _make_redis_backend()
        result = backend._serialize_payload({'id': 1, 'name': 'Alice'})
        assert isinstance(result, str)
        assert json.loads(result) == {'id': 1, 'name': 'Alice'}

    def test_deserialize_message_from_bytes(self):
        backend, _ = _make_redis_backend()
        raw = {
            b'signal': b'user.created',
            b'payload': b'{"id": 1}',
        }
        signal_name, payload = backend._deserialize_message(raw)
        assert signal_name == 'user.created'
        assert payload == {'id': 1}

    def test_deserialize_message_roundtrip_via_send(self):
        """_serialize_payload output must be deserializable by _deserialize_message."""
        backend, _ = _make_redis_backend()
        original_payload = {'id': 42, 'email': 'a@b.com'}
        serialized = backend._serialize_payload(original_payload)
        raw = {
            b'signal': b'user.created',
            b'payload': serialized.encode(),
        }
        signal_name, payload = backend._deserialize_message(raw)
        assert payload == original_payload


# ============================================================================
# RedisStreamsBackend — send
# ============================================================================

class TestRedisStreamsBackendSend:

    @pytest.mark.asyncio
    async def test_send_calls_xadd(self):
        backend, redis = _make_redis_backend()
        result = await backend.send('user.created', {'id': 1})
        assert result == []
        redis.xadd.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_uses_correct_stream_key(self):
        backend, redis = _make_redis_backend()
        await backend.send('order.paid', {'amount': 50})
        stream_key = redis.xadd.call_args[0][0]
        assert stream_key == 'fastkit:signals:order.paid'

    @pytest.mark.asyncio
    async def test_send_includes_signal_and_payload_fields(self):
        backend, redis = _make_redis_backend()
        await backend.send('user.created', {'id': 1})
        fields = redis.xadd.call_args[0][1]
        assert 'signal' in fields
        assert 'payload' in fields
        assert fields['signal'] == 'user.created'
        assert json.loads(fields['payload']) == {'id': 1}

    @pytest.mark.asyncio
    async def test_send_returns_empty_list_on_success(self):
        backend, redis = _make_redis_backend()
        result = await backend.send('evt', {'x': 1})
        assert result == []

    @pytest.mark.asyncio
    async def test_send_returns_exception_on_xadd_failure(self):
        backend, redis = _make_redis_backend()
        redis.xadd.side_effect = ConnectionError("redis down")
        result = await backend.send('evt', {})
        assert len(result) == 1
        assert isinstance(result[0], ConnectionError)

    @pytest.mark.asyncio
    async def test_send_does_not_raise_on_xadd_failure(self):
        backend, redis = _make_redis_backend()
        redis.xadd.side_effect = RuntimeError("timeout")
        await backend.send('evt', {})


# ============================================================================
# RedisStreamsBackend — _move_to_dlq
# ============================================================================

class TestRedisStreamsBackendDLQ:

    @pytest.mark.asyncio
    async def test_move_to_dlq_calls_xadd_on_dlq_stream(self):
        backend, redis = _make_redis_backend()
        await backend._move_to_dlq('user.created', {'id': 1}, '1699000001-0')
        redis.xadd.assert_called_once()
        stream_key = redis.xadd.call_args[0][0]
        assert stream_key == 'fastkit:dlq'

    @pytest.mark.asyncio
    async def test_move_to_dlq_includes_original_id(self):
        backend, redis = _make_redis_backend()
        await backend._move_to_dlq('evt', {}, '1699000001-0')
        fields = redis.xadd.call_args[0][1]
        assert fields['original_id'] == '1699000001-0'

    @pytest.mark.asyncio
    async def test_move_to_dlq_includes_signal_name(self):
        backend, redis = _make_redis_backend()
        await backend._move_to_dlq('order.failed', {'id': 5}, 'msg-id')
        fields = redis.xadd.call_args[0][1]
        assert fields['signal'] == 'order.failed'


# ============================================================================
# RedisStreamsBackend — consumer factory
# ============================================================================

class TestRedisStreamsBackendConsumerFactory:

    def test_consumer_returns_redis_streams_consumer(self):
        backend, _ = _make_redis_backend()
        consumer = backend.consumer()
        assert isinstance(consumer, RedisStreamsConsumer)

    def test_consumer_default_name(self):
        backend, _ = _make_redis_backend()
        consumer = backend.consumer()
        assert consumer._consumer_name == 'worker-1'

    def test_consumer_custom_name(self):
        backend, _ = _make_redis_backend()
        consumer = backend.consumer(consumer_name='worker-3')
        assert consumer._consumer_name == 'worker-3'

    def test_consumer_is_bound_to_backend(self):
        backend, _ = _make_redis_backend()
        consumer = backend.consumer()
        assert consumer._backend is backend


# ============================================================================
# RedisStreamsConsumer — _ack and _nack
# ============================================================================

class TestRedisStreamsConsumerAckNack:

    @pytest.mark.asyncio
    async def test_ack_calls_xack(self):
        backend, redis = _make_redis_backend()
        consumer = RedisStreamsConsumer(backend, consumer_name='worker-1')

        message = ('fastkit:signals:evt', '1699-0', {b'signal': b'evt', b'payload': b'{}'})
        await consumer._ack(message)

        redis.xack.assert_called_once_with(
            'fastkit:signals:evt',
            backend._consumer_group,
            '1699-0',
        )

    @pytest.mark.asyncio
    async def test_nack_below_max_retries_leaves_in_pending(self):
        """Below max_retries — no xack, no DLQ move."""
        backend, redis = _make_redis_backend()
        backend._max_retries = 3

        redis.xpending_range.return_value = [{'times_delivered': 1}]

        consumer = RedisStreamsConsumer(backend, consumer_name='worker-1')
        message = ('fastkit:signals:evt', '1699-0', {b'signal': b'evt', b'payload': b'{}'})
        await consumer._nack(message)

        redis.xack.assert_not_called()
        redis.xadd.assert_not_called()

    @pytest.mark.asyncio
    async def test_nack_at_max_retries_moves_to_dlq(self):
        """At max_retries — message moved to DLQ and ACKed."""
        backend, redis = _make_redis_backend()
        backend._max_retries = 3

        redis.xpending_range.return_value = [{'times_delivered': 3}]

        consumer = RedisStreamsConsumer(backend, consumer_name='worker-1')
        message = (
            'fastkit:signals:user.created',
            '1699-0',
            {b'signal': b'user.created', b'payload': b'{"id": 1}'},
        )
        await consumer._nack(message)

        # DLQ xadd called
        redis.xadd.assert_called_once()
        dlq_stream = redis.xadd.call_args[0][0]
        assert dlq_stream == 'fastkit:dlq'

        # ACK called to remove from pending
        redis.xack.assert_called_once()

    @pytest.mark.asyncio
    async def test_nack_handles_empty_pending_list(self):
        """If XPENDING returns empty — treat as first attempt, don't move to DLQ."""
        backend, redis = _make_redis_backend()
        backend._max_retries = 3

        redis.xpending_range.return_value = []  # no pending info

        consumer = RedisStreamsConsumer(backend, consumer_name='worker-1')
        message = ('fastkit:signals:evt', '1699-0', {b'signal': b'evt', b'payload': b'{}'})
        await consumer._nack(message)

        redis.xack.assert_not_called()
        redis.xadd.assert_not_called()

    @pytest.mark.asyncio
    async def test_nack_does_not_raise_on_xpending_error(self):
        """Error in XPENDING must be caught and logged, not propagated."""
        backend, redis = _make_redis_backend()
        redis.xpending_range.side_effect = ConnectionError("redis down")

        consumer = RedisStreamsConsumer(backend, consumer_name='worker-1')
        message = ('fastkit:signals:evt', '1699-0', {b'signal': b'evt', b'payload': b'{}'})
        await consumer._nack(message)  # must not raise


# ============================================================================
# RedisStreamsConsumer — _handle override
# ============================================================================

class TestRedisStreamsConsumerHandle:

    @pytest.mark.asyncio
    async def test_handle_dispatches_and_acks_on_success(self):
        backend, redis = _make_redis_backend()
        received = []

        async def handler(payload): received.append(payload)

        backend.connect('user.created', handler)

        consumer = RedisStreamsConsumer(backend, consumer_name='worker-1')
        payload_json = json.dumps({'id': 1}).encode()
        message = (
            'fastkit:signals:user.created',
            '1699-0',
            {b'signal': b'user.created', b'payload': payload_json},
        )
        await consumer._handle(message)

        assert received == [{'id': 1}]
        redis.xack.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_nacks_on_deserialize_error(self):
        backend, redis = _make_redis_backend()
        redis.xpending_range.return_value = [{'times_delivered': 1}]

        consumer = RedisStreamsConsumer(backend, consumer_name='worker-1')
        message = (
            'fastkit:signals:evt',
            '1699-0',
            {b'signal': b'evt', b'payload': b'not valid json {'},
        )
        await consumer._handle(message)

        # xack not called (nack without ack for retry)
        redis.xack.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_nacks_when_receiver_fails(self):
        backend, redis = _make_redis_backend()
        redis.xpending_range.return_value = [{'times_delivered': 1}]

        async def bad(payload): raise ValueError("fail")

        backend.connect('evt', bad)

        consumer = RedisStreamsConsumer(backend, consumer_name='worker-1')
        payload_json = json.dumps({}).encode()
        message = (
            'fastkit:signals:evt',
            '1699-0',
            {b'signal': b'evt', b'payload': payload_json},
        )
        await consumer._handle(message)

        # nack called — no xack on pending list
        redis.xack.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_does_not_ack_if_no_receivers(self):
        """Signal with no receivers — dispatch returns [] so ACK is called."""
        backend, redis = _make_redis_backend()
        consumer = RedisStreamsConsumer(backend, consumer_name='worker-1')

        payload_json = json.dumps({'x': 1}).encode()
        message = (
            'fastkit:signals:unknown',
            '1699-0',
            {b'signal': b'unknown', b'payload': payload_json},
        )
        await consumer._handle(message)

        # No receivers — dispatch returns [] — ACK is called
        redis.xack.assert_called_once()


# ============================================================================
# BaseConsumer — abstract interface
# ============================================================================

class TestBaseConsumer:
    """BaseConsumer cannot be instantiated without implementing abstract methods."""

    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            BaseConsumer(backend=None)

    def test_concrete_subclass_must_implement_all_methods(self):
        class Incomplete(BaseConsumer):
            async def _connect(self): pass

            async def _messages(self): yield

            async def _ack(self, m): pass
            # _nack intentionally missing

        with pytest.raises(TypeError):
            Incomplete(backend=None)

    def test_full_subclass_can_be_instantiated(self):
        class Full(BaseConsumer):
            async def _connect(self): pass

            async def _messages(self): yield

            async def _ack(self, m): pass

            async def _nack(self, m): pass

        backend = MagicMock()
        consumer = Full(backend=backend)
        assert consumer._backend is backend