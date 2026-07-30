from __future__ import annotations
from typing import TYPE_CHECKING, AsyncGenerator, Any
import logging

from fastkit_core.events.consumer import BaseConsumer

if TYPE_CHECKING:
    from fastkit_core.events.backends.redis_streams import RedisStreamsBackend

logger = logging.getLogger(__name__)


class RedisStreamsConsumer(BaseConsumer):
    """
    Redis Streams message consumer.

    Uses XREADGROUP for at-least-once delivery within a consumer group.
    ACKs on successful dispatch, moves to DLQ after max_retries failures.

    Example:
```python
        consumer = backend.consumer(consumer_name='worker-1')
        asyncio.create_task(consumer.run())
```
    """

    def __init__(self, backend: RedisStreamsBackend, consumer_name: str) -> None:
        super().__init__(backend)
        self._consumer_name = consumer_name
        self._registered_streams: set[str] = set()

    async def _connect(self) -> None:
        """
        Create consumer group for each registered signal stream.

        MKSTREAM creates the stream if it doesn't exist yet.
        XGROUP CREATE with $ means: start reading only new messages
        (not historical ones that arrived before this consumer started).
        """
        for signal_name in self._backend._receivers.keys():
            stream_key = self._backend._stream_key(signal_name)
            try:
                await self._backend._redis.xgroup_create(
                    stream_key,
                    self._backend._consumer_group,
                    id='$',
                    mkstream=True,
                )
                self._registered_streams.add(stream_key)
                logger.info(
                    "Consumer group '%s' created for stream '%s'",
                    self._backend._consumer_group, stream_key,
                )
            except Exception as e:
                # BUSYGROUP — group already exists, safe to ignore
                if 'BUSYGROUP' in str(e):
                    self._registered_streams.add(stream_key)
                    logger.debug(
                        "Consumer group '%s' already exists for stream '%s'",
                        self._backend._consumer_group, stream_key,
                    )
                else:
                    raise

    async def _messages(self) -> AsyncGenerator[tuple[str, str, dict], None]:
        """
        Async generator — polls XREADGROUP with 5s block timeout.

        Yields tuples of (stream_key, message_id, fields_dict).
        block=5000 means Redis holds the connection open up to 5s
        waiting for new messages — no busy polling.
        """
        streams = {key: '>' for key in self._registered_streams}
        if not streams:
            logger.warning("No streams registered — consumer has nothing to listen to.")
            return

        while True:
            results = await self._backend._redis.xreadgroup(
                self._backend._consumer_group,
                self._consumer_name,
                streams,
                count=10,    # process up to 10 messages per poll
                block=5000,  # wait up to 5s for new messages
            )
            if not results:
                continue

            for stream_key, messages in results:
                for message_id, fields in messages:
                    yield stream_key, message_id, fields

    async def _handle(self, raw_message: Any) -> None:
        """
        Override BaseConsumer._handle to pass message_id for DLQ support.
        """
        stream_key, message_id, fields = raw_message

        try:
            signal_name, payload = self._backend._deserialize_message(fields)
        except Exception as e:
            logger.error("Failed to deserialize message %s: %s", message_id, e)
            await self._nack((stream_key, message_id, fields))
            return

        errors = await self._backend._dispatch(signal_name, payload)

        if errors:
            await self._nack((stream_key, message_id, fields))
        else:
            await self._ack((stream_key, message_id, fields))

    async def _ack(self, message: Any) -> None:
        """XACK — tell Redis this message was successfully processed."""
        stream_key, message_id, fields = message
        signal_name = fields.get(b'signal', b'unknown').decode()

        await self._backend._redis.xack(
            stream_key,
            self._backend._consumer_group,
            message_id,
        )

    async def _nack(self, message: Any) -> None:
        """
        Check pending count via XPENDING.
        Move to DLQ after max_retries, otherwise leave for redelivery.
        """
        stream_key, message_id, fields = message

        try:
            # XPENDING returns pending message info including delivery count
            pending = await self._backend._redis.xpending_range(
                stream_key,
                self._backend._consumer_group,
                min=message_id,
                max=message_id,
                count=1,
            )

            delivery_count = pending[0]['times_delivered'] if pending else 1

            if delivery_count >= self._backend._max_retries:
                # Max retries reached — move to DLQ and ACK to remove from pending
                signal_name = fields.get(b'signal', b'unknown').decode()
                payload_raw = fields.get(b'payload', b'{}')
                payload = __import__('json').loads(payload_raw)

                await self._backend._move_to_dlq(signal_name, payload, message_id)
                await self._backend._redis.xack(
                    stream_key,
                    self._backend._consumer_group,
                    message_id,
                )
                logger.warning(
                    "Message %s moved to DLQ after %d retries",
                    message_id, delivery_count,
                )
            else:
                # Leave in pending list — Redis will redeliver on next XREADGROUP
                logger.warning(
                    "Message %s failed (attempt %d/%d) — will retry",
                    message_id, delivery_count, self._backend._max_retries,
                )

        except Exception as e:
            logger.error("Failed to process nack for message %s: %s", message_id, e)