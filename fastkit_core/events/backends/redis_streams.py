from __future__ import annotations
import json
import logging
from collections import defaultdict
from typing import Any, Callable

from redis.asyncio import Redis

from fastkit_core.events.backends.base import BaseSignalBackend

logger = logging.getLogger(__name__)

STREAM_PREFIX = 'fastkit:signals'
DLQ_STREAM = 'fastkit:dlq'
CONSUMER_GROUP = 'fastkit-consumers'


class RedisStreamsBackend(BaseSignalBackend):

    def __init__(
            self,
            redis: Redis,
            stream_prefix: str = STREAM_PREFIX,
            consumer_group: str = CONSUMER_GROUP,
            max_retries: int = 3,
    ) -> None:
        self._redis = redis
        self._stream_prefix = stream_prefix
        self._consumer_group = consumer_group
        self._max_retries = max_retries
        self._receivers: dict[str, list[Callable]] = defaultdict(list)

    # ── Public API ────────────────────────────────────────────────────────

    async def send(self, signal_name: str, payload: Any, **kwargs) -> list[Exception]:
        """
            Publish signal to Redis Stream.

            Fire-and-forget — receivers run in the consumer via XREADGROUP.
            Returns [] on successful publish, [exception] if XADD fails.
            """
        try:
            await self._redis.xadd(
                self._stream_key(signal_name),
                {
                    'signal': signal_name,
                    'payload': self._serialize_payload(payload),
                },
            )
            return []
        except Exception as e:
            logger.error(
                "Failed to publish signal '%s' to Redis Streams: %s",
                signal_name, e,
                exc_info=True,
            )
            return [e]

    def connect(self, signal_name: str, receiver: Callable) -> None:
        if receiver not in self._receivers[signal_name]:
            self._receivers[signal_name].append(receiver)

    def disconnect(self, signal_name: str, receiver: Callable) -> None:
        try:
            self._receivers[signal_name].remove(receiver)
        except ValueError:
            pass

    def receivers(self, signal_name: str) -> list[Callable]:
        return list(self._receivers.get(signal_name, []))

    def consumer(self, consumer_name: str = 'worker-1') -> 'RedisStreamsConsumer':
        """Factory — returns a consumer bound to this backend."""
        from fastkit_core.events.consumer_redis_streams import RedisStreamsConsumer
        return RedisStreamsConsumer(self, consumer_name=consumer_name)

    # ── Internal ──────────────────────────────────────────────────────────

    def _stream_key(self, signal_name: str) -> str:
        return f'{self._stream_prefix}:{signal_name}'

    def _serialize_payload(self, payload: Any) -> str:
        return json.dumps(payload, default=str)

    def _deserialize_message(self, raw: dict) -> tuple[str, Any]:
        # raw is the field-value dict from XREADGROUP
        signal_name = raw[b'signal'].decode()
        payload = json.loads(raw[b'payload'])
        return signal_name, payload

    async def _move_to_dlq(self, signal_name: str, payload: Any, message_id: str) -> None:
        """Move a repeatedly-failed message to the dead letter stream."""
        await self._redis.xadd(DLQ_STREAM, {
            'signal': signal_name,
            'payload': self._serialize_payload(payload),
            'original_id': message_id,
        })