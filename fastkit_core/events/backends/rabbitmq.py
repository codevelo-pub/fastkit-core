from __future__ import annotations
import json
import logging
from collections import defaultdict
from typing import Any, Callable

import aio_pika

from fastkit_core.events.backends.base import BaseSignalBackend

logger = logging.getLogger(__name__)

EXCHANGE_NAME = 'fastkit.signals'
DLQ_EXCHANGE_NAME = 'fastkit.dlq'
DLQ_QUEUE_NAME = 'fastkit.dlq'


class RabbitMQBackend(BaseSignalBackend):

    def __init__(
            self,
            url: str,
            exchange_name: str = EXCHANGE_NAME,
            queue_prefix: str = 'fastkit',
            max_retries: int = 3,
    ) -> None:
        self._url = url
        self._exchange_name = exchange_name
        self._queue_prefix = queue_prefix
        self._max_retries = max_retries
        self._receivers: dict[str, list[Callable]] = defaultdict(list)
        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.Channel | None = None
        self._exchange: aio_pika.Exchange | None = None

    # ── Public API ────────────────────────────────────────────────────────

    async def initialize(self) -> None:
        """
        Establish broker connection and declare exchanges and DLQ.
        Call once at application startup before any send() calls.
        """
        self._connection = await aio_pika.connect_robust(self._url)
        self._channel = await self._connection.channel()

        # Declare main topic exchange
        self._exchange = await self._channel.declare_exchange(
            self._exchange_name,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )

        # Declare DLQ exchange and queue
        dlq_exchange = await self._channel.declare_exchange(
            DLQ_EXCHANGE_NAME,
            aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        dlq_queue = await self._channel.declare_queue(
            DLQ_QUEUE_NAME,
            durable=True,
        )
        await dlq_queue.bind(dlq_exchange, routing_key=DLQ_QUEUE_NAME)

    async def send(self, signal_name: str, payload: Any, **kwargs) -> list[Exception]:
        """
        Publish signal to RabbitMQ topic exchange.

        Fire-and-forget from caller's perspective — receivers run in the consumer.
        Returns [] on successful publish, [exception] if publish fails.
        """
        if self._exchange is None:
            raise RuntimeError(
                "RabbitMQBackend is not initialized. "
                "Call await backend.initialize() at application startup."
            )
        try:
            body = self._serialize_message(signal_name, payload)
            message = aio_pika.Message(
                body=body,
                content_type='application/json',
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  # survives broker restart
            )
            await self._exchange.publish(message, routing_key=signal_name)
            return []
        except Exception as e:
            logger.error(
                "Failed to publish signal '%s' to RabbitMQ: %s",
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

    def consumer(self, queue_name: str | None = None) -> 'RabbitMQConsumer':
        """Factory — returns a consumer bound to this backend."""
        from fastkit_core.events.consumer_rabbitmq import RabbitMQConsumer
        return RabbitMQConsumer(self, queue_name=queue_name or f'{self._queue_prefix}.consumer')

    # ── Internal ──────────────────────────────────────────────────────────

    def _serialize_message(self, signal_name: str, payload: Any) -> bytes:
        envelope = {'signal': signal_name, 'payload': payload}
        return json.dumps(envelope, default=str).encode()

    def _deserialize_message(self, raw: aio_pika.IncomingMessage) -> tuple[str, Any]:
        envelope = json.loads(raw.body)
        return envelope['signal'], envelope['payload']
