from __future__ import annotations
from typing import TYPE_CHECKING
import logging

import aio_pika

from fastkit_core.events.consumer import BaseConsumer

if TYPE_CHECKING:
    from fastkit_core.events.backends.rabbitmq import RabbitMQBackend

logger = logging.getLogger(__name__)


class RabbitMQConsumer(BaseConsumer):
    """
    RabbitMQ message consumer.

    Reads from a durable queue bound to the fastkit.signals topic exchange.
    ACKs on successful dispatch, NACKs with requeue=False on failure
    so RabbitMQ routes the message to the DLQ.

    Example:
```python
        consumer = backend.consumer(queue_name='fastkit.consumer')
        asyncio.create_task(consumer.run())
```
    """

    def __init__(self, backend: RabbitMQBackend, queue_name: str) -> None:
        super().__init__(backend)
        self._queue_name = queue_name
        self._queue: aio_pika.Queue | None = None

    async def _connect(self) -> None:
        """
        Declare the consumer queue and bind it to the signals exchange.

        The queue is declared with dead-letter-exchange so failed messages
        are routed to fastkit.dlq automatically by RabbitMQ.
        """
        channel = self._backend._channel
        if channel is None:
            raise RuntimeError(
                "RabbitMQBackend is not initialized. "
                "Call await backend.initialize() before starting the consumer."
            )

        self._queue = await channel.declare_queue(
            self._queue_name,
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'fastkit.dlq',
            },
        )

        # Bind to topic exchange — '#' receives all signals
        await self._queue.bind(
            self._backend._exchange,
            routing_key='#',
        )

    async def _messages(self):
        """Async generator — yields messages from the queue."""
        async with self._queue.iterator() as queue_iter:
            async for message in queue_iter:
                yield message

    async def _ack(self, message: aio_pika.IncomingMessage) -> None:
        await message.ack()

    async def _nack(self, message: aio_pika.IncomingMessage) -> None:
        """Reject without requeue — RabbitMQ routes to DLQ."""
        await message.nack(requeue=False)