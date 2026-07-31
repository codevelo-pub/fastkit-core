from abc import ABC, abstractmethod
from typing import Any
import logging

logger = logging.getLogger(__name__)


class BaseConsumer(ABC):
    """
    Base class for broker message consumers.

    Subclasses implement _connect(), _messages(), _ack(), and _nack()
    for their specific broker. The shared run() loop handles
    deserialization, receiver dispatch, and error isolation.
    """

    def __init__(self, backend) -> None:
        self._backend = backend

    async def run(self) -> None:
        """Start the consumer loop. Call from an async background task."""
        await self._connect()
        logger.info("%s started", self.__class__.__name__)
        async for raw_message in self._messages():
            await self._handle(raw_message)

    async def _handle(self, raw_message: Any) -> None:
        """Deserialize and dispatch a single message to local receivers."""
        try:
            signal_name, payload = self._backend._deserialize_message(raw_message)
        except Exception as e:
            logger.error("Failed to deserialize message: %s", e)
            await self._nack(raw_message)
            return

        errors = await self._backend._dispatch(signal_name, payload)

        if errors:
            await self._nack(raw_message)
        else:
            await self._ack(raw_message)

    @abstractmethod
    async def _connect(self) -> None:
        """Establish broker connection and set up queues/streams."""
        ...

    @abstractmethod
    async def _messages(self):
        """Async generator that yields raw broker messages."""
        ...

    @abstractmethod
    async def _ack(self, message: Any) -> None:
        """Acknowledge successful processing."""
        ...

    @abstractmethod
    async def _nack(self, message: Any) -> None:
        """Negative-acknowledge — broker will retry or route to DLQ."""
        ...