from abc import ABC, abstractmethod
from typing import Any, Callable
import asyncio
import logging

logger = logging.getLogger(__name__)


class BaseSignalBackend(ABC):

    @abstractmethod
    async def send(self, signal_name: str, payload: Any, **kwargs) -> list[Exception]:
        """
        Send signal to all connected receivers.

        Receiver exceptions are caught and returned — never propagated to the sender.
        Returns list of exceptions from failed receivers, empty list if all succeeded.
        """
        pass

    @abstractmethod
    def connect(self, signal_name: str, receiver: Callable) -> None:
        pass

    @abstractmethod
    def disconnect(self, signal_name: str, receiver: Callable) -> None:
        pass

    @abstractmethod
    def receivers(self, signal_name: str) -> list[Callable]:
        """Return all receivers connected to this signal."""
        pass

    async def _dispatch(self, signal_name: str, payload: Any, **kwargs,) -> list[Exception]:
        """
        Dispatch payload to all local receivers for signal_name.

        Exceptions from individual receivers are caught, logged, and collected.
        All receivers always run — one failure does not prevent others.
        """
        errors = []
        for receiver in self.receivers(signal_name):
            try:
                if asyncio.iscoroutinefunction(receiver):
                    await receiver(payload, **kwargs)
                else:
                    receiver(payload, **kwargs)
            except Exception as e:
                logger.error(
                    "Receiver '%s' for signal '%s' raised: %s",
                    getattr(receiver, '__name__', repr(receiver)),
                    signal_name, e,
                    exc_info=True,
                )
                errors.append(e)
        return errors