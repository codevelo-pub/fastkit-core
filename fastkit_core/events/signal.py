from typing import Callable, Any, Generator
import warnings
import dataclasses
from pydantic import BaseModel
from contextlib import contextmanager

from fastkit_core.events.backends.base import BaseSignalBackend
from fastkit_core.events.backends.inprocess import InProcessBackend

_backend_instance: BaseSignalBackend | None = None

class Signal:

    @staticmethod
    def _get_backend() -> BaseSignalBackend:
        global _backend_instance
        if _backend_instance is None:
            _backend_instance = InProcessBackend()
        return _backend_instance

    def __init__(self, name: str):
        self.name = name

    @property
    def _backend(self) -> BaseSignalBackend:
        return self._get_backend()

    def connect(self, receiver: Callable) -> Callable:
        self._backend.connect(self.name, receiver)
        return receiver

    def disconnect(self, receiver: Callable) -> None:
        self._backend.disconnect(self.name, receiver)

    async def send(self, payload: Any = None, **kwargs) -> list[Exception]:
        self._warn_if_payload_not_serializable(payload)
        return await self._backend.send(self.name, payload, **kwargs)

    @contextmanager
    def connected_to(self, receiver: Callable) -> Generator[None, None, None]:
        self.connect(receiver)
        try:
            yield
        finally:
            self.disconnect(receiver)

    @property
    def receivers(self) -> list[Callable]:
        return self._backend.receivers(self.name)

    def __bool__(self) -> bool:
        return len(self.receivers) > 0

    def __repr__(self) -> str:
        return f"Signal(name={self.name!r}, receivers={len(self.receivers)})"

    @staticmethod
    def _warn_if_payload_not_serializable(payload: Any) -> None:
        if payload is None:
            return
        if isinstance(payload, (dict, BaseModel)) or dataclasses.is_dataclass(payload):
            return
        warnings.warn(
            f"Signal payload of type '{type(payload).__name__}' may not be "
            "serializable to a message broker. Use dict, dataclass, or Pydantic model "
            "for forward compatibility with 0.5.0 broker backends.",
            UserWarning,
            stacklevel=3
        )

def setup_signal_backend(backend: BaseSignalBackend) -> None:
    """
    Configure the global signal backend.

    Call once at application startup, before any Signal instances are created
    or any signals are sent.

    Args:
        backend: Any BaseSignalBackend implementation.

    Example:
        from fastkit_core.events import setup_signal_backend
        from fastkit_core.events.backends.rabbitmq import RabbitMQBackend

        setup_signal_backend(RabbitMQBackend(url='amqp://guest:guest@localhost/'))
    """
    global _backend_instance
    _backend_instance = backend