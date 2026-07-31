import asyncio
from collections import defaultdict
from typing import Any, Callable
import logging

from fastkit_core.events.backends.base import BaseSignalBackend

logger = logging.getLogger(__name__)

class InProcessBackend(BaseSignalBackend):

    def __init__(self):
        self._receivers: dict[str, list[Callable]] = defaultdict(list)

    async def send(self, signal_name: str, payload: Any, **kwargs) -> list[Exception]:
        return await self._dispatch(signal_name, payload, **kwargs)

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