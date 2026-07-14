from abc import ABC, abstractmethod
from typing import Any

class BaseCursorBackend(ABC):
    """
    Interface for cursor encoding and decoding in cursor_paginate().

    Implementations may be stateless (encode the value directly into the token)
    or stateful (store metadata server-side and return an opaque token).
    """

    @abstractmethod
    async def encode(
            self,
            field: str,
            value: Any,
            filters: dict | None = None,
            direction: str = 'asc'
    ) -> str:
        """
        Encode cursor state into an opaque token string.

        Args:
            field:     The cursor field name (e.g. 'id', 'created_at').
            value:     The field value at the current page boundary.
            filters:   Active query filters — stored to detect filter tampering.
            direction: 'asc' or 'desc'.

        Returns:
            URL-safe token string safe to send to the client.
        """
        pass