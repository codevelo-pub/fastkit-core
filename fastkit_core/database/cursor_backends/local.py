import base64
import json
from typing import Any

from fastkit_core.database.cursor_backends.base import BaseCursorBackend
from fastkit_core.http import InvalidCursorError


class LocalCursorBackend(BaseCursorBackend):
    """
    Stateless cursor backend — encodes state directly into the token.

    Default backend. No external dependencies. Token is base64(json({...})).
    Note: the cursor value is visible to anyone who decodes the token.
    Use RedisCursorBackend for sensitive datasets.
    """

    async def encode(
            self,
            field: str,
            value: Any,
            filters: dict | None = None,
            direction: str = 'asc',
    ) -> str:
        payload = json.dumps(
            {'field': field, 'value': value, 'direction': direction},
            default=str,
        )
        return base64.urlsafe_b64encode(payload.encode()).decode()