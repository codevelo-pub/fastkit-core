"""
FastKit Database Module

Provides:
- Base model with timestamps and serialization
- Useful mixins (UUID, SoftDelete, Timestamps, Slug, Publishable, etc.)
- Session management with read/write replica support
- Connection manager for multiple databases
- Generic repository pattern
- FastAPI integration

Example:
```python
    from fastkit_core.database import (
        Base,
        DatabaseManager,
        Repository,
        init_database,
        get_db,
        # Mixins
        UUIDMixin,
        SoftDeleteMixin,
        SlugMixin,
        PublishableMixin,
    )

    # Define model
    class User(Base, SoftDeleteMixin):
        name: Mapped[str]
        email: Mapped[str]

    # Initialize database
    config = ConfigManager()
    db = DatabaseManager(config)

    # Use repository
    with db.session() as session:
        user_repo = Repository(User, session)
        user = user_repo.create({'name': 'John', 'email': 'john@test.com'})
```
"""

from fastkit_core.database.base import Base
from fastkit_core.database.base_with_timestamps import BaseWithTimestamps
from fastkit_core.database.manager import (
    ConnectionManager,
    get_connection_manager,
    set_connection_manager,
)
from fastkit_core.database.mixins import (
    PublishableMixin,
    SlugMixin,
    SoftDeleteMixin,
    TimestampMixin,
    UUIDMixin,
    IntIdMixin,
)
from fastkit_core.database.repository import Repository, create_repository
from fastkit_core.database.async_repository import AsyncRepository, create_async_repository

# Synchronous session management
from fastkit_core.database.session import (
    DatabaseManager,
    build_database_url,
    get_db,
    get_db_manager,
    get_read_db,
    health_check_all,
    init_database,
    shutdown_database,
)

# Asynchronous session management
from fastkit_core.database.session import (
    AsyncDatabaseManager,
    get_async_db,
    get_async_db_manager,
    get_async_read_db,
    health_check_all_async,
    init_async_database,
    shutdown_async_database,
)

from fastkit_core.database.translatable import TranslatableMixin, set_locale_from_request

from fastkit_core.database.cursor_backends import (
    BaseCursorBackend,
    LocalCursorBackend,
    RedisCursorBackend,
    RedisAsyncCursorBackend,
)
from fastkit_core.database.exceptions import InvalidCursorError
from fastkit_core.database.scopes import QueryScope

__all__ = [
    # Base
    'Base',
    # Session Management
    'DatabaseManager',
    'init_database',
    'get_db_manager',
    'get_db',
    'get_read_db',
    'shutdown_database',
    'health_check_all',
    'build_database_url',

    # Asynchronous Session Management
    'AsyncDatabaseManager',
    'init_async_database',
    'get_async_db_manager',
    'get_async_db',
    'get_async_read_db',
    'shutdown_async_database',
    'health_check_all_async',

    # Connection Manager
    'ConnectionManager',
    'get_connection_manager',
    'set_connection_manager',
    # Repository
    'Repository',
    'create_repository',
    # Async Repository
    'AsyncRepository',
    'create_async_repository',

    # Mixins
    'IntIdMixin',
    'UUIDMixin',
    'SoftDeleteMixin',
    'TimestampMixin',
    'SlugMixin',
    'PublishableMixin',
    'BaseWithTimestamps',
    # Translations
    'TranslatableMixin',
    'set_locale_from_request',
    # Cursor Pagination
    'BaseCursorBackend',
    'LocalCursorBackend',
    'RedisCursorBackend',
    'RedisAsyncCursorBackend',
    'InvalidCursorError',
    'QueryScope',
]