"""
Async Generic Repository Pattern for Database Operations.

Provides common CRUD operations and query helpers for async SQLAlchemy.
Full feature parity with sync Repository.
"""

from __future__ import annotations

from typing import Any, Generic, Type, TypeVar, Sequence, Literal

from sqlalchemy import and_, or_, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Load
import inspect

from fastkit_core.database.base import Base
from fastkit_core.database.base_repository import _BaseRepositoryMixin
from fastkit_core.database.cursor_backends import BaseCursorBackend, LocalCursorBackend

T = TypeVar('T', bound=Base)


class AsyncRepository(_BaseRepositoryMixin, Generic[T]):
    """
    Async generic repository for database operations.

    Provides common CRUD operations without writing boilerplate.
    Full async/await support with feature parity to sync Repository.

    Example:
    ```python
        from fastkit_core.database import AsyncRepository
        from sqlalchemy.ext.asyncio import AsyncSession

        # Create repository for User model
        user_repo = AsyncRepository(User, session)

        # Create
        user = await user_repo.create({'name': 'John', 'email': 'john@test.com'})

        # Read
        user = await user_repo.get(1)
        users = await user_repo.get_all()
        active_users = await user_repo.filter(active=True)

        # Update
        user = await user_repo.update(1, {'name': 'Jane'})

        # Delete
        await user_repo.delete(1)

        # Pagination
        users, meta = await user_repo.paginate(page=1, per_page=10)
    ```
    """

    def __init__(
            self,
            model: Type[T],
            session: AsyncSession,
            cursor_backend: BaseCursorBackend | None = None,
    ):
        """
        Initialize async repository.

        Args:
            model: SQLAlchemy model class
            session: Async database session
            cursor_backend: Cursor token storage
        """
        self.model = model
        self.session = session
        self._cursor_backend = cursor_backend or LocalCursorBackend()

    async def _encode_cursor_token(
            self,
            field: str,
            value: Any,
            direction: str = 'asc',
            filters: dict | None = None,
    ) -> str:
        result = self._cursor_backend.encode(
            field=field,
            value=value,
            filters=filters,
            direction=direction,
        )
        if inspect.isawaitable(result):
            return await result
        return result

    async def _decode_cursor_token(self, token: str) -> dict:
        result = self._cursor_backend.decode(token)
        if inspect.isawaitable(result):
            return await result
        return result

    # ========================================================================
    # CREATE
    # ========================================================================

    async def create(self, data: dict[str, Any], commit: bool = True) -> T:
        """
        Create a new record asynchronously.

        Args:
            data: Dictionary of attributes
            commit: Whether to commit immediately

        Returns:
            Created model instance

        Example:
        ```python
            user = await repo.create({'name': 'John', 'email': 'john@test.com'})
        ```
        """
        instance = self.model(**data)
        self.session.add(instance)

        if commit:
            await self.session.commit()
            await self.session.refresh(instance)

        return instance

    async def create_many(
            self,
            data_list: list[dict[str, Any]],
            commit: bool = True
    ) -> list[T]:
        """
        Create multiple records asynchronously.

        Args:
            data_list: List of attribute dictionaries
            commit: Whether to commit immediately

        Returns:
            List of created instances

        Example:
        ```python
            users = await repo.create_many([
                {'name': 'John', 'email': 'john@test.com'},
                {'name': 'Jane', 'email': 'jane@test.com'}
            ])
        ```
        """
        instances = [self.model(**data) for data in data_list]
        self.session.add_all(instances)

        if commit:
            await self.session.commit()
            for instance in instances:
                await self.session.refresh(instance)

        return instances

    # ========================================================================
    # READ
    # ========================================================================

    async def get(self, id: Any, load_relations: Sequence[Load] | None = None) -> T | None:
        """
        Get record by primary key asynchronously.

        Excludes soft-deleted records by default.

        Args:
            id: Primary key value
            load_relations: SQLAlchemy Load objects for eager loading (prevents N+1)


        Returns:
            Model instance or None if not found or soft-deleted

       Example:
        ```python
            # Simple get
            user = await repo.get(1)

            # With eager loading
            from sqlalchemy.orm import selectinload

            invoice = await repo.get(
                invoice_id,
                load_relations=[selectinload(Invoice.items)]
            )

            # Nested relationships
            invoice = await repo.get(
                invoice_id,
                load_relations=[
                    selectinload(Invoice.items).selectinload(InvoiceItem.product)
                ]
            )
        ```
        """
        query = select(self.model).where(self.model.id == id)

        if load_relations:
            query = self._apply_eager_loading(query, load_relations)

        if self._has_soft_delete():
            query = query.where(self.model.deleted_at.is_(None))

        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def get_or_404(self, id: Any, load_relations: Sequence[Load] | None = None  ) -> T:
        """
        Get record by ID or raise exception asynchronously.

        Args:
            id: Primary key value
            load_relations: SQLAlchemy Load objects for eager loading (prevents N+1)

        Returns:
            Model instance

        Raises:
            ValueError: If record not found

        Example:
        ```python
            user = await repo.get_or_404(1)
        ```
        """
        instance = await self.get(id, load_relations=load_relations)
        if instance is None:
            raise ValueError(f"{self.model.__name__} with id={id} not found")
        return instance

    async def get_all(self,
                      limit: int | None = None,
                      load_relations: Sequence[Load] | None = None,
                      _order_by: str | list[str] | None = None
                      ) -> list[T]:
        """
        Get all records asynchronously.

        Args:
            limit: Maximum number of records to return
            load_relations: SQLAlchemy Load objects for eager loading (prevents N+1)
            _order_by: List of columns for ordering
        Returns:
            List of model instances

        Example:
        ```python
            all_users = await repo.get_all()
            first_100 = await repo.get_all(limit=100)
        ```
        """
        query = select(self.model)

        if load_relations:
            query = self._apply_eager_loading(query, load_relations)

        if self._has_soft_delete():
            query = query.where(self.model.deleted_at.is_(None))

        if _order_by:
            query = self._apply_ordering(query, _order_by)

        if limit:
            query = query.limit(limit)

        result = await self.session.execute(query)
        return result.scalars().all()

    async def filter(
            self,
            _limit: int | None = None,
            _offset: int | None = None,
            _order_by: str | list[str] | None = None,
            _load_relations: Sequence[Load] | None = None,
            **filters
    ) -> list[T]:
        """
        Filter records with operator support asynchronously.

        Supports Django-style field lookups:
        - field__operator=value

        Operators:
        - eq: Equal (default if no operator)
        - ne: Not equal
        - lt, lte, gt, gte: Comparisons
        - in, not_in: IN/NOT IN lists
        - like, ilike: LIKE patterns
        - is_null: IS NULL (pass True/False)
        - is_not_null: IS NOT NULL
        - between: BETWEEN (pass tuple/list of 2 values)
        - startswith, endswith, contains: String patterns

        Examples:
        ```python
            # Simple equality
            await repo.filter(status='active')

            # With operators
            await repo.filter(age__gte=18, age__lt=65)
            await repo.filter(email__ilike='%@gmail.com')
            await repo.filter(status__in=['active', 'pending'])
            await repo.filter(deleted_at__is_null=True)
            await repo.filter(price__between=(10, 100))
            await repo.filter(name__startswith='John')

            # With pagination
            await repo.filter(status='active', _limit=10, _offset=20)

            # With ordering
            await repo.filter(age__gte=18, _order_by='name')  # ASC
            await repo.filter(age__gte=18, _order_by='-created_at')  # DESC
        ```
        """
        query = select(self.model)

        if self._has_soft_delete():
            query = query.where(self.model.deleted_at.is_(None))

        # Build WHERE conditions
        conditions = []
        for key, value in filters.items():
            self._parse_field_operator(key, value, conditions)

        # Apply all conditions
        if conditions:
            query = query.where(and_(*conditions))

        if _load_relations:
            query = self._apply_eager_loading(query, _load_relations)

        # Apply ordering
        if _order_by:
            query = self._apply_ordering(query, _order_by)

        # Apply offset
        if _offset:
            query = query.offset(_offset)

        # Apply limit
        if _limit:
            query = query.limit(_limit)

        result = await self.session.execute(query)
        return result.scalars().all()

    async def first(self,
                    _order_by: str | list[str] | None = None,
                    _load_relations: Sequence[Load] | None = None,
                    **filters) -> T | None:
        """
        Get first record matching filters asynchronously.

        Args:
            _order_by: Order by field
            _load_relations: SQLAlchemy Load objects for eager loading (prevents N+1)
            **filters: Filter conditions

        Returns:
            First matching record or None

        Example:
        ```python
            user = await repo.first(email='john@test.com')
            newest = await repo.first(_order_by='-created_at')
        ```
        """
        results = await self.filter(_limit=1, _order_by=_order_by, _load_relations=_load_relations, **filters)
        return results[0] if results else None

    async def exists(self, **filters) -> bool:
        """
        Check if any records match filters asynchronously.

        Args:
            **filters: Filter conditions

        Returns:
            True if any records exist

        Example:
        ```python
            exists = await repo.exists(email='john@test.com')
        ```
        """
        count = await self.count(**filters)
        return count > 0

    async def filter_or(
            self,
            *filter_groups: dict[str, Any],
            _load_relations: Sequence[Load] | None = None,
            _order_by: str | list[str] | None = None,
            **and_filters,
    ) -> list[T]:
        """
        Filter with OR conditions between groups, combined with AND filters.

        Each positional dict is an OR group. Keyword arguments are applied
        as additional AND conditions on top of the OR clause.

        Args:
            *filter_groups: Dicts of filter conditions combined with OR
            _load_relations: SQLAlchemy Load objects for eager loading
            _order_by: Ordering field(s), prefix with - for DESC
            **and_filters: Additional AND conditions (supports operators)

        Returns:
            List of matching model instances

        Example:
    ```python
            # (status='active' OR status='pending') AND age__gte=18
            users = await repo.filter_or(
                {'status': 'active'},
                {'status': 'pending'},
                age__gte=18,
            )
    ```
        """
        query = select(self.model)

        if self._has_soft_delete():
            query = query.where(self.model.deleted_at.is_(None))

        if filter_groups:
            or_conditions = []
            for group in filter_groups:
                group_conditions: list[Any] = []
                for key, value in group.items():
                    self._parse_field_operator(key, value, group_conditions)
                if group_conditions:
                    or_conditions.append(and_(*group_conditions))

            if or_conditions:
                query = query.where(or_(*or_conditions))

        if and_filters:
            and_conditions: list[Any] = []
            for key, value in and_filters.items():
                self._parse_field_operator(key, value, and_conditions)
            if and_conditions:
                query = query.where(and_(*and_conditions))

        if _load_relations:
            query = self._apply_eager_loading(query, _load_relations)

        if _order_by:
            query = self._apply_ordering(query, _order_by)

        result = await self.session.execute(query)
        return result.scalars().all()

    async def count(self, **filters) -> int:
        """
        Count records matching filters asynchronously.

        Args:
            **filters: Filter conditions

        Returns:
            Number of matching records

        Example:
        ```python
            total_active = await repo.count(status='active')
        ```
        """
        query = select(func.count()).select_from(self.model)

        if self._has_soft_delete():
            query = query.where(self.model.deleted_at.is_(None))

        # Build WHERE conditions
        conditions = []
        for key, value in filters.items():
            self._parse_field_operator(key, value, conditions)

        if conditions:
            query = query.where(and_(*conditions))

        result = await self.session.execute(query)
        return result.scalar() or 0

    # ========================================================================
    # UPDATE
    # ========================================================================

    async def update(
            self,
            id: Any,
            data: dict[str, Any],
            commit: bool = True
    ) -> T | None:
        """
        Update record by ID asynchronously.

        Args:
            id: Primary key value
            data: Dictionary of attributes to update
            commit: Whether to commit immediately

        Returns:
            Updated instance or None if not found

        Example:
        ```python
            user = await repo.update(1, {'name': 'Jane'})
        ```
        """
        instance = await self.get(id)

        if instance is None:
            return None

        for key, value in data.items():
            if hasattr(instance, key):
                setattr(instance, key, value)

        if commit:
            await self.session.commit()
            await self.session.refresh(instance)

        return instance

    async def update_many(
            self,
            filters: dict[str, Any],
            data: dict[str, Any],
            commit: bool = True
    ) -> int:
        """
        Update multiple records matching filters asynchronously.

        Args:
            filters: Filter conditions
            data: Data to update
            commit: Whether to commit immediately

        Returns:
            Number of updated records

        Example:
        ```python
            # Deactivate all banned users
            count = await repo.update_many(
                filters={'status': 'banned'},
                data={'active': False}
            )
        ```
        """
        instances = await self.filter(**filters)

        for instance in instances:
            for key, value in data.items():
                if hasattr(instance, key):
                    setattr(instance, key, value)

        if commit:
            await self.session.commit()

        return len(instances)

    # ========================================================================
    # DELETE
    # ========================================================================

    async def delete(self, id: Any, commit: bool = True, force: bool = False) -> bool:
        """
        Delete record by ID asynchronously.

        Uses soft delete if available, unless force=True.

        Args:
            id: Primary key value
            commit: Whether to commit immediately
            force: Force hard delete even if soft delete available

        Returns:
            True if deleted, False if not found

        Example:
        ```python
            # Soft delete (if available)
            deleted = await repo.delete(1)

            # Force hard delete
            deleted = await repo.delete(1, force=True)
        ```
        """
        instance = await self.get(id)

        if instance is None:
            return False

        if hasattr(instance, 'soft_delete') and not force:
            instance.soft_delete()
        else:
            await self.session.delete(instance)

        if commit:
            await self.session.commit()

        return True

    async def delete_many(
            self,
            filters: dict[str, Any],
            commit: bool = True,
            force: bool = False
    ) -> int:
        """
        Delete multiple records matching filters asynchronously.

        Args:
            filters: Filter conditions
            commit: Whether to commit immediately
            force: Force hard delete

        Returns:
            Number of deleted records

        Example:
        ```python
            # Delete all inactive users
            count = await repo.delete_many({'active': False})
        ```
        """
        instances = await self.filter(**filters)

        for instance in instances:
            if hasattr(instance, 'soft_delete') and not force:
                instance.soft_delete()
            else:
                await self.session.delete(instance)

        if commit:
            await self.session.commit()

        return len(instances)

    # ========================================================================
    # PAGINATION
    # ========================================================================

    async def paginate(
            self,
            page: int = 1,
            per_page: int = 20,
            _order_by: str | list[str] | None = None,
            _load_relations: Sequence[Load] | None = None,
            **filters
    ) -> tuple[list[T], dict[str, Any]]:
        """
        Paginate records with metadata asynchronously.

        Excludes soft-deleted records by default.

        Args:
            page: Page number (1-indexed)
            per_page: Items per page
            _order_by: Order by field (prefix with - for DESC)
            _load_relations: SQLAlchemy Load objects for eager loading (prevents N+1)
            **filters: Filter conditions with operators

        Returns:
            Tuple of (items, metadata)

        Example:
        ```python
            # Page 2, 20 items per page, sorted by created_at descending
            users, meta = await repo.paginate(
                page=2,
                per_page=20,
                _order_by='-created_at',
                is_active=True
            )

            # meta = {
            #     'page': 2,
            #     'per_page': 20,
            #     'total': 150,
            #     'total_pages': 8,
            #     'has_next': True,
            #     'has_prev': True
            # }
        ```
        """
        # Get total count (with filters)
        total = await self.count(**filters)

        # Calculate pagination metadata
        offset = (page - 1) * per_page

        # Get items with limit, offset, and ordering
        items = await self.filter(
            _limit=per_page,
            _offset=offset,
            _order_by=_order_by,
            _load_relations=_load_relations,
            **filters
        )

        # Build metadata
        metadata = self._build_pagination_meta(page, per_page, total)

        return items, metadata

    async def cursor_paginate(
            self,
            per_page: int = 20,
            cursor: str | None = None,
            cursor_field: str = 'id',
            direction: Literal['asc', 'desc'] = 'asc',
            _load_relations: Sequence[Load] | None = None,
            **filters
    ) -> tuple[list[T], str | None]:
        """
        Paginate records using cursor-based pagination asynchronously.

        More efficient than offset pagination for large datasets — no COUNT query,
        consistent results even when records are inserted between pages.

        The cursor encodes the position of the last seen record. On each subsequent
        request, only records after that position are returned. The cursor is opaque
        to the client — its internal format depends on the configured cursor backend:
        - LocalCursorBackend (default): base64-encoded JSON, stateless
        - RedisAsyncCursorBackend: random token stored server-side, hides field values

        Args:
            per_page: Number of records per page. Default: 20.
            cursor: Opaque cursor string from a previous response. None for first page.
            cursor_field: Model field used as the cursor anchor. Must be unique and
                monotonically ordered (e.g. 'id', 'created_at'). Default: 'id'.
            direction: Sort direction — 'asc' for oldest-first, 'desc' for newest-first.
                Default: 'asc'.
            _load_relations: SQLAlchemy Load objects for eager loading (prevents N+1).
            **filters: Additional filter conditions with Django-style operator support.

        Returns:
            Tuple of (items, next_cursor) where next_cursor is None on the last page.

        Raises:
            InvalidCursorError: If the cursor token is forged, expired, or malformed.

        Example:
        ```python
            # First page
            products, next_cursor = await repo.cursor_paginate(per_page=20)

            # Next page
            products, next_cursor = await repo.cursor_paginate(
                per_page=20,
                cursor=next_cursor,
            )

            # With filters and custom cursor field
            products, next_cursor = await repo.cursor_paginate(
                per_page=20,
                cursor=next_cursor,
                cursor_field='created_at',
                direction='desc',
                status='active',
            )

            # With eager loading
            from sqlalchemy.orm import selectinload

            products, next_cursor = await repo.cursor_paginate(
                per_page=20,
                _load_relations=[selectinload(Product.category)],
            )
        ```
        """
        if cursor is not None:
            decoded = await self._decode_cursor_token(cursor)
            cursor_field = decoded['field']
            cursor_value = decoded['value']
            if direction == 'asc':
                filters[f'{cursor_field}__gt'] = cursor_value
            elif direction == 'desc':
                filters[f'{cursor_field}__lt'] = cursor_value

        order = f'-{cursor_field}' if direction == 'desc' else cursor_field
        items = await self.filter(
            _limit=per_page + 1,
            _order_by=order,
            _load_relations=_load_relations,
            **filters
        )

        next_cursor = None
        if len(items) > per_page:
            items = items[:per_page]
            next_cursor = await self._encode_cursor_token(
                field=cursor_field,
                value=getattr(items[-1], cursor_field),
                direction=direction
            )

        return items, next_cursor

    # ========================================================================
    # UTILITY
    # ========================================================================

    async def refresh(self, instance: T) -> T:
        """
        Refresh instance from database asynchronously.

        Args:
            instance: Model instance to refresh

        Returns:
            Refreshed instance

        Example:
        ```python
            user = await repo.refresh(user)
        ```
        """
        await self.session.refresh(instance)
        return instance

    async def commit(self) -> None:
        """Commit current transaction asynchronously."""
        await self.session.commit()

    async def rollback(self) -> None:
        """Rollback current transaction asynchronously."""
        await self.session.rollback()

    async def flush(self) -> None:
        """Flush pending changes asynchronously."""
        await self.session.flush()


# ============================================================================
# Async Repository Factory
# ============================================================================

def create_async_repository(model: Type[T], session: AsyncSession) -> AsyncRepository[T]:
    """
    Factory function to create async repository.

    Args:
        model: SQLAlchemy model class
        session: Async database session

    Returns:
        AsyncRepository instance

    Example:
    ```python
        from fastkit_core.database import create_async_repository
        from sqlalchemy.ext.asyncio import AsyncSession

        user_repo = create_async_repository(User, session)
        post_repo = create_async_repository(Post, session)

        # Use it
        users = await user_repo.get_all()
        user = await user_repo.create({'name': 'John'})
    ```
    """
    return AsyncRepository(model, session)