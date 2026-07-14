"""
Generic Repository Pattern for Database Operations.

Provides common CRUD operations and query helpers.
"""

from __future__ import annotations

from typing import Any, Generic, Type, TypeVar, Sequence, Literal

from sqlalchemy import func, select
from sqlalchemy.orm import Session, Load
from sqlalchemy import and_, or_

from fastkit_core.database.base import Base
from fastkit_core.database.base_repository import _BaseRepositoryMixin
from fastkit_core.database.cursor_backends.base import BaseCursorBackend
from fastkit_core.database.cursor_backends.local import LocalCursorBackend

T = TypeVar('T', bound=Base)


class Repository(_BaseRepositoryMixin, Generic[T]):
    """
    Generic repository for database operations.

    Provides common CRUD operations without writing boilerplate.

    Example:
```python
        from fastkit_core.database import Repository
        from sqlalchemy.orm import Session

        # Create repository for User model
        user_repo = Repository(User, session)

        # Create
        user = user_repo.create({'name': 'John', 'email': 'john@test.com'})

        # Read
        user = user_repo.get(1)
        users = user_repo.get_all()
        active_users = user_repo.filter(active=True)

        # Update
        user = user_repo.update(1, {'name': 'Jane'})

        # Delete
        user_repo.delete(1)

        # Pagination
        users, total = user_repo.paginate(page=1, per_page=10)
```
    """

    def __init__(
            self,
            model: Type[T],
            session: Session,
            cursor_backend: BaseCursorBackend | None = None,
    ):
        """
        Initialize repository.

        Args:
            model: SQLAlchemy model class
            session: Database session
            cursor_backend: Cursor token storage
        """
        self.model = model
        self.session = session
        self._cursor_backend = cursor_backend or LocalCursorBackend()

    # ========================================================================
    # CREATE
    # ========================================================================

    def create(self, data: dict[str, Any], commit: bool = True) -> T:
        """
        Create a new record.

        Args:
            data: Dictionary of attributes
            commit: Whether to commit immediately

        Returns:
            Created model instance

        Example:
```python
            user = repo.create({'name': 'John', 'email': 'john@test.com'})
```
        """
        instance = self.model(**data)
        self.session.add(instance)

        if commit:
            self.session.commit()
            self.session.refresh(instance)

        return instance

    def create_many(
            self,
            data_list: list[dict[str, Any]],
            commit: bool = True
    ) -> list[T]:
        """
        Create multiple records.

        Args:
            data_list: List of attribute dictionaries
            commit: Whether to commit immediately

        Returns:
            List of created instances

        Example:
```python
            users = repo.create_many([
                {'name': 'John', 'email': 'john@test.com'},
                {'name': 'Jane', 'email': 'jane@test.com'}
            ])
```
        """
        instances = [self.model(**data) for data in data_list]
        self.session.add_all(instances)

        if commit:
            self.session.commit()
            for instance in instances:
                self.session.refresh(instance)

        return instances

    # ========================================================================
    # READ
    # ========================================================================

    def get(self, id: Any, load_relations: Sequence[Load] | None = None) -> T | None:
        """
        Get record by primary key.

        Excludes soft-deleted records by default.

        Args:
            load_relations: SQLAlchemy Load objects for eager loading (prevents N+1)
            id: Primary key value

        Returns:
            Model instance or None if not found or soft-deleted
        """
        query = select(self.model).where(self.model.id == id)

        if load_relations:
            query = self._apply_eager_loading(query, load_relations)

        # Exclude soft-deleted records
        if self._has_soft_delete():
            query = query.where(self.model.deleted_at.is_(None))

        result = self.session.execute(query)
        return result.scalar_one_or_none()

    def get_or_404(self, id: Any, load_relations: Sequence[Load] | None = None) -> T:
        """
        Get record by ID or raise exception.

        Args:
            id: Primary key value
            load_relations: SQLAlchemy Load objects for eager loading (prevents N+1)
        Returns:
            Model instance

        Raises:
            ValueError: If record not found
        """
        instance = self.get(id, load_relations=load_relations)
        if instance is None:
            raise ValueError(f"{self.model.__name__} with id={id} not found")
        return instance

    def get_all(self,
                limit: int | None = None,
                load_relations: Sequence[Load] | None = None,
                _order_by: str | list[str] | None = None
                ) -> list[T]:
        """
        Get all records.

        Args:
            limit: Maximum number of records to return
            load_relations: SQLAlchemy Load objects for eager loading (prevents N+1)
            _order_by: List of columns for ordering
        Returns:
            List of model instances

        Example:
```python
            all_users = repo.get_all()
            first_100 = repo.get_all(limit=100)
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

        result = self.session.execute(query)
        return result.scalars().all()

    def filter(
            self,
            _limit: int | None = None,
            _offset: int | None = None,
            _order_by: str | list[str] | None = None,
            _load_relations: Sequence[Load] | None = None,
            **filters
    ) -> list[T]:
        """
        Filter records with operator support.

        Supports Django-style field lookups:
        - field__operator=value

        Operators:
        - eq: Equal (default if no operator)
        - ne: Not equal
        - lt, lte, gt, gte: Comparisons
        - in, not_in: IN/NOT IN lists
        - like, ilike: LIKE patterns
        - is_null: IS NULL (pass True/False)
        - is_not_null: IS NOT NULL (pass True/False)
        - between: BETWEEN (pass tuple/list of 2 values)
        - startswith, endswith, contains: String patterns

        Examples:
            # Simple equality (no operator needed)
            repo.filter(status='active')

            # With operators
            repo.filter(age__gte=18, age__lt=65)
            repo.filter(email__ilike='%@gmail.com')
            repo.filter(status__in=['active', 'pending'])
            repo.filter(deleted_at__is_null=True)
            repo.filter(price__between=(10, 100))
            repo.filter(name__startswith='John')

            # With pagination
            repo.filter(status='active', _limit=10, _offset=20)

            # With ordering
            repo.filter(age__gte=18, _order_by='name')  # ASC
            repo.filter(age__gte=18, _order_by='-created_at')  # DESC
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

        # Apply limit and offset
        if _offset:
            query = query.offset(_offset)
        if _limit:
            query = query.limit(_limit)

        # Execute
        result = self.session.execute(query)
        return result.scalars().all()

    def first(self,
              _load_relations: Sequence[Load] | None = None,
              _order_by: str | list[str] | None = None,
              **filters) -> T | None:
        """
        Get first record matching filters.

        Args:
            _load_relations: SQLAlchemy Load objects for eager loading (prevents N+1)
            _order_by: List of columns for ordering
            **filters: Keyword arguments for filtering

        Returns:
            First matching instance or None

        Example:
```python
            user = repo.first(email='john@test.com')
```
        """
        results = self.filter(_limit=1, _load_relations=_load_relations, _order_by=_order_by, **filters)
        return results[0] if results else None

    def exists(self, **filters) -> bool:
        """
        Check if record exists.

        Args:
            **filters: Keyword arguments for filtering

        Returns:
            True if exists, False otherwise

        Example:
```python
            exists = repo.exists(email='john@test.com')
```
        """
        return self.first(**filters) is not None

    def filter_or(
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
            # (status='active' OR status='pending') AND age__gte=18
            users = repo.filter_or(
                {'status': 'active'},
                {'status': 'pending'},
                age__gte=18,
            )
        """
        query = select(self.model)

        if self._has_soft_delete():
            query = query.where(self.model.deleted_at.is_(None))

        # OR conditions — each group is internally AND-ed, groups are OR-ed
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

        # AND conditions on top of OR clause
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

        result = self.session.execute(query)
        return result.scalars().all()

    def count(self, **filters) -> int:
        """
        Count records with operator support.

        Args:
            **filters: Keyword arguments for filtering (supports operators)

        Returns:
            Number of matching records

        Example:
    ```python
            total = repo.count()
            adult_count = repo.count(age__gte=18)
            active_count = repo.count(status='active', deleted_at__is_null=True)
    ```
        """
        query = select(func.count()).select_from(self.model)

        if self._has_soft_delete():
            query = query.where(self.model.deleted_at.is_(None))

        # Build WHERE conditions using operator support
        conditions = []
        for key, value in filters.items():
            self._parse_field_operator(key, value, conditions)

        if conditions:
            query = query.where(and_(*conditions))

        result = self.session.execute(query)
        return result.scalar() or 0

    # ========================================================================
    # UPDATE
    # ========================================================================

    def update(
            self,
            id: Any,
            data: dict[str, Any],
            commit: bool = True
    ) -> T | None:
        """
        Update record by ID.

        Args:
            id: Primary key value
            data: Dictionary of attributes to update
            commit: Whether to commit immediately

        Returns:
            Updated instance or None if not found

        Example:
```python
            user = repo.update(1, {'name': 'Jane'})
```
        """
        instance = self.get(id)

        if instance is None:
            return None

        for key, value in data.items():
            if hasattr(instance, key):
                setattr(instance, key, value)

        if commit:
            self.session.commit()
            self.session.refresh(instance)

        return instance

    def update_many(
            self,
            filters: dict[str, Any],
            data: dict[str, Any],
            commit: bool = True
    ) -> int:
        """
        Update multiple records matching filters.

        Args:
            filters: Filter conditions
            data: Data to update
            commit: Whether to commit immediately

        Returns:
            Number of updated records

        Example:
```python
            # Deactivate all banned users
            count = repo.update_many(
                filters={'status': 'banned'},
                data={'active': False}
            )
```
        """
        instances = self.filter(**filters)

        for instance in instances:
            for key, value in data.items():
                if hasattr(instance, key):
                    setattr(instance, key, value)

        if commit:
            self.session.commit()

        return len(instances)

    # ========================================================================
    # DELETE
    # ========================================================================

    def delete(self, id: Any, commit: bool = True, force: bool = False) -> bool:
        """
        Delete record by ID.

        Args:
            id: Primary key value
            commit: Whether to commit immediately

        Returns:
            True if deleted, False if not found

        Example:
```python
            deleted = repo.delete(1)
```
        :param id:
        :param commit:
        :param force:
        """
        instance = self.get(id)

        if instance is None:
            return False

        if hasattr(instance, 'soft_delete') and not force:
            instance.soft_delete()
        else:
            self.session.delete(instance)

        if commit:
            self.session.commit()

        return True

    def delete_many(
            self,
            filters: dict[str, Any],
            commit: bool = True,
            force: bool = False,
    ) -> int:
        """
        Delete multiple records matching filters.

        Args:
            filters: Filter conditions
            commit: Whether to commit immediately
            force: Force delete

        Returns:
            Number of deleted records

        Example:
```python
            # Delete all inactive users
            count = repo.delete_many({'active': False})
```
        """
        instances = self.filter(**filters)

        for instance in instances:
            if hasattr(instance, 'soft_delete') and not force:
                instance.soft_delete()
            else:
                self.session.delete(instance)

        if commit:
            self.session.commit()

        return len(instances)

    # ========================================================================
    # PAGINATION
    # ========================================================================

    def paginate(
            self,
            page: int = 1,
            per_page: int = 20,
            _order_by: str | list[str] | None = None,
            _load_relations: Sequence[Load] | None = None,
            **filters
    ) -> tuple[list[T], dict[str, Any]]:
        """
        Paginate records with metadata.

        Excludes soft-deleted records by default.

        Args:
            page: Page number (1-indexed)
            per_page: Items per page
            _order_by: List of columns for ordering
            _load_relations: SQLAlchemy Load objects for eager loading (prevents N+1)
            **filters: Filter conditions with operators

        Returns:
            Tuple of (items, metadata)

        Example:
            # Page 2, 20 items per page, sorted by created_at descending
            users, meta = repo.paginate(
                page=2,
                per_page=20,
                _order_by='-created_at',
                is_active=True
            )
        """
        # Get total count (with filters)
        total = self.count(**filters)

        # Calculate pagination metadata
        offset = (page - 1) * per_page

        # Get items with limit, offset, and ordering
        items = self.filter(
            _limit=per_page,
            _offset=offset,
            _order_by=_order_by,
            _load_relations=_load_relations,
            **filters
        )

        # Build metadata
        metadata = self._build_pagination_meta(page, per_page, total)

        return items, metadata

    def cursor_paginate(
            self,
            per_page: int = 20,
            cursor: str | None = None,
            cursor_field: str = 'id',
            direction: Literal['asc', 'desc'] = 'asc',
            _load_relations: Sequence[Load] | None = None,
            **filters
    ) -> tuple[list[T], str | None]:

        if cursor is not None:
            cursor_value = self._cursor_backend.decode(cursor)
            if direction == 'asc':
                filters[f'{cursor_field}__gt'] = cursor_value
            elif direction == 'desc':
                filters[f'{cursor_field}__lt'] = cursor_value

        order = f'-{cursor_field}' if direction == 'desc' else cursor_field
        items = self.filter(
            _limit=per_page + 1,
            _order_by=order,
            _load_relations=_load_relations,
            **filters
        )

        next_cursor = None
        if len(items) > per_page:
            items = items[:per_page]
            next_cursor = self._cursor_backend.encode(
                field=cursor_field,
                value=getattr(items[-1], cursor_field),
                filters=filters,
                direction=direction,
            )

        return items, next_cursor


    # ========================================================================
    # UTILITY
    # ========================================================================

    def refresh(self, instance: T) -> T:
        """
        Refresh instance from database.

        Args:
            instance: Model instance to refresh

        Returns:
            Refreshed instance
        """
        self.session.refresh(instance)
        return instance

    def commit(self) -> None:
        """Commit current transaction."""
        self.session.commit()

    def rollback(self) -> None:
        """Rollback current transaction."""
        self.session.rollback()

    def flush(self) -> None:
        """Flush pending changes."""
        self.session.flush()


# ============================================================================
# Repository Factory
# ============================================================================

def create_repository(model: Type[T], session: Session) -> Repository[T]:
    """
    Factory function to create repository.

    Args:
        model: SQLAlchemy model class
        session: Database session

    Returns:
        Repository instance

    Example:
```python
        from fastkit_core.database import create_repository

        user_repo = create_repository(User, session)
        post_repo = create_repository(Post, session)
```
    """
    return Repository(model, session)