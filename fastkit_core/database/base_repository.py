from sqlalchemy.orm import Load
from sqlalchemy import select
from typing import Sequence, Any
from fastkit_core.database.cursor_backends.base import BaseCursorBackend
from fastkit_core.database.scopes import QueryScope


class _BaseRepositoryMixin:
    model: Any

    _cursor_backend: BaseCursorBackend
    _scopes: list[QueryScope]

    LOOKUP_OPERATORS = {
        'eq': lambda col, val: col == val,
        'ne': lambda col, val: col != val,
        'lt': lambda col, val: col < val,
        'lte': lambda col, val: col <= val,
        'gt': lambda col, val: col > val,
        'gte': lambda col, val: col >= val,
        'in': lambda col, val: col.in_(val),
        'not_in': lambda col, val: col.not_in(val),
        'like': lambda col, val: col.like(val),
        'ilike': lambda col, val: col.ilike(val),
        'is_null': lambda col, val: col.is_(None) if val else col.isnot(None),
        'is_not_null': lambda col, val: col.isnot(None),
        'between': lambda col, val: col.between(val[0], val[1]),
        'startswith': lambda col, val: col.like(f'{val}%'),
        'endswith': lambda col, val: col.like(f'%{val}'),
        'contains': lambda col, val: col.like(f'%{val}%'),
    }

    def _apply_ordering(self, query, order_by: str | list[str] | None):
        if not order_by:
            return query

        fields = [order_by] if isinstance(order_by, str) else order_by

        for field in fields:
            if field.startswith('-'):
                col = field[1:]
                if hasattr(self.model, col):
                    query = query.order_by(getattr(self.model, col).desc())
            else:
                if hasattr(self.model, field):
                    query = query.order_by(getattr(self.model, field))

        return query

    def _apply_eager_loading(
            self,
            stmt,
            load: Sequence[Load] | None = None
    ):
        """
        Apply eager loading options to statement.

        Uses SQLAlchemy's native Load objects for type-safe relationship loading.

        Args:
            stmt: SQLAlchemy select statement
            load: Sequence of SQLAlchemy Load objects (selectinload, joinedload, etc.)

        Returns:
            Statement with eager loading options applied

        Example:
            from sqlalchemy.orm import selectinload

            # Single relationship
            stmt = self._apply_eager_loading(
                stmt,
                [selectinload(Invoice.items)]
            )

            # Nested relationships
            stmt = self._apply_eager_loading(
                stmt,
                [selectinload(Invoice.items).selectinload(InvoiceItem.product)]
            )

            # Multiple relationships
            stmt = self._apply_eager_loading(
                stmt,
                [
                    selectinload(Invoice.client),
                    selectinload(Invoice.items).selectinload(InvoiceItem.product)
                ]
            )
        """
        if not load:
            return stmt

        # Simply apply each SQLAlchemy Load object to the statement
        for load_option in load:
            stmt = stmt.options(load_option)

        return stmt

    def _has_soft_delete(self) -> bool:
        """Check if model has soft delete support."""
        return hasattr(self.model, 'deleted_at')

    def query(self):
        """Get query builder for complex queries."""
        return select(self.model)

    def _parse_field_operator(self, key: str, value: Any, conditions: list[Any]):
        # Parse field__operator format
        if '__' in key:
            field_name, operator = key.rsplit('__', 1)
        else:
            field_name = key
            operator = 'eq'  # Default to equality

        # Validate field exists on model
        if not hasattr(self.model, field_name):
            raise ValueError(
                f"Field '{field_name}' does not exist on {self.model.__name__}"
            )

        # Validate operator
        if operator not in self.LOOKUP_OPERATORS:
            raise ValueError(
                f"Unknown operator '{operator}'. "
                f"Available: {', '.join(self.LOOKUP_OPERATORS.keys())}"
            )

        # Get column and apply operator
        column = getattr(self.model, field_name)
        condition = self.LOOKUP_OPERATORS[operator](column, value)
        conditions.append(condition)

    def _build_pagination_meta(
            self,
            page: int,
            per_page: int,
            total: int,
    ) -> dict[str, Any]:
        total_pages = (total + per_page - 1) // per_page if total > 0 else 0
        return {
            'page': page,
            'per_page': per_page,
            'total': total,
            'total_pages': total_pages,
            'has_next': page < total_pages,
            'has_prev': page > 1,
        }

    def add_scope(self, scope: QueryScope) -> None:
        """
        Register a query scope applied to all subsequent queries.

        Scopes are applied in registration order — each scope receives
        the query returned by the previous one.

        Args:
            scope: Any object implementing the QueryScope protocol.

        Example:
        ```python
            repo.add_scope(AgencyScope(current_user.agency_id))
            repo.add_scope(PublishedScope())
         ```
        """
        self._scopes.append(scope)

    def remove_scope(self, scope_type: type) -> None:
        """
        Remove all registered scopes of a given type.

        Useful for admin queries that need to bypass tenant or
        visibility restrictions.

        Args:
            scope_type: The class of the scope to remove.

        Example:
        ```python
            repo.remove_scope(AgencyScope)
            all_items = await repo.get_all()  # bypasses agency filter
        ```
        """
        self._scopes = [s for s in self._scopes if not isinstance(s, scope_type)]

