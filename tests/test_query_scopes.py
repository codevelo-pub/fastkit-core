"""
Tests for Query Scopes feature.

Coverage:
- QueryScope protocol structural typing
- add_scope: single, multiple, duplicate
- remove_scope: by type, nonexistent, multiple types
- without_scope: single scope, multiple scopes, scope restoration after block,
  restoration on exception, nested blocks
- _apply_scopes integration with all query methods:
  get, get_all, filter, filter_or, count, exists, paginate,
  cursor_paginate, first
- Sync Repository and AsyncRepository have feature parity
"""

import pytest
import pytest_asyncio
from typing import Any

from sqlalchemy import String, Integer, select
from sqlalchemy.orm import Mapped, mapped_column, Session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from fastkit_core.database import Base, IntIdMixin, Repository, AsyncRepository
from fastkit_core.database.scopes import QueryScope


# ============================================================================
# Test models
# ============================================================================

class ScopedProduct(Base, IntIdMixin):
    """Product model used across all scope tests."""
    __tablename__ = 'scoped_products'

    name: Mapped[str] = mapped_column(String(100))
    category: Mapped[str] = mapped_column(String(50))
    tenant_id: Mapped[int] = mapped_column(Integer)
    price: Mapped[int] = mapped_column(Integer, default=0)
    is_active: Mapped[bool] = mapped_column(default=True)


# ============================================================================
# Scope implementations used in tests
# ============================================================================

class TenantScope:
    """Filters records by tenant_id — simulates SaaS agency scope."""

    def __init__(self, tenant_id: int) -> None:
        self._tenant_id = tenant_id

    def apply(self, query: Any, model: Any) -> Any:
        return query.where(model.tenant_id == self._tenant_id)


class CategoryScope:
    """Filters records by category."""

    def __init__(self, category: str) -> None:
        self._category = category

    def apply(self, query: Any, model: Any) -> Any:
        return query.where(model.category == self._category)


class ActiveScope:
    """Filters only active records."""

    def apply(self, query: Any, model: Any) -> Any:
        return query.where(model.is_active.is_(True))


class PriceCapScope:
    """Filters records below a price cap."""

    def __init__(self, max_price: int) -> None:
        self._max_price = max_price

    def apply(self, query: Any, model: Any) -> Any:
        return query.where(model.price <= self._max_price)


# ============================================================================
# Sync fixtures
# ============================================================================

@pytest.fixture
def sync_engine():
    engine = create_engine('sqlite:///:memory:', echo=False)
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def sync_session(sync_engine):
    Session_ = sessionmaker(bind=sync_engine)
    session = Session_()
    yield session
    session.rollback()
    session.close()


@pytest.fixture
def product_repo(sync_session):
    return Repository(ScopedProduct, sync_session)


@pytest.fixture
def seeded_products(product_repo):
    """
    Seed:
      tenant 1 — Electronics: 2 active, 1 inactive
      tenant 1 — Furniture: 1 active
      tenant 2 — Electronics: 2 active
    """
    product_repo.create_many([
        {'name': 'Laptop', 'category': 'Electronics', 'tenant_id': 1, 'price': 1000, 'is_active': True},
        {'name': 'Phone', 'category': 'Electronics', 'tenant_id': 1, 'price': 500, 'is_active': True},
        {'name': 'Old PC', 'category': 'Electronics', 'tenant_id': 1, 'price': 200, 'is_active': False},
        {'name': 'Desk', 'category': 'Furniture', 'tenant_id': 1, 'price': 300, 'is_active': True},
        {'name': 'Monitor', 'category': 'Electronics', 'tenant_id': 2, 'price': 400, 'is_active': True},
        {'name': 'Tablet', 'category': 'Electronics', 'tenant_id': 2, 'price': 600, 'is_active': True},
    ])


# ============================================================================
# Async fixtures
# ============================================================================

@pytest_asyncio.fixture
async def async_engine():
    engine = create_async_engine('sqlite+aiosqlite:///:memory:', echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def async_session(async_engine):
    maker = async_sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as session:
        yield session
        await session.rollback()


@pytest_asyncio.fixture
async def async_product_repo(async_session):
    return AsyncRepository(ScopedProduct, async_session)


@pytest_asyncio.fixture
async def async_seeded_products(async_product_repo):
    await async_product_repo.create_many([
        {'name': 'Laptop', 'category': 'Electronics', 'tenant_id': 1, 'price': 1000, 'is_active': True},
        {'name': 'Phone', 'category': 'Electronics', 'tenant_id': 1, 'price': 500, 'is_active': True},
        {'name': 'Old PC', 'category': 'Electronics', 'tenant_id': 1, 'price': 200, 'is_active': False},
        {'name': 'Desk', 'category': 'Furniture', 'tenant_id': 1, 'price': 300, 'is_active': True},
        {'name': 'Monitor', 'category': 'Electronics', 'tenant_id': 2, 'price': 400, 'is_active': True},
        {'name': 'Tablet', 'category': 'Electronics', 'tenant_id': 2, 'price': 600, 'is_active': True},
    ])


# ============================================================================
# QueryScope protocol
# ============================================================================

class TestQueryScopeProtocol:
    """QueryScope is a Protocol — structural typing, no inheritance required."""

    def test_tenant_scope_satisfies_protocol(self):
        """TenantScope satisfies QueryScope without inheriting from it."""
        scope = TenantScope(1)
        assert callable(scope.apply)

    def test_scope_apply_returns_modified_query(self):
        """apply() must accept (query, model) and return the modified query."""
        scope = TenantScope(1)
        fake_query = select(ScopedProduct)
        result = scope.apply(fake_query, ScopedProduct)
        # Result should differ from original (WHERE clause added)
        assert str(result) != str(fake_query)
        assert 'tenant_id' in str(result)

    def test_multiple_scope_types_satisfy_protocol(self):
        """Any class with apply(query, model) satisfies QueryScope."""
        for scope in [TenantScope(1), CategoryScope('Electronics'), ActiveScope(), PriceCapScope(500)]:
            assert hasattr(scope, 'apply')
            assert callable(scope.apply)


# ============================================================================
# add_scope
# ============================================================================

class TestAddScope:
    """add_scope registers scopes on the repository instance."""

    def test_add_scope_empty_by_default(self, product_repo):
        assert product_repo._scopes == []

    def test_add_scope_single(self, product_repo):
        scope = TenantScope(1)
        product_repo.add_scope(scope)
        assert len(product_repo._scopes) == 1
        assert product_repo._scopes[0] is scope

    def test_add_scope_multiple(self, product_repo):
        product_repo.add_scope(TenantScope(1))
        product_repo.add_scope(CategoryScope('Electronics'))
        assert len(product_repo._scopes) == 2

    def test_add_scope_preserves_order(self, product_repo):
        s1 = TenantScope(1)
        s2 = CategoryScope('Electronics')
        s3 = ActiveScope()
        product_repo.add_scope(s1)
        product_repo.add_scope(s2)
        product_repo.add_scope(s3)
        assert product_repo._scopes[0] is s1
        assert product_repo._scopes[1] is s2
        assert product_repo._scopes[2] is s3

    def test_add_scope_same_type_twice(self, product_repo):
        """Two instances of the same type are both registered."""
        product_repo.add_scope(TenantScope(1))
        product_repo.add_scope(TenantScope(2))
        assert len(product_repo._scopes) == 2

    def test_add_scope_does_not_affect_other_instances(self, sync_session):
        """Scopes are per-instance, not shared across repositories."""
        repo_a = Repository(ScopedProduct, sync_session)
        repo_b = Repository(ScopedProduct, sync_session)
        repo_a.add_scope(TenantScope(1))
        assert repo_b._scopes == []


# ============================================================================
# remove_scope
# ============================================================================

class TestRemoveScope:
    """remove_scope removes all scopes of a given type."""

    def test_remove_scope_single(self, product_repo):
        product_repo.add_scope(TenantScope(1))
        product_repo.remove_scope(TenantScope)
        assert product_repo._scopes == []

    def test_remove_scope_leaves_other_types(self, product_repo):
        product_repo.add_scope(TenantScope(1))
        product_repo.add_scope(CategoryScope('Electronics'))
        product_repo.remove_scope(TenantScope)
        assert len(product_repo._scopes) == 1
        assert isinstance(product_repo._scopes[0], CategoryScope)

    def test_remove_scope_removes_all_instances_of_type(self, product_repo):
        """If two scopes of the same type are registered, both are removed."""
        product_repo.add_scope(TenantScope(1))
        product_repo.add_scope(TenantScope(2))
        product_repo.add_scope(ActiveScope())
        product_repo.remove_scope(TenantScope)
        assert len(product_repo._scopes) == 1
        assert isinstance(product_repo._scopes[0], ActiveScope)

    def test_remove_scope_nonexistent_type_is_noop(self, product_repo):
        """Removing a type that was never registered is a no-op."""
        product_repo.add_scope(TenantScope(1))
        product_repo.remove_scope(CategoryScope)
        assert len(product_repo._scopes) == 1

    def test_remove_scope_on_empty_repo_is_noop(self, product_repo):
        product_repo.remove_scope(TenantScope)
        assert product_repo._scopes == []


# ============================================================================
# without_scope
# ============================================================================

class TestWithoutScope:
    """without_scope is a context manager that temporarily removes scopes."""

    def test_without_scope_removes_scope_inside_block(self, product_repo):
        product_repo.add_scope(TenantScope(1))
        with product_repo.without_scope(TenantScope):
            assert not any(isinstance(s, TenantScope) for s in product_repo._scopes)

    def test_without_scope_restores_scope_after_block(self, product_repo):
        product_repo.add_scope(TenantScope(1))
        with product_repo.without_scope(TenantScope):
            pass
        assert any(isinstance(s, TenantScope) for s in product_repo._scopes)

    def test_without_scope_restores_on_exception(self, product_repo):
        """Scopes must be restored even if the block raises."""
        product_repo.add_scope(TenantScope(1))
        try:
            with product_repo.without_scope(TenantScope):
                raise ValueError("simulated error")
        except ValueError:
            pass
        assert any(isinstance(s, TenantScope) for s in product_repo._scopes)

    def test_without_scope_multiple_types(self, product_repo):
        product_repo.add_scope(TenantScope(1))
        product_repo.add_scope(CategoryScope('Electronics'))
        product_repo.add_scope(ActiveScope())
        with product_repo.without_scope(TenantScope, CategoryScope):
            remaining = product_repo._scopes
            assert not any(isinstance(s, TenantScope) for s in remaining)
            assert not any(isinstance(s, CategoryScope) for s in remaining)
            assert any(isinstance(s, ActiveScope) for s in remaining)
        # All restored
        assert len(product_repo._scopes) == 3

    def test_without_scope_nonexistent_type_is_noop(self, product_repo):
        product_repo.add_scope(TenantScope(1))
        with product_repo.without_scope(CategoryScope):
            assert len(product_repo._scopes) == 1

    def test_without_scope_yields_repo(self, product_repo):
        product_repo.add_scope(TenantScope(1))
        with product_repo.without_scope(TenantScope) as repo:
            assert repo is product_repo

    def test_without_scope_nested_blocks(self, product_repo):
        """Nested without_scope blocks each restore correctly."""
        product_repo.add_scope(TenantScope(1))
        product_repo.add_scope(CategoryScope('Electronics'))

        with product_repo.without_scope(TenantScope):
            assert not any(isinstance(s, TenantScope) for s in product_repo._scopes)

            with product_repo.without_scope(CategoryScope):
                assert product_repo._scopes == []

            # CategoryScope restored after inner block
            assert any(isinstance(s, CategoryScope) for s in product_repo._scopes)

        # TenantScope restored after outer block
        assert any(isinstance(s, TenantScope) for s in product_repo._scopes)
        assert len(product_repo._scopes) == 2

    def test_without_scope_does_not_affect_queries_outside_block(
            self, product_repo, seeded_products
    ):
        """Scope is applied normally before and after the block."""
        product_repo.add_scope(TenantScope(1))

        count_before = product_repo.count()
        with product_repo.without_scope(TenantScope):
            count_inside = product_repo.count()
        count_after = product_repo.count()

        assert count_before == 4  # tenant 1 only
        assert count_inside == 6  # all tenants
        assert count_after == 4  # tenant 1 restored


# ============================================================================
# Sync Repository — scope integration with query methods
# ============================================================================

class TestSyncScopeQueryIntegration:
    """_apply_scopes is called in every read query method."""

    def test_get_all_with_tenant_scope(self, product_repo, seeded_products):
        product_repo.add_scope(TenantScope(1))
        results = product_repo.get_all()
        assert len(results) == 4
        assert all(p.tenant_id == 1 for p in results)

    def test_filter_with_tenant_scope(self, product_repo, seeded_products):
        product_repo.add_scope(TenantScope(1))
        results = product_repo.filter(category='Electronics')
        assert len(results) == 3
        assert all(p.tenant_id == 1 for p in results)

    def test_filter_or_with_tenant_scope(self, product_repo, seeded_products):
        product_repo.add_scope(TenantScope(1))
        results = product_repo.filter_or(
            {'category': 'Electronics'},
            {'category': 'Furniture'},
        )
        assert len(results) == 4
        assert all(p.tenant_id == 1 for p in results)

    def test_count_with_tenant_scope(self, product_repo, seeded_products):
        product_repo.add_scope(TenantScope(1))
        count = product_repo.count()
        assert count == 4

    def test_count_with_additional_filter(self, product_repo, seeded_products):
        product_repo.add_scope(TenantScope(1))
        count = product_repo.count(category='Electronics')
        assert count == 3

    def test_exists_with_tenant_scope(self, product_repo, seeded_products):
        product_repo.add_scope(TenantScope(2))
        assert product_repo.exists(name='Monitor') is True
        assert product_repo.exists(name='Laptop') is False  # Laptop belongs to tenant 1

    def test_first_with_tenant_scope(self, product_repo, seeded_products):
        product_repo.add_scope(TenantScope(2))
        result = product_repo.first()
        assert result is not None
        assert result.tenant_id == 2

    def test_get_with_tenant_scope(self, product_repo, seeded_products):
        """get() by id is also scoped."""
        tenant2_product = product_repo.first(tenant_id=2)
        product_repo.add_scope(TenantScope(1))
        # tenant 1 scope — should not find tenant 2 record
        result = product_repo.get(tenant2_product.id)
        assert result is None

    def test_paginate_with_tenant_scope(self, product_repo, seeded_products):
        product_repo.add_scope(TenantScope(1))
        items, meta = product_repo.paginate(page=1, per_page=2)
        assert len(items) == 2
        assert meta['total'] == 4
        assert all(p.tenant_id == 1 for p in items)

    def test_cursor_paginate_with_tenant_scope(self, product_repo, seeded_products):
        product_repo.add_scope(TenantScope(1))
        items, next_cursor = product_repo.cursor_paginate(per_page=2)
        assert len(items) == 2
        assert all(p.tenant_id == 1 for p in items)

    def test_cursor_paginate_second_page_with_scope(self, product_repo, seeded_products):
        product_repo.add_scope(TenantScope(1))
        _, cursor = product_repo.cursor_paginate(per_page=2)
        items_p2, _ = product_repo.cursor_paginate(per_page=2, cursor=cursor)
        assert all(p.tenant_id == 1 for p in items_p2)

    def test_composed_scopes(self, product_repo, seeded_products):
        """Multiple scopes compose with AND semantics."""
        product_repo.add_scope(TenantScope(1))
        product_repo.add_scope(CategoryScope('Electronics'))
        results = product_repo.get_all()
        assert len(results) == 3
        assert all(p.tenant_id == 1 and p.category == 'Electronics' for p in results)

    def test_three_composed_scopes(self, product_repo, seeded_products):
        product_repo.add_scope(TenantScope(1))
        product_repo.add_scope(CategoryScope('Electronics'))
        product_repo.add_scope(ActiveScope())
        results = product_repo.get_all()
        assert len(results) == 2
        assert all(p.is_active for p in results)

    def test_without_scope_restores_filter_behavior(self, product_repo, seeded_products):
        product_repo.add_scope(TenantScope(1))
        with product_repo.without_scope(TenantScope):
            all_results = product_repo.get_all()
        scoped_results = product_repo.get_all()
        assert len(all_results) == 6
        assert len(scoped_results) == 4

    def test_scope_does_not_affect_write_operations(self, product_repo, seeded_products):
        """Scopes are SELECT-only — create/update/delete use direct session."""
        product_repo.add_scope(TenantScope(2))
        # create should work regardless of scope
        new = product_repo.create({
            'name': 'Keyboard',
            'category': 'Electronics',
            'tenant_id': 1,
            'price': 100,
            'is_active': True,
        })
        assert new.id is not None
        assert new.tenant_id == 1  # tenant 1 even though scope is tenant 2


# ============================================================================
# AsyncRepository — scope integration with query methods
# ============================================================================

class TestAsyncScopeQueryIntegration:
    """Feature parity with sync — scopes must work in all async read methods."""

    @pytest.mark.asyncio
    async def test_get_all_with_tenant_scope(self, async_product_repo, async_seeded_products):
        async_product_repo.add_scope(TenantScope(1))
        results = await async_product_repo.get_all()
        assert len(results) == 4
        assert all(p.tenant_id == 1 for p in results)

    @pytest.mark.asyncio
    async def test_filter_with_tenant_scope(self, async_product_repo, async_seeded_products):
        async_product_repo.add_scope(TenantScope(1))
        results = await async_product_repo.filter(category='Electronics')
        assert len(results) == 3
        assert all(p.tenant_id == 1 for p in results)

    @pytest.mark.asyncio
    async def test_filter_or_with_tenant_scope(self, async_product_repo, async_seeded_products):
        async_product_repo.add_scope(TenantScope(1))
        results = await async_product_repo.filter_or(
            {'category': 'Electronics'},
            {'category': 'Furniture'},
        )
        assert len(results) == 4
        assert all(p.tenant_id == 1 for p in results)

    @pytest.mark.asyncio
    async def test_count_with_tenant_scope(self, async_product_repo, async_seeded_products):
        async_product_repo.add_scope(TenantScope(1))
        count = await async_product_repo.count()
        assert count == 4

    @pytest.mark.asyncio
    async def test_exists_with_tenant_scope(self, async_product_repo, async_seeded_products):
        async_product_repo.add_scope(TenantScope(2))
        assert await async_product_repo.exists(name='Monitor') is True
        assert await async_product_repo.exists(name='Laptop') is False

    @pytest.mark.asyncio
    async def test_first_with_tenant_scope(self, async_product_repo, async_seeded_products):
        async_product_repo.add_scope(TenantScope(2))
        result = await async_product_repo.first()
        assert result is not None
        assert result.tenant_id == 2

    @pytest.mark.asyncio
    async def test_get_with_tenant_scope(self, async_product_repo, async_seeded_products):
        tenant2_product = await async_product_repo.first(tenant_id=2)
        async_product_repo.add_scope(TenantScope(1))
        result = await async_product_repo.get(tenant2_product.id)
        assert result is None

    @pytest.mark.asyncio
    async def test_paginate_with_tenant_scope(self, async_product_repo, async_seeded_products):
        async_product_repo.add_scope(TenantScope(1))
        items, meta = await async_product_repo.paginate(page=1, per_page=2)
        assert len(items) == 2
        assert meta['total'] == 4
        assert all(p.tenant_id == 1 for p in items)

    @pytest.mark.asyncio
    async def test_cursor_paginate_with_tenant_scope(self, async_product_repo, async_seeded_products):
        async_product_repo.add_scope(TenantScope(1))
        items, next_cursor = await async_product_repo.cursor_paginate(per_page=2)
        assert len(items) == 2
        assert all(p.tenant_id == 1 for p in items)

    @pytest.mark.asyncio
    async def test_composed_scopes(self, async_product_repo, async_seeded_products):
        async_product_repo.add_scope(TenantScope(1))
        async_product_repo.add_scope(CategoryScope('Electronics'))
        results = await async_product_repo.get_all()
        assert len(results) == 3
        assert all(p.tenant_id == 1 and p.category == 'Electronics' for p in results)

    @pytest.mark.asyncio
    async def test_without_scope_in_async_context(self, async_product_repo, async_seeded_products):
        """without_scope is a sync context manager — use `with`, not `async with`."""
        async_product_repo.add_scope(TenantScope(1))
        with async_product_repo.without_scope(TenantScope):
            all_results = await async_product_repo.get_all()
        scoped_results = await async_product_repo.get_all()
        assert len(all_results) == 6
        assert len(scoped_results) == 4

    @pytest.mark.asyncio
    async def test_without_scope_restores_on_async_exception(
            self, async_product_repo, async_seeded_products
    ):
        """Scope must be restored even if an awaited call inside the block raises."""
        async_product_repo.add_scope(TenantScope(1))

        class FakeError(Exception):
            pass

        try:
            with async_product_repo.without_scope(TenantScope):
                raise FakeError
        except FakeError:
            pass

        count = await async_product_repo.count()
        assert count == 4  # tenant 1 scope restored

    @pytest.mark.asyncio
    async def test_scope_does_not_affect_create(self, async_product_repo, async_seeded_products):
        async_product_repo.add_scope(TenantScope(2))
        new = await async_product_repo.create({
            'name': 'Keyboard',
            'category': 'Electronics',
            'tenant_id': 1,
            'price': 100,
            'is_active': True,
        })
        assert new.id is not None
        assert new.tenant_id == 1
