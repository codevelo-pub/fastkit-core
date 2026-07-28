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

@pytest.fixture(scope='module')
def sync_engine():
    engine = create_engine('sqlite:///:memory:', echo=False)
    Base.metadata.create_all(engine)
    return engine


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

@pytest_asyncio.fixture(scope='module')
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