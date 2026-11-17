"""
Database configuration and session management for SQLAlchemy 2.0.

Supports 100% compatibility across SQLite (local development/testing) and
Microsoft SQL Server (production) using environment-driven connection URLs.
"""

import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from dotenv import load_dotenv
from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

load_dotenv()

DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    "sqlite+aiosqlite:///./opc_cycle_reports.db",
)

# Dialect-aware engine arguments to ensure smooth behavior on both SQLite and MSSQL
_is_sqlite = DATABASE_URL.startswith("sqlite")

engine_kwargs: dict = {
    "echo": os.getenv("SQL_ECHO", "false").lower() == "true",
    "future": True,
}

if _is_sqlite:
    # SQLite requires check_same_thread=False for multi-context async access
    engine_kwargs["connect_args"] = {"check_same_thread": False}
else:
    # Connection pool settings suited for MS SQL Server in production
    engine_kwargs.update(
        {
            "pool_pre_ping": True,
            "pool_size": 10,
            "max_overflow": 20,
            "pool_recycle": 1800,
        }
    )

engine: AsyncEngine = create_async_engine(DATABASE_URL, **engine_kwargs)

# Enable WAL mode and foreign key constraints automatically when using SQLite
if _is_sqlite:

    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


class Base(DeclarativeBase):
    """Declarative base class for all SQLAlchemy ORM models."""


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency that yields an asynchronous database session.
    Automatically rolls back on unhandled exceptions and closes the session.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


@asynccontextmanager
async def get_Session_context() -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager for background services (such as the OPC-UA poller)
    to acquire a scoped database session outside of FastAPI request lifecycles.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db() -> None:
    """
    Initialize database tables from SQLAlchemy metadata.
    Note: In production, prefer running Alembic migrations (`alembic upgrade head`).
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
