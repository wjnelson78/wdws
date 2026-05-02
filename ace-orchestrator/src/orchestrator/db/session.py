from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from orchestrator.config import get_settings

_settings = get_settings()

engine = create_async_engine(
    _settings.database_url,
    pool_size=10,
    max_overflow=10,
    pool_pre_ping=True,
)

ace_engine = create_async_engine(
    _settings.ace_database_url,
    pool_size=5,
    max_overflow=5,
    pool_pre_ping=True,
)

# Memory engine uses the scoped memory_rw role for runtime writes; falls back to
# the orchestrator engine when MEMORY_DATABASE_URL is empty (Phase 1 deployments).
memory_engine = (
    create_async_engine(
        _settings.memory_database_url,
        pool_size=5,
        max_overflow=5,
        pool_pre_ping=True,
    )
    if _settings.memory_database_url
    else engine
)

SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
AceSessionLocal = async_sessionmaker(ace_engine, expire_on_commit=False, class_=AsyncSession)
MemorySessionLocal = async_sessionmaker(
    memory_engine, expire_on_commit=False, class_=AsyncSession
)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session


async def get_ace_session() -> AsyncIterator[AsyncSession]:
    async with AceSessionLocal() as session:
        yield session


async def get_memory_session() -> AsyncIterator[AsyncSession]:
    async with MemorySessionLocal() as session:
        yield session
