from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from wms2.config import Settings


def create_engine(settings: Settings):
    return create_async_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        echo=settings.debug,
    )


def create_session_factory(engine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
