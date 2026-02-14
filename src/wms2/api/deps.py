from typing import AsyncGenerator

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from wms2.config import Settings
from wms2.db.repository import Repository


async def get_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    session_factory = request.app.state.session_factory
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_repository(session: AsyncSession = Depends(get_session)) -> Repository:
    return Repository(session)


async def get_settings(request: Request) -> Settings:
    return request.app.state.settings
