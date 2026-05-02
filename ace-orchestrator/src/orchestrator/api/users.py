"""Read-only /v1/users endpoints + cache invalidation."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from orchestrator.api.deps import (
    get_db,
    get_user_context_loader,
    require_api_key,
)
from orchestrator.db.models import User, UserCaseRegistry, UserContext
from orchestrator.schemas.user import (
    RenderedUserContextOut,
    UserCaseOut,
    UserContextOut,
    UserOut,
)
from orchestrator.services.user_context_loader import UserContextLoader

router = APIRouter(
    prefix="/v1/users",
    tags=["users"],
    dependencies=[Depends(require_api_key)],
)


@router.get("", response_model=list[UserOut])
async def list_users(
    include_inactive: bool = False, db: AsyncSession = Depends(get_db)
):
    stmt = select(User).order_by(User.identifier)
    if not include_inactive:
        stmt = stmt.where(User.active.is_(True))
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def _user_or_404(identifier: str, db: AsyncSession) -> User:
    result = await db.execute(select(User).where(User.identifier == identifier))
    user = result.scalar_one_or_none()
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="user not found")
    return user


@router.get("/{identifier}", response_model=UserOut)
async def get_user(identifier: str, db: AsyncSession = Depends(get_db)):
    return await _user_or_404(identifier, db)


@router.get("/{identifier}/context", response_model=RenderedUserContextOut)
async def rendered_user_context(
    identifier: str,
    db: AsyncSession = Depends(get_db),
    loader: UserContextLoader = Depends(get_user_context_loader),
):
    try:
        rendered = await loader.load(db, identifier)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return RenderedUserContextOut(
        user_id=rendered.user_id,
        identifier=rendered.identifier,
        display_name=rendered.display_name,
        preferred_name=rendered.preferred_name,
        role=rendered.role,
        user_profile_block=rendered.user_profile_block,
        active_case_context_block=rendered.active_case_context_block,
        content_hash=rendered.content_hash,
        rendered_at=rendered.rendered_at,
        contexts_count=len(rendered.raw_contexts),
        cases_count=len(rendered.raw_cases),
    )


@router.get("/{identifier}/contexts", response_model=list[UserContextOut])
async def list_user_contexts(
    identifier: str,
    include_inactive: bool = False,
    db: AsyncSession = Depends(get_db),
):
    user = await _user_or_404(identifier, db)
    stmt = select(UserContext).where(UserContext.user_id == user.id)
    if not include_inactive:
        stmt = stmt.where(UserContext.active.is_(True))
    stmt = stmt.order_by(UserContext.context_type, UserContext.priority.desc())
    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.get("/{identifier}/cases", response_model=list[UserCaseOut])
async def list_user_cases(
    identifier: str, db: AsyncSession = Depends(get_db)
):
    user = await _user_or_404(identifier, db)
    result = await db.execute(
        select(UserCaseRegistry)
        .where(UserCaseRegistry.user_id == user.id)
        .order_by(UserCaseRegistry.priority.desc(), UserCaseRegistry.case_id)
    )
    return list(result.scalars().all())


@router.post("/{identifier}/invalidate-cache", status_code=204)
async def invalidate_user_cache(
    identifier: str,
    db: AsyncSession = Depends(get_db),
    loader: UserContextLoader = Depends(get_user_context_loader),
):
    user = await _user_or_404(identifier, db)
    loader.invalidate(user.id)
