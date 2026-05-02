"""Patch 02 §B.5 — /v1/principals/* config + onboarding endpoints.

Per Patch 02 §A.4, this lands as additive routes alongside the existing
/v1/users/* surface. Phase 3 will rename /v1/users/* → /v1/principals/*.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from orchestrator.api.deps import (
    get_db,
    get_user_context_loader,
    require_api_key,
)
from orchestrator.db.models import User
from orchestrator.schemas.config import ConfigBundleOut, ConfigValueIn, ConfigValueOut
from orchestrator.services.config_resolver import PARAMETERS, get_config_resolver
from orchestrator.services.user_context_loader import UserContextLoader

router = APIRouter(
    prefix="/v1/principals",
    tags=["principals"],
    dependencies=[Depends(require_api_key)],
)


async def _principal_or_404(identifier: str, db: AsyncSession) -> User:
    result = await db.execute(select(User).where(User.identifier == identifier))
    user = result.scalar_one_or_none()
    if user is None:
        raise HTTPException(status_code=404, detail=f"principal {identifier!r} not found")
    return user


@router.get("/{identifier}/config", response_model=ConfigBundleOut)
async def get_full_config(identifier: str, db: AsyncSession = Depends(get_db)):
    user = await _principal_or_404(identifier, db)
    resolver = get_config_resolver()
    values = await resolver.get_all(user.id)
    return ConfigBundleOut(user_identifier=identifier, parameters=values)


@router.get(
    "/{identifier}/config/{parameter_path:path}", response_model=ConfigValueOut
)
async def get_one_parameter(
    identifier: str, parameter_path: str, db: AsyncSession = Depends(get_db)
):
    if parameter_path not in PARAMETERS:
        raise HTTPException(
            status_code=400,
            detail=f"unknown parameter {parameter_path!r}; not in registry",
        )
    user = await _principal_or_404(identifier, db)
    resolver = get_config_resolver()
    value = await resolver.get(user.id, parameter_path)
    # Look up the row source if any (resolver doesn't expose it).
    from orchestrator.db.memory_models import SystemConfig
    from orchestrator.db.session import MemorySessionLocal

    async with MemorySessionLocal() as ms:
        r = await ms.execute(
            select(SystemConfig.source).where(
                SystemConfig.user_id == user.id,
                SystemConfig.parameter_path == parameter_path,
            )
        )
        source = r.scalar_one_or_none() or "default"
    return ConfigValueOut(parameter_path=parameter_path, value=value, source=source)


@router.put(
    "/{identifier}/config/{parameter_path:path}", response_model=ConfigValueOut
)
async def set_one_parameter(
    identifier: str,
    parameter_path: str,
    body: ConfigValueIn,
    db: AsyncSession = Depends(get_db),
):
    if parameter_path not in PARAMETERS:
        raise HTTPException(
            status_code=400,
            detail=f"unknown parameter {parameter_path!r}; not in registry",
        )
    user = await _principal_or_404(identifier, db)
    resolver = get_config_resolver()
    await resolver.set(user.id, parameter_path, body.value, source=body.source)
    return ConfigValueOut(
        parameter_path=parameter_path, value=body.value, source=body.source
    )


@router.delete(
    "/{identifier}/config/{parameter_path:path}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_one_parameter(
    identifier: str, parameter_path: str, db: AsyncSession = Depends(get_db)
):
    if parameter_path not in PARAMETERS:
        raise HTTPException(
            status_code=400,
            detail=f"unknown parameter {parameter_path!r}; not in registry",
        )
    user = await _principal_or_404(identifier, db)
    from orchestrator.db.memory_models import SystemConfig
    from orchestrator.db.session import MemorySessionLocal

    async with MemorySessionLocal() as ms:
        await ms.execute(
            SystemConfig.__table__.delete().where(
                SystemConfig.user_id == user.id,
                SystemConfig.parameter_path == parameter_path,
            )
        )
        await ms.commit()
    get_config_resolver().invalidate(user.id)


@router.post("/{identifier}/onboarding/complete", status_code=status.HTTP_204_NO_CONTENT)
async def complete_onboarding(
    identifier: str,
    db: AsyncSession = Depends(get_db),
    loader: UserContextLoader = Depends(get_user_context_loader),
):
    """Patch 02 §C.5: flip preferences.onboarding_incomplete to false."""
    user = await _principal_or_404(identifier, db)
    prefs = dict(user.preferences or {})
    prefs["onboarding_incomplete"] = False
    user.preferences = prefs
    user.updated_at = text("now()")  # type: ignore[assignment]
    await db.flush()
    await db.commit()
    loader.invalidate(user.id)
