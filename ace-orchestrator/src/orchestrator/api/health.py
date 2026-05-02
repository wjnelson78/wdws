from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from orchestrator.api.deps import get_db
from orchestrator.db.session import ace_engine

router = APIRouter(tags=["health"])


@router.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@router.get("/readyz")
async def readyz(db: AsyncSession = Depends(get_db)) -> dict:
    db_ok = False
    ace_ok = False
    try:
        await db.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        pass
    try:
        async with ace_engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        ace_ok = True
    except Exception:
        pass
    return {
        "status": "ready" if (db_ok and ace_ok) else "degraded",
        "db": db_ok,
        "ace": ace_ok,
    }
