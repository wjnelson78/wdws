"""Patch 02 §B.3: seed memory.system_config with current env-var values for every principal.

Idempotent. Re-running upserts the same rows. Skips parameters that already
have a non-default source row (so principal/adaptation overrides are never
clobbered).

Usage:
    .venv/bin/python scripts/bootstrap_user_context.py     # one-time
    .venv/bin/python scripts/seed_default_config.py        # this script
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import sys

from dotenv import load_dotenv
from sqlalchemy import select

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))

from orchestrator.db.memory_models import SystemConfig  # noqa: E402
from orchestrator.db.models import User  # noqa: E402
from orchestrator.db.session import MemorySessionLocal, SessionLocal  # noqa: E402
from orchestrator.services.config_resolver import PARAMETERS  # noqa: E402


async def main() -> None:
    load_dotenv()

    async with SessionLocal() as session:
        result = await session.execute(
            select(User).where(User.account_type == "principal", User.active.is_(True))
        )
        principals = list(result.scalars().all())

    print(f"Seeding default config for {len(principals)} principal(s):")
    for p in principals:
        print(f"  {p.identifier}  ({p.id})")

    inserted = 0
    skipped = 0
    async with MemorySessionLocal() as msession:
        for principal in principals:
            existing_q = await msession.execute(
                select(SystemConfig.parameter_path, SystemConfig.source).where(
                    SystemConfig.user_id == principal.id
                )
            )
            existing = {row[0]: row[1] for row in existing_q.all()}

            for param_path, param_def in PARAMETERS.items():
                if param_path in existing and existing[param_path] != "default":
                    skipped += 1
                    continue

                if param_def.env_var:
                    env_raw = os.getenv(param_def.env_var)
                else:
                    env_raw = None

                if env_raw is not None and env_raw != "":
                    try:
                        value = param_def.parser(env_raw)
                    except Exception:
                        value = param_def.default
                else:
                    value = param_def.default

                if param_path in existing:
                    await msession.execute(
                        SystemConfig.__table__.update()
                        .where(
                            SystemConfig.user_id == principal.id,
                            SystemConfig.parameter_path == param_path,
                        )
                        .values(value=value, source="default")
                    )
                else:
                    msession.add(
                        SystemConfig(
                            user_id=principal.id,
                            parameter_path=param_path,
                            value=value,
                            source="default",
                        )
                    )
                inserted += 1
        await msession.commit()

    print()
    print(f"Wrote {inserted} default config row(s); preserved {skipped} principal/adaptation override(s).")


if __name__ == "__main__":
    asyncio.run(main())
