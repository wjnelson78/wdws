"""Bootstrap the v2.0 system prompt into orchestrator.system_prompts + _blocks.

Reads the prompt content from `core.documents` in ACE (read-only via ace_engine),
parses it into the Patch 01 block layout, and persists everything in the
orchestrator state DB. Idempotent — running twice yields the same active set.

Usage:
    .venv/bin/python scripts/bootstrap_system_prompt.py
"""
from __future__ import annotations

import argparse
import asyncio
import re
import sys

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Ensure src is on path when run directly.
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))

from orchestrator.config import get_settings  # noqa: E402
from orchestrator.db.models import SystemPrompt, SystemPromptBlock  # noqa: E402
from orchestrator.db.session import AceSessionLocal, SessionLocal  # noqa: E402
from orchestrator.services.prompt_parser import parse_v2_blocks  # noqa: E402

EXTRA_BLOCKS_DIR = pathlib.Path(__file__).resolve().parent.parent / "data" / "system_prompt_blocks"
_VERSION_SUFFIX_RE = re.compile(r"_v\d+$")


def _block_name_from_filename(path: pathlib.Path) -> str:
    """`onboarding_overlay_v1.md` → `ONBOARDING_OVERLAY`."""
    stem = path.stem
    stem = _VERSION_SUFFIX_RE.sub("", stem)
    return stem.upper()


def load_standalone_blocks() -> list[tuple[str, str]]:
    """Scan the standalone-blocks directory and return [(block_name, content)]."""
    if not EXTRA_BLOCKS_DIR.exists():
        return []
    out: list[tuple[str, str]] = []
    for path in sorted(EXTRA_BLOCKS_DIR.glob("*.md")):
        out.append((_block_name_from_filename(path), path.read_text(encoding="utf-8")))
    return out


async def fetch_v2_content_from_ace(doc_id: str) -> str:
    """Read the full markdown body straight from `core.documents.full_content`."""
    async with AceSessionLocal() as session:
        result = await session.execute(
            text(
                "SELECT title, full_content FROM core.documents WHERE id = :id"
            ),
            {"id": doc_id},
        )
        row = result.first()
        if row is None:
            raise SystemExit(f"ACE doc {doc_id} not found in core.documents")
        title, content = row
        if not content:
            raise SystemExit(
                f"ACE doc {doc_id} ({title!r}) has no full_content; ingestion incomplete"
            )
        return content


def fetch_v2_content_from_disk(path: str) -> str:
    """Fallback: read the canonical source from the repo's data/ dir."""
    return pathlib.Path(path).read_text(encoding="utf-8")


async def persist(version: str, source_doc_id: str, content: str) -> None:
    blocks = parse_v2_blocks(content)
    standalone = load_standalone_blocks()
    print(
        f"Parsed {len(blocks)} v{version} blocks: {[b.name for b in blocks]}"
    )
    if standalone:
        print(
            f"Loaded {len(standalone)} standalone block(s): {[n for n, _ in standalone]}"
        )

    async with SessionLocal() as session:
        await session.execute(
            update(SystemPrompt).where(SystemPrompt.active.is_(True)).values(active=False)
        )

        sp_stmt = pg_insert(SystemPrompt.__table__).values(
            version=version,
            source_doc_id=source_doc_id,
            content=content,
            active=True,
        )
        sp_stmt = sp_stmt.on_conflict_do_update(
            index_elements=[SystemPrompt.version],
            set_={
                "source_doc_id": sp_stmt.excluded.source_doc_id,
                "content": sp_stmt.excluded.content,
                "active": True,
            },
        )
        await session.execute(sp_stmt)

        await session.execute(
            SystemPromptBlock.__table__.delete().where(
                SystemPromptBlock.version == version
            )
        )
        next_sort = 0
        for b in blocks:
            await session.execute(
                SystemPromptBlock.__table__.insert().values(
                    version=version,
                    block_name=b.name,
                    content=b.content,
                    token_count=len(b.content) // 4,
                    cache_ttl=b.cache_ttl,
                    sort_order=b.sort_order,
                )
            )
            next_sort = max(next_sort, b.sort_order + 1)

        for name, body in standalone:
            await session.execute(
                SystemPromptBlock.__table__.insert().values(
                    version=version,
                    block_name=name,
                    content=body,
                    token_count=len(body) // 4,
                    cache_ttl="1h",
                    sort_order=next_sort,
                )
            )
            next_sort += 1

        await session.commit()

        result = await session.execute(
            select(SystemPromptBlock.block_name, SystemPromptBlock.token_count)
            .where(SystemPromptBlock.version == version)
            .order_by(SystemPromptBlock.sort_order)
        )
        for name, tc in result.all():
            print(f"  {name:<32} ~{tc} tokens")


async def main() -> None:
    settings = get_settings()
    version = settings.active_system_prompt_version
    doc_id = settings.athena_system_prompt_doc_id
    print(f"Bootstrapping {version} from ACE doc {doc_id}")

    try:
        content = await fetch_v2_content_from_ace(doc_id)
        print(f"Fetched {len(content)} chars from ACE")
    except SystemExit as exc:
        print(f"ACE fetch failed: {exc}; falling back to data/system_prompt_v2_0.md")
        content = fetch_v2_content_from_disk(
            str(pathlib.Path(__file__).resolve().parent.parent / "data" / "system_prompt_v2_0.md")
        )
        print(f"Loaded {len(content)} chars from disk")

    await persist(version, doc_id, content)
    print(f"\n{version} active.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="(no-op marker; the script is always idempotent and re-runnable)",
    )
    parser.parse_args()
    asyncio.run(main())
