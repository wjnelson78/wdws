"""
Athena Cognitive Engine — Legal Module

Provides:
  1. Case-aware RAG — automatically pulls case context before LLM calls
  2. Document template library — fill and render legal templates
  3. Deadline tracking — monitor court deadlines with alerts

Designed to integrate with the existing PostgreSQL schema at /opt/wdws
(legal.cases, legal.case_documents, core.documents, core.document_chunks).

Usage:
    module = LegalModule(pool)

    # Case-aware context for LLM prompts
    context = await module.build_case_context("24-2-01031-31")
    system_prompt = module.inject_case_context(base_prompt, context)

    # Templates
    templates = await module.list_templates(category="motion")
    filled = await module.fill_template(template_id=1, variables={...})

    # Deadlines
    upcoming = await module.get_upcoming_deadlines(days=14)
    alerts = await module.check_deadline_alerts()
"""

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import asyncpg

log = logging.getLogger("athena.legal")


# ── Data Classes ─────────────────────────────────────────────

@dataclass
class CaseContext:
    """Structured case context for LLM prompt injection."""
    case_number: str
    case_title: str
    court: str
    status: str
    parties: list[dict]
    recent_filings: list[dict]
    key_dates: list[dict]
    related_entities: list[str]
    summary_stats: dict

    def to_prompt_block(self) -> str:
        """Format case context as a structured text block for system prompts."""
        lines = [
            f"## Active Case Context: {self.case_title}",
            f"**Case Number:** {self.case_number}",
            f"**Court:** {self.court}",
            f"**Status:** {self.status}",
            "",
        ]

        if self.parties:
            lines.append("### Parties")
            for p in self.parties:
                lines.append(f"- {p.get('name', 'Unknown')} ({p.get('role', 'unknown role')})")
            lines.append("")

        if self.recent_filings:
            lines.append("### Recent Filings (last 10)")
            for f in self.recent_filings[:10]:
                date_str = f.get("date", "unknown date")
                lines.append(f"- [{date_str}] {f.get('title', 'Untitled')} ({f.get('type', '')})")
            lines.append("")

        if self.key_dates:
            lines.append("### Key Dates")
            for d in self.key_dates:
                lines.append(f"- {d.get('date', '?')}: {d.get('description', '')}")
            lines.append("")

        if self.related_entities:
            lines.append(f"### Key Entities: {', '.join(self.related_entities[:20])}")
            lines.append("")

        stats = self.summary_stats
        lines.append(
            f"### Stats: {stats.get('total_documents', 0)} documents, "
            f"{stats.get('total_emails', 0)} emails, "
            f"{stats.get('total_attachments', 0)} attachments"
        )

        return "\n".join(lines)


@dataclass
class DeadlineAlert:
    """A deadline requiring attention."""
    deadline_id: int
    case_number: str
    case_title: str
    title: str
    deadline_date: date
    days_remaining: int
    priority: str
    deadline_type: str
    description: str = ""


@dataclass
class FilledTemplate:
    """A template with variables filled in."""
    template_id: int
    template_name: str
    rendered_body: str
    variables_used: dict[str, str]
    missing_variables: list[str]


# ── Legal Module ─────────────────────────────────────────────

class LegalModule:
    """Legal domain specialist module for Athena."""

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    # ── Case-Aware RAG ───────────────────────────────────────

    async def build_case_context(
        self,
        case_identifier: str,
        max_filings: int = 10,
        max_entities: int = 20,
    ) -> Optional[CaseContext]:
        """Build structured case context from PostgreSQL.

        Args:
            case_identifier: Case number (e.g. "24-2-01031-31") or partial match
            max_filings: Maximum recent filings to include
            max_entities: Maximum entities to include
        """
        async with self.pool.acquire() as conn:
            # Find the case
            case = await conn.fetchrow("""
                SELECT c.id, c.case_number, c.case_title, c.court, c.status,
                       c.filed_date, c.metadata
                FROM legal.cases c
                WHERE c.case_number ILIKE $1
                   OR c.case_title ILIKE $1
                LIMIT 1
            """, f"%{case_identifier}%")

            if not case:
                log.warning("Case not found: %s", case_identifier)
                return None

            case_id = case["id"]

            # Get parties
            parties = await conn.fetch("""
                SELECT p.name, cp.role
                FROM legal.case_parties cp
                JOIN legal.parties p ON cp.party_id = p.id
                WHERE cp.case_id = $1
                ORDER BY cp.role
            """, case_id)

            # Get recent filings (documents linked to this case)
            filings = await conn.fetch("""
                SELECT d.title, d.document_type, d.filename,
                       d.created_at, d.metadata
                FROM legal.case_documents cd
                JOIN core.documents d ON cd.document_id = d.id
                WHERE cd.case_id = $1
                ORDER BY d.created_at DESC
                LIMIT $2
            """, case_id, max_filings)

            # Get entities mentioned in case documents
            entities = await conn.fetch("""
                SELECT DISTINCT e.name
                FROM core.entity_mentions em
                JOIN core.entities e ON em.entity_id = e.id
                JOIN legal.case_documents cd ON em.document_id = cd.document_id
                WHERE cd.case_id = $1
                LIMIT $2
            """, case_id, max_entities)

            # Get stats
            stats = await conn.fetchrow("""
                SELECT
                    COUNT(DISTINCT cd.document_id) AS total_documents,
                    COUNT(DISTINCT CASE WHEN d.document_type = 'email' THEN d.id END) AS total_emails,
                    COUNT(DISTINCT ea.id) AS total_attachments
                FROM legal.case_documents cd
                JOIN core.documents d ON cd.document_id = d.id
                LEFT JOIN legal.email_attachments ea ON ea.email_doc_id = d.id
                WHERE cd.case_id = $1
            """, case_id)

            # Get deadlines
            deadlines = await conn.fetch("""
                SELECT title, deadline_date, deadline_type, status
                FROM legal.case_deadlines
                WHERE case_id = $1 AND status = 'pending'
                ORDER BY deadline_date
                LIMIT 5
            """, case_id)

            key_dates = []
            if case["filed_date"]:
                key_dates.append({
                    "date": str(case["filed_date"]),
                    "description": "Case filed",
                })
            for dl in deadlines:
                key_dates.append({
                    "date": str(dl["deadline_date"]),
                    "description": f"{dl['deadline_type']}: {dl['title']}",
                })

            return CaseContext(
                case_number=case["case_number"],
                case_title=case["case_title"] or case["case_number"],
                court=case["court"] or "Unknown court",
                status=case["status"] or "active",
                parties=[{"name": p["name"], "role": p["role"]} for p in parties],
                recent_filings=[
                    {
                        "title": f["title"] or f["filename"],
                        "type": f["document_type"],
                        "date": str(f["created_at"].date()) if f["created_at"] else "unknown",
                    }
                    for f in filings
                ],
                key_dates=key_dates,
                related_entities=[e["name"] for e in entities],
                summary_stats={
                    "total_documents": stats["total_documents"] if stats else 0,
                    "total_emails": stats["total_emails"] if stats else 0,
                    "total_attachments": stats["total_attachments"] if stats else 0,
                },
            )

    def inject_case_context(
        self,
        base_system_prompt: str,
        case_context: Optional[CaseContext],
    ) -> str:
        """Inject case context into system prompt for case-aware RAG."""
        if not case_context:
            return base_system_prompt

        context_block = case_context.to_prompt_block()
        return (
            f"{base_system_prompt}\n\n"
            f"---\n"
            f"The following case context has been automatically loaded from the database. "
            f"Use it to ground your responses in the specific facts of this case.\n\n"
            f"{context_block}"
        )

    # ── Document Templates ───────────────────────────────────

    async def list_templates(
        self,
        category: Optional[str] = None,
        jurisdiction: Optional[str] = None,
    ) -> list[dict]:
        """List available document templates."""
        conditions = ["is_active = true"]
        params: list[Any] = []
        idx = 1

        if category:
            conditions.append(f"category = ${idx}")
            params.append(category)
            idx += 1
        if jurisdiction:
            conditions.append(f"jurisdiction = ${idx}")
            params.append(jurisdiction)
            idx += 1

        where = " AND ".join(conditions)
        rows = await self.pool.fetch(f"""
            SELECT id, name, category, description, jurisdiction, court_type,
                   variables, created_at
            FROM legal.document_templates
            WHERE {where}
            ORDER BY category, name
        """, *params)

        return [
            {
                "id": r["id"],
                "name": r["name"],
                "category": r["category"],
                "description": r["description"],
                "jurisdiction": r["jurisdiction"],
                "court_type": r["court_type"],
                "variables": json.loads(r["variables"]) if isinstance(r["variables"], str) else r["variables"],
            }
            for r in rows
        ]

    async def get_template(self, template_id: int) -> Optional[dict]:
        """Get a template by ID with its full body."""
        row = await self.pool.fetchrow("""
            SELECT id, name, category, description, template_body,
                   variables, jurisdiction, court_type
            FROM legal.document_templates
            WHERE id = $1 AND is_active = true
        """, template_id)

        if not row:
            return None

        return {
            "id": row["id"],
            "name": row["name"],
            "category": row["category"],
            "description": row["description"],
            "template_body": row["template_body"],
            "variables": json.loads(row["variables"]) if isinstance(row["variables"], str) else row["variables"],
            "jurisdiction": row["jurisdiction"],
            "court_type": row["court_type"],
        }

    async def fill_template(
        self,
        template_id: int,
        variables: dict[str, str],
    ) -> Optional[FilledTemplate]:
        """Fill a template with provided variables.

        Variables in the template use {{variable_name}} syntax.
        """
        template = await self.get_template(template_id)
        if not template:
            return None

        body = template["template_body"]
        var_defs = template["variables"]

        # Apply defaults for missing variables
        defaults = {}
        for v in var_defs:
            if v.get("default_value"):
                defaults[v["name"]] = v["default_value"]

        merged = {**defaults, **variables}

        # Find all placeholders
        placeholders = set(re.findall(r'\{\{(\w+)\}\}', body))

        # Fill placeholders
        rendered = body
        used = {}
        missing = []

        for placeholder in placeholders:
            if placeholder in merged:
                rendered = rendered.replace(f"{{{{{placeholder}}}}}", str(merged[placeholder]))
                used[placeholder] = str(merged[placeholder])
            else:
                missing.append(placeholder)

        return FilledTemplate(
            template_id=template_id,
            template_name=template["name"],
            rendered_body=rendered,
            variables_used=used,
            missing_variables=missing,
        )

    async def fill_template_with_case(
        self,
        template_id: int,
        case_identifier: str,
        additional_variables: dict[str, str] | None = None,
    ) -> Optional[FilledTemplate]:
        """Fill a template using case data + additional variables."""
        context = await self.build_case_context(case_identifier)
        if not context:
            return None

        # Auto-populate common variables from case context
        auto_vars = {
            "case_number": context.case_number,
            "case_caption": context.case_title,
            "court_name": context.court,
            "date": date.today().strftime("%B %d, %Y"),
        }

        # Add parties
        for p in context.parties:
            role = p.get("role", "").lower().replace(" ", "_")
            if role:
                auto_vars[role] = p.get("name", "")

        if additional_variables:
            auto_vars.update(additional_variables)

        return await self.fill_template(template_id, auto_vars)

    # ── Deadline Tracking ────────────────────────────────────

    async def add_deadline(
        self,
        case_identifier: str,
        title: str,
        deadline_date: date,
        deadline_type: str = "filing",
        priority: str = "normal",
        description: str = "",
        source_document_id: Optional[str] = None,
        reminder_days: list[int] | None = None,
    ) -> int:
        """Add a deadline for a case. Returns the deadline ID."""
        async with self.pool.acquire() as conn:
            case = await conn.fetchrow(
                "SELECT id FROM legal.cases WHERE case_number ILIKE $1 LIMIT 1",
                f"%{case_identifier}%",
            )
            if not case:
                raise ValueError(f"Case not found: {case_identifier}")

            row = await conn.fetchrow("""
                INSERT INTO legal.case_deadlines
                    (case_id, title, deadline_date, deadline_type, priority,
                     description, source_document_id, reminder_days)
                VALUES ($1, $2, $3, $4, $5, $6, $7::uuid, $8)
                RETURNING id
            """,
                case["id"], title, deadline_date, deadline_type, priority,
                description,
                source_document_id,
                reminder_days or [7, 3, 1],
            )
            log.info("Added deadline %d for case %s: %s on %s",
                     row["id"], case_identifier, title, deadline_date)
            return row["id"]

    async def get_upcoming_deadlines(
        self,
        days: int = 14,
        case_identifier: Optional[str] = None,
    ) -> list[DeadlineAlert]:
        """Get upcoming deadlines within N days."""
        cutoff = date.today() + timedelta(days=days)

        conditions = ["d.status = 'pending'", "d.deadline_date <= $1"]
        params: list[Any] = [cutoff]
        idx = 2

        if case_identifier:
            conditions.append(f"c.case_number ILIKE ${idx}")
            params.append(f"%{case_identifier}%")
            idx += 1

        where = " AND ".join(conditions)

        rows = await self.pool.fetch(f"""
            SELECT d.id, d.title, d.deadline_date, d.deadline_type,
                   d.priority, d.description,
                   c.case_number, c.case_title
            FROM legal.case_deadlines d
            JOIN legal.cases c ON d.case_id = c.id
            WHERE {where}
            ORDER BY d.deadline_date ASC
        """, *params)

        today = date.today()
        return [
            DeadlineAlert(
                deadline_id=r["id"],
                case_number=r["case_number"],
                case_title=r["case_title"] or r["case_number"],
                title=r["title"],
                deadline_date=r["deadline_date"],
                days_remaining=(r["deadline_date"] - today).days,
                priority=r["priority"],
                deadline_type=r["deadline_type"],
                description=r["description"] or "",
            )
            for r in rows
        ]

    async def check_deadline_alerts(self) -> list[DeadlineAlert]:
        """Check for deadlines that need alerts sent today.

        Returns deadlines where today matches one of their reminder_days
        before the deadline, or the deadline is today/overdue.
        """
        today = date.today()

        rows = await self.pool.fetch("""
            SELECT d.id, d.title, d.deadline_date, d.deadline_type,
                   d.priority, d.description, d.reminder_days,
                   c.case_number, c.case_title
            FROM legal.case_deadlines d
            JOIN legal.cases c ON d.case_id = c.id
            WHERE d.status = 'pending'
              AND d.deadline_date >= $1
        """, today)

        alerts = []
        for r in rows:
            days_until = (r["deadline_date"] - today).days
            reminder_days = r["reminder_days"] or [7, 3, 1]

            # Alert if today matches a reminder day, or deadline is today
            if days_until in reminder_days or days_until == 0:
                alerts.append(DeadlineAlert(
                    deadline_id=r["id"],
                    case_number=r["case_number"],
                    case_title=r["case_title"] or r["case_number"],
                    title=r["title"],
                    deadline_date=r["deadline_date"],
                    days_remaining=days_until,
                    priority="critical" if days_until <= 1 else r["priority"],
                    deadline_type=r["deadline_type"],
                    description=r["description"] or "",
                ))

        # Also check overdue deadlines
        overdue = await self.pool.fetch("""
            SELECT d.id, d.title, d.deadline_date, d.deadline_type,
                   d.priority, d.description,
                   c.case_number, c.case_title
            FROM legal.case_deadlines d
            JOIN legal.cases c ON d.case_id = c.id
            WHERE d.status = 'pending'
              AND d.deadline_date < $1
        """, today)

        for r in overdue:
            alerts.append(DeadlineAlert(
                deadline_id=r["id"],
                case_number=r["case_number"],
                case_title=r["case_title"] or r["case_number"],
                title=f"OVERDUE: {r['title']}",
                deadline_date=r["deadline_date"],
                days_remaining=(r["deadline_date"] - today).days,
                priority="critical",
                deadline_type=r["deadline_type"],
                description=r["description"] or "",
            ))

        return sorted(alerts, key=lambda a: a.deadline_date)

    async def complete_deadline(self, deadline_id: int) -> bool:
        """Mark a deadline as completed."""
        result = await self.pool.execute("""
            UPDATE legal.case_deadlines
            SET status = 'completed', completed_at = now(), updated_at = now()
            WHERE id = $1 AND status = 'pending'
        """, deadline_id)
        return result == "UPDATE 1"

    async def extend_deadline(
        self,
        deadline_id: int,
        new_date: date,
        reason: str = "",
    ) -> bool:
        """Extend a deadline to a new date."""
        result = await self.pool.execute("""
            UPDATE legal.case_deadlines
            SET deadline_date = $2, status = 'extended',
                notes = COALESCE(notes, '') || E'\nExtended: ' || $3,
                updated_at = now()
            WHERE id = $1 AND status IN ('pending', 'extended')
        """, deadline_id, new_date, reason)
        return result.endswith("1")

    # ── Deadline Monitoring Agent Loop ───────────────────────

    async def run_deadline_monitor(
        self,
        notify_fn: Optional[callable] = None,
        check_interval: int = 3600,
    ):
        """Run continuous deadline monitoring loop.

        Args:
            notify_fn: async callable(alert: DeadlineAlert) for sending notifications
            check_interval: seconds between checks (default 1 hour)
        """
        log.info("Starting deadline monitor (interval=%ds)", check_interval)

        while True:
            try:
                alerts = await self.check_deadline_alerts()
                if alerts:
                    log.info("Found %d deadline alerts", len(alerts))
                    for alert in alerts:
                        log.warning(
                            "DEADLINE ALERT [%s]: %s — %s (%d days)",
                            alert.case_number, alert.title,
                            alert.deadline_date, alert.days_remaining,
                        )
                        if notify_fn:
                            try:
                                await notify_fn(alert)
                            except Exception as e:
                                log.error("Notification failed for deadline %d: %s",
                                         alert.deadline_id, e)
                else:
                    log.debug("No deadline alerts")

            except Exception as e:
                log.error("Deadline monitor error: %s", e)

            await asyncio.sleep(check_interval)
