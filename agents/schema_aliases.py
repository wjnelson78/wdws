"""
Schema Alias Resolution — Intelligent column name normalization.

The database schema has specific column names, but users and LLMs often refer
to the same concept using different names.  For example:
  - "filed_date" vs "date_filed"  (legal.cases)
  - "email_date" vs "date_sent"   (legal.email_metadata)
  - "filing_date" vs "actual_filing_date" (legal.filing_metadata)

This module provides bidirectional alias resolution so the system can
understand any reasonable name variation and map it to the correct column,
or rewrite SQL transparently before it hits PostgreSQL.

Usage:
    from schema_aliases import resolve_column, normalize_sql

    # Resolve a single column reference
    resolve_column("legal.cases", "filed_date")  # → "date_filed"

    # Auto-fix SQL before execution
    fixed_sql = normalize_sql("SELECT c.filed_date FROM legal.cases c")
    # → "SELECT c.date_filed FROM legal.cases c"
"""

import re
import logging
from typing import Optional

log = logging.getLogger("schema-aliases")

# ── Canonical Schema ─────────────────────────────────────────
# Maps schema.table → { alias → correct_column_name }
# The canonical column name maps to itself for completeness.
COLUMN_ALIASES: dict[str, dict[str, str]] = {
    "legal.cases": {
        # date_filed is canonical
        "date_filed":   "date_filed",
        "filed_date":   "date_filed",
        "filing_date":  "date_filed",
        "file_date":    "date_filed",
        # date_closed is canonical
        "date_closed":  "date_closed",
        "closed_date":  "date_closed",
        "close_date":   "date_closed",
    },
    "legal.email_metadata": {
        # date_sent is canonical
        "date_sent":    "date_sent",
        "email_date":   "date_sent",
        "sent_date":    "date_sent",
        "send_date":    "date_sent",
        "date_email":   "date_sent",
        # document_id is the PK (no separate 'id')
        "email_id":     "document_id",
        "id":           "document_id",
    },
    "legal.filing_metadata": {
        # actual_filing_date is canonical
        "actual_filing_date": "actual_filing_date",
        "filing_date":        "actual_filing_date",
        "filed_date":         "actual_filing_date",
        "date_filed":         "actual_filing_date",
        "file_date":          "actual_filing_date",
        "date_filing":        "actual_filing_date",
    },
    "legal.email_attachments": {
        # extracted_text is canonical
        "extracted_text": "extracted_text",
        "ocr_text":       "extracted_text",
        "ocr":            "extracted_text",
        "text_content":   "extracted_text",
        # email_doc_id is canonical
        "email_doc_id":   "email_doc_id",
        "email_id":       "email_doc_id",
    },
    "core.documents": {
        # full_content is canonical
        "full_content":         "full_content",
        "full_content_preview": "full_content",
        "content_preview":      "full_content",
        "body":                 "full_content",
        "content":              "full_content",
    },
    "core.document_chunks": {
        # embedding is canonical (no has_embedding boolean)
        "has_embedding": None,   # → must use 'embedding IS NOT NULL' instead
        "embedding":     "embedding",
    },
    "ops.health_checks": {
        "check_name":  "check_name",
        "service":     "check_name",
        "service_name":"check_name",
        "value":       "value",
        "response_ms": "value",
        "detail":      "detail",
        "details":     "detail",
    },
    "medical.record_metadata": {
        "date_of_service": "date_of_service",
        "service_date":    "date_of_service",
        "dos":             "date_of_service",
    },
}

# ── Table Alias Detection ────────────────────────────────────
# Maps common SQL table aliases to their schema.table.
# Extracted from FROM/JOIN clauses at runtime; these are defaults.
DEFAULT_TABLE_ALIASES: dict[str, str] = {
    "c":  "legal.cases",        # most common usage
    "e":  "legal.email_metadata",
    "em": "legal.email_metadata",
    "fm": "legal.filing_metadata",
    "ea": "legal.email_attachments",
    "d":  "core.documents",
    "dc": "core.document_chunks",
    "ch": "core.document_chunks",
    "hc": "ops.health_checks",
    "rm": "medical.record_metadata",
    "p":  "medical.patients",
    "pr": "medical.providers",
}


def resolve_column(table: str, alias: str) -> Optional[str]:
    """Resolve a column alias to the canonical column name.

    Args:
        table: schema.table name (e.g., "legal.cases")
        alias: the column name as written (e.g., "filed_date")

    Returns:
        Canonical column name, None if the alias maps to a non-column
        concept (like has_embedding), or the alias unchanged if no mapping found.
    """
    table_aliases = COLUMN_ALIASES.get(table, {})
    if alias in table_aliases:
        return table_aliases[alias]
    # Not a known alias — return as-is (might be correct already)
    return alias


def _extract_table_aliases(sql: str) -> dict[str, str]:
    """Parse SQL to find table alias → schema.table mappings.
    
    Handles patterns like:
        FROM legal.cases c
        JOIN legal.email_metadata em ON ...
        LEFT JOIN core.documents d ON ...
    """
    aliases = dict(DEFAULT_TABLE_ALIASES)
    # Match FROM/JOIN schema.table alias patterns
    pattern = re.compile(
        r'(?:FROM|JOIN)\s+'
        r'(\w+\.\w+)'      # schema.table
        r'\s+(?:AS\s+)?'
        r'(\w+)',           # alias
        re.IGNORECASE
    )
    for match in pattern.finditer(sql):
        schema_table = match.group(1).lower()
        alias = match.group(2).lower()
        # Skip SQL keywords that look like aliases
        if alias not in ('on', 'where', 'and', 'or', 'set', 'left', 'right',
                         'inner', 'outer', 'cross', 'full', 'natural', 'using',
                         'group', 'order', 'having', 'limit', 'offset', 'union'):
            aliases[alias] = schema_table
    return aliases


def normalize_sql(sql: str, log_rewrites: bool = True) -> str:
    """Transparently rewrite known column aliases in SQL to canonical names.

    This is the key intelligence layer — it means the system can handle
    filed_date, date_filed, filing_date, etc. interchangeably.

    Args:
        sql: The SQL query string
        log_rewrites: Whether to log when rewrites happen

    Returns:
        SQL with column aliases resolved to canonical names
    """
    if not sql or not sql.strip():
        return sql

    # Extract table aliases from the SQL itself
    table_aliases = _extract_table_aliases(sql)

    rewrites = []

    # Pattern: alias.column_name  (e.g., c.filed_date, fm.filing_date)
    def _rewrite_column(match):
        prefix = match.group(1)       # the table alias (e.g., "c", "fm")
        col_name = match.group(2)     # the column name (e.g., "filed_date")

        table_alias = prefix.lower()
        schema_table = table_aliases.get(table_alias)
        if not schema_table:
            return match.group(0)  # Unknown alias, leave unchanged

        canonical = resolve_column(schema_table, col_name.lower())
        if canonical is None:
            # Special case: has_embedding → must be rewritten differently
            # Can't auto-fix this in SQL because the semantics change
            rewrites.append(f"WARNING: {prefix}.{col_name} has no column equivalent "
                          f"in {schema_table} (use 'embedding IS NOT NULL' pattern)")
            return match.group(0)

        if canonical.lower() != col_name.lower():
            rewrites.append(f"{prefix}.{col_name} → {prefix}.{canonical}")
            return f"{prefix}.{canonical}"

        return match.group(0)

    # Match alias.column patterns — word boundary aware
    # Handles: c.filed_date, fm.date_filed, e.email_date, etc.
    result = re.sub(
        r'\b(\w+)\.(\w+)\b',
        _rewrite_column,
        sql
    )

    if rewrites and log_rewrites:
        log.info("SQL alias resolution: %s", "; ".join(rewrites))

    return result


def get_schema_context() -> str:
    """Generate a schema reference string for LLM prompts.

    Returns a concise mapping of common aliases to canonical columns,
    suitable for inclusion in agent instructions or system prompts.
    """
    lines = [
        "COLUMN NAME ALIASES — use canonical names in SQL, but understand all aliases:",
        ""
    ]
    for table, aliases in sorted(COLUMN_ALIASES.items()):
        # Group by canonical name
        canonical_map: dict[str, list[str]] = {}
        for alias, canonical in aliases.items():
            if canonical is None:
                lines.append(f"  {table}: {alias} → NO COLUMN (use 'embedding IS NOT NULL' pattern)")
                continue
            if canonical not in canonical_map:
                canonical_map[canonical] = []
            if alias != canonical:
                canonical_map[canonical].append(alias)

        for canonical, alt_names in sorted(canonical_map.items()):
            if alt_names:
                alts = ", ".join(sorted(alt_names))
                lines.append(f"  {table}.{canonical} (also known as: {alts})")
            else:
                lines.append(f"  {table}.{canonical}")

    return "\n".join(lines)


def get_alias_map_for_table(table: str) -> dict[str, str]:
    """Get all alias→canonical mappings for a specific table.

    Args:
        table: schema.table name (e.g., "legal.cases")

    Returns:
        Dict of alias → canonical column name
    """
    return dict(COLUMN_ALIASES.get(table, {}))


# ── Quick test ───────────────────────────────────────────────
if __name__ == "__main__":
    # Demo
    test_sql = """
        SELECT c.id, c.case_number, c.filed_date, c.closed_date,
               e.email_date, e.sender,
               fm.filing_date, fm.filed_by,
               d.full_content_preview
        FROM legal.cases c
        LEFT JOIN legal.email_metadata e ON e.document_id = d.id
        LEFT JOIN legal.filing_metadata fm ON fm.document_id = d.id
        LEFT JOIN core.documents d ON d.id = c.id
    """
    print("ORIGINAL:")
    print(test_sql)
    print("\nNORMALIZED:")
    print(normalize_sql(test_sql))
    print("\nSCHEMA CONTEXT:")
    print(get_schema_context())
