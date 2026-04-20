from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Optional, Sequence


# Mailboxes to scan
MAILBOXES = [
    "william@seattleseahawks.me",
    "athena@seattleseahawks.me",
]


# Target domains — emails to/from these domains will be synced
TARGET_DOMAINS = [
    # Government / court
    "snoco.org",
    "co.snohomish.wa.us",
    "co.chelan.wa.us",
    "courts.wa.gov",
    "ca9.uscourts.gov",
    "*.uscourts.gov",
    "southsnofire.org",
    # Opposing / defense counsel law firms
    "workerlaw.com",        # Iglitzin, Cole, Fernando, Rozzano
    "csdlaw.com",           # Becker, Davis, Dobbs, Paxton (same firm as cabornelaw)
    "cabornelaw.com",       # alternate domain for CSD Law
    "clearpathpllc.com",    # Daniel Fox
    "fwwlaw.com",           # Lyman, Bertolino, Himes, Ross
    "kantorlaw.net",        # B. Davis
    "bn-lawyers.com",       # Suttell
    "ogletree.com",         # Shapero, Shely
    "ogletreedeakins.com",  # Shapero, Shely (alternate domain)
    "lewisbrisbois.com",    # Hunter
    "grsm.com",             # Jardine, Lockwood
    "insleebest.com",       # Chambers, Lee
    "jmblawyers.com",       # Baker
    "uscanadalaw.com",      # Paul
    "favros.com",           # James B. Meade
    "cozen.com",            # Lee
    "slwsd.com",            # Brees
    "lldkb.com",            # tam@
    "mhb.com",              # MHB Law
    # Organizations / other correspondence
    "wahbexchange.org",         # Washington Health Benefit Exchange (WAHBE) — ADA accommodation requests
    "evergreenhealthcare.org",  # EvergreenHealth — primary domain (MyChart, staff, donotreply)
    "evergreenhealth.com",      # EvergreenHealth — staff email domain (rameeks@, cbredeson@, mshepler@)
    "starbucks.com",            # Starbucks corp counsel
    "playfasa.com",             # PlayFASA website development correspondence
]


# Specific email addresses to always include (regardless of domain)
# Used for individual contacts whose domains aren't exclusively legal
TARGET_SPECIFIC_EMAILS = [
    "fayejwonglawfirm@gmail.com",   # GAL Faye Wong
    "sara.c.murray@gmail.com",      # Sara Murray
    "wjnelson78@gmail.com",         # William personal
    "rcwcodebuster@gmail.com",      # self research
    "rcwcodebuster@aol.com",        # self research
    "rcwcodebuster@yahoo.com",      # self research
]


@dataclass(frozen=True)
class SyncConfigResolution:
    mailboxes: list[str]
    domains: list[str]
    specific_emails: list[str]
    explicit_mailboxes: bool
    explicit_domains: bool
    db_mailboxes: list[str] = field(default_factory=list)
    db_domains: list[str] = field(default_factory=list)
    db_specific_emails: list[str] = field(default_factory=list)
    code_only_domains: list[str] = field(default_factory=list)
    db_only_domains: list[str] = field(default_factory=list)


_STATUS_PRIORITY = {
    "healthy": 0,
    "degraded": 1,
    "unhealthy": 2,
}


def normalize_config_values(values: Optional[Iterable[str]]) -> list[str]:
    """Trim, lowercase, de-duplicate, and preserve input order."""
    normalized: list[str] = []
    seen: set[str] = set()

    for value in values or []:
        text = (value or "").strip().lower()
        if not text or text in seen:
            continue
        normalized.append(text)
        seen.add(text)

    return normalized


def _rule_value(rule: Mapping[str, object] | object, field: str) -> Optional[str]:
    if isinstance(rule, Mapping):
        value = rule.get(field)
    else:
        try:
            value = rule[field]
        except Exception:
            value = getattr(rule, field, None)

    if value is None:
        return None
    return str(value)


def _merge_status(current: str, candidate: str) -> str:
    return (
        candidate
        if _STATUS_PRIORITY[candidate] > _STATUS_PRIORITY[current]
        else current
    )


def _config_source(*, explicit: bool, db_values: Sequence[str]) -> str:
    if explicit:
        return "explicit"
    if db_values:
        return "database"
    return "defaults"


def resolve_sync_configuration(
    *,
    default_mailboxes: Sequence[str],
    default_domains: Sequence[str],
    default_specific_emails: Sequence[str],
    cli_mailboxes: Optional[Sequence[str]] = None,
    cli_domains: Optional[Sequence[str]] = None,
    db_mailboxes: Optional[Sequence[str]] = None,
    db_rules: Optional[Sequence[Mapping[str, object] | object]] = None,
) -> SyncConfigResolution:
    """Resolve effective sync config while preserving explicit CLI overrides."""
    explicit_mailboxes = cli_mailboxes is not None
    explicit_domains = cli_domains is not None

    default_mailboxes_norm = normalize_config_values(default_mailboxes)
    default_domains_norm = normalize_config_values(default_domains)
    default_specific_norm = normalize_config_values(default_specific_emails)

    db_mailboxes_norm = normalize_config_values(db_mailboxes)
    db_rules = list(db_rules or [])
    db_domains_norm = normalize_config_values(
        _rule_value(rule, "pattern")
        for rule in db_rules
        if (_rule_value(rule, "rule_type") or "").lower() == "domain"
    )
    db_specific_norm = normalize_config_values(
        _rule_value(rule, "pattern")
        for rule in db_rules
        if (_rule_value(rule, "rule_type") or "").lower() == "email_address"
    )

    if explicit_mailboxes:
        resolved_mailboxes = normalize_config_values(cli_mailboxes)
    elif db_mailboxes_norm:
        resolved_mailboxes = db_mailboxes_norm
    else:
        resolved_mailboxes = default_mailboxes_norm

    if explicit_domains:
        resolved_domains = normalize_config_values(cli_domains)
    elif db_domains_norm:
        resolved_domains = db_domains_norm
    else:
        resolved_domains = default_domains_norm

    resolved_specific = normalize_config_values([
        *default_specific_norm,
        *db_specific_norm,
    ])

    code_only_domains = []
    db_only_domains = []
    if db_domains_norm:
        code_set = set(default_domains_norm)
        db_set = set(db_domains_norm)
        code_only_domains = sorted(code_set - db_set)
        db_only_domains = sorted(db_set - code_set)

    return SyncConfigResolution(
        mailboxes=resolved_mailboxes,
        domains=resolved_domains,
        specific_emails=resolved_specific,
        explicit_mailboxes=explicit_mailboxes,
        explicit_domains=explicit_domains,
        db_mailboxes=db_mailboxes_norm,
        db_domains=db_domains_norm,
        db_specific_emails=db_specific_norm,
        code_only_domains=code_only_domains,
        db_only_domains=db_only_domains,
    )


def build_sync_health_report(config: SyncConfigResolution) -> dict[str, object]:
    """Summarize resolved email sync config as a health payload."""
    status = "healthy"
    issues: list[str] = []

    if not config.mailboxes:
        status = _merge_status(status, "unhealthy")
        issues.append("No email sync mailboxes are configured.")
    elif not config.db_mailboxes and not config.explicit_mailboxes:
        status = _merge_status(status, "degraded")
        issues.append("No active DB mailboxes found; using code-default mailboxes.")

    if not config.domains and not config.specific_emails:
        status = _merge_status(status, "unhealthy")
        issues.append("No email sync domains or specific email rules are configured.")
    elif not config.db_domains and not config.db_specific_emails and not config.explicit_domains:
        status = _merge_status(status, "degraded")
        issues.append("No active DB sync rules found; using code-default targets.")

    if config.code_only_domains:
        status = _merge_status(status, "degraded")
        issues.append(
            "Code defaults include domains missing from active DB rules: "
            + ", ".join(config.code_only_domains)
        )

    if config.db_only_domains:
        status = _merge_status(status, "degraded")
        issues.append(
            "Active DB rules include domains missing from code defaults: "
            + ", ".join(config.db_only_domains)
        )

    summary = (
        f"Email sync config {status}: "
        f"{len(config.mailboxes)} mailbox(es), "
        f"{len(config.domains)} domain rule(s), "
        f"{len(config.specific_emails)} specific email address(es)."
    )

    if config.code_only_domains or config.db_only_domains:
        summary += (
            f" Drift detected ({len(config.code_only_domains)} code-only, "
            f"{len(config.db_only_domains)} db-only domains)."
        )
    elif issues:
        summary += f" {issues[0]}"

    return {
        "status": status,
        "summary": summary,
        "issues": issues,
        "mailboxes": config.mailboxes,
        "domains": config.domains,
        "specific_emails": config.specific_emails,
        "counts": {
            "mailboxes": len(config.mailboxes),
            "domains": len(config.domains),
            "specific_emails": len(config.specific_emails),
            "db_mailboxes": len(config.db_mailboxes),
            "db_domains": len(config.db_domains),
            "db_specific_emails": len(config.db_specific_emails),
        },
        "sources": {
            "mailboxes": _config_source(
                explicit=config.explicit_mailboxes,
                db_values=config.db_mailboxes,
            ),
            "domains": _config_source(
                explicit=config.explicit_domains,
                db_values=config.db_domains,
            ),
            "specific_emails": "merged",
        },
        "drift": {
            "has_drift": bool(config.code_only_domains or config.db_only_domains),
            "code_only_domains": config.code_only_domains,
            "db_only_domains": config.db_only_domains,
        },
    }


async def fetch_active_sync_configuration(conn: Any) -> tuple[list[str], list[object]]:
    """Load active sync mailboxes and rules from the database."""
    mailbox_rows = await conn.fetch(
        """
        SELECT email
        FROM ops.sync_mailboxes
        WHERE is_active = true
        ORDER BY id
        """
    )
    rule_rows = await conn.fetch(
        """
        SELECT rule_type, pattern
        FROM ops.sync_rules
        WHERE is_active = true
          AND rule_type IN ('domain', 'email_address')
        ORDER BY priority DESC, id
        """
    )

    db_mailboxes = normalize_config_values(
        row["email"]
        for row in mailbox_rows
    )

    return db_mailboxes, list(rule_rows)


async def audit_sync_configuration_health(
    conn: Any,
    *,
    default_mailboxes: Sequence[str] = MAILBOXES,
    default_domains: Sequence[str] = TARGET_DOMAINS,
    default_specific_emails: Sequence[str] = TARGET_SPECIFIC_EMAILS,
) -> dict[str, object]:
    """Resolve the active sync config and return a health report."""
    db_mailboxes, db_rules = await fetch_active_sync_configuration(conn)
    config = resolve_sync_configuration(
        default_mailboxes=default_mailboxes,
        default_domains=default_domains,
        default_specific_emails=default_specific_emails,
        db_mailboxes=db_mailboxes,
        db_rules=db_rules,
    )
    return build_sync_health_report(config)