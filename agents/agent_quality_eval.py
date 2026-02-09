"""
✅ Quality Eval Agent — RAG + Safety Regression Suite
══════════════════════════════════════════════════════
Runs a lightweight evaluation suite to measure retrieval quality and
basic safety exposure (PII patterns) in retrieved context.

- Uses core.hybrid_search (semantic + full-text) for consistency
- Scores pass/fail against expected domain/case/document_type
- Tracks pass rate and flags regressions
- Logs findings into ops.agent_findings and run metrics
"""
import json
import os
import re
import uuid
from pathlib import Path
from typing import Any, Optional

from framework import BaseAgent, RunContext, get_llm

# ── Eval config ─────────────────────────────────────────────
EVAL_CASES_PATH = Path(__file__).parent / "evals" / "eval_cases.json"
EVAL_EMBED_MODEL = os.getenv("EVAL_EMBED_MODEL", "text-embedding-3-large")
EVAL_EMBED_DIMS = int(os.getenv("EVAL_EMBED_DIMS", "3072"))
EVAL_TOP_K = int(os.getenv("EVAL_TOP_K", "8"))
EVAL_SEM_WEIGHT = float(os.getenv("EVAL_SEM_WEIGHT", "0.7"))
EVAL_FT_WEIGHT = float(os.getenv("EVAL_FT_WEIGHT", "0.3"))
EVAL_RRF_K = int(os.getenv("EVAL_RRF_K", "60"))

# ── Safety patterns ─────────────────────────────────────────
PII_PATTERNS = {
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "credit_card": re.compile(r"\b(?:\d[ -]*?){13,16}\b"),
    "mrn": re.compile(r"\bMRN[: ]?\d{6,12}\b", re.IGNORECASE),
}


def _safe_load_cases() -> list[dict[str, Any]]:
    if not EVAL_CASES_PATH.exists():
        return []
    return json.loads(EVAL_CASES_PATH.read_text())


async def _embed_query(text: str) -> list[float]:
    client = get_llm()
    resp = await client.embeddings.create(
        model=EVAL_EMBED_MODEL,
        input=text,
        dimensions=EVAL_EMBED_DIMS,
    )
    return resp.data[0].embedding


def _vec_literal(vec: list[float]) -> str:
    return "[" + ",".join(str(v) for v in vec) + "]"


def _check_pii(text: str) -> dict[str, int]:
    hits = {}
    for name, pattern in PII_PATTERNS.items():
        hits[name] = len(pattern.findall(text or ""))
    return hits


def _score_result(
    expected: dict,
    results: list[dict],
    min_results: int,
) -> tuple[bool, dict]:
    passes = {
        "min_results": len(results) >= min_results,
        "domain": True,
        "case_number": True,
        "document_type": True,
    }

    expected_domain = expected.get("domain")
    expected_case = expected.get("case_number")
    expected_type = expected.get("document_type")

    if expected_domain:
        passes["domain"] = any(r.get("domain") == expected_domain for r in results)
    if expected_case:
        passes["case_number"] = any(expected_case in (r.get("case_numbers") or []) for r in results)
    if expected_type:
        passes["document_type"] = any(r.get("document_type") == expected_type for r in results)

    score = 0.0
    score += 0.2 if passes["min_results"] else 0.0
    score += 0.4 if passes["domain"] else 0.0
    score += 0.3 if passes["case_number"] else 0.0
    score += 0.1 if passes["document_type"] else 0.0

    passed = score >= 0.7
    return passed, {"passes": passes, "score": round(score, 2)}


class QualityEvalAgent(BaseAgent):
    agent_id = "quality-eval"
    agent_name = "Quality Eval"
    description = "Runs regression evals for RAG quality + safety exposure"
    version = "1.0.0"
    schedule = "0 */12 * * *"  # Every 12 hours
    priority = 3
    capabilities = ["evals", "rag-quality", "safety-check"]

    instructions = "You evaluate retrieval quality and safety regressions."

    async def run(self, ctx: RunContext) -> dict:
        metrics: dict[str, Any] = {
            "total_cases": 0,
            "passed": 0,
            "failed": 0,
            "skipped": 0,
            "avg_results": 0,
            "pii_hits": {},
        }

        cases = _safe_load_cases()
        if not cases:
            await ctx.finding("warning", "evals", "No eval cases found",
                             f"Missing or empty: {EVAL_CASES_PATH}")
            return {"summary": "Eval skipped: no cases", "metrics": metrics}

        p = await ctx.db()
        total_results = 0
        failures = []

        for case in cases:
            case_id = case.get("id", "unknown")
            query = (case.get("query") or "").strip()
            expected = case.get("expected", {})
            min_results = int(case.get("min_results", 1))
            require_case_exists = bool(case.get("require_case_exists", False))
            min_domain_docs = int(case.get("min_domain_docs", 0))

            if not query:
                metrics["skipped"] += 1
                continue

            if require_case_exists and expected.get("case_number"):
                exists = await p.fetchval(
                    "SELECT 1 FROM legal.cases WHERE case_number ILIKE $1 LIMIT 1",
                    f"%{expected['case_number']}%",
                )
                if not exists:
                    metrics["skipped"] += 1
                    continue

            if min_domain_docs and expected.get("domain"):
                domain_count = await p.fetchval(
                    "SELECT COUNT(*) FROM core.documents WHERE domain = $1",
                    expected["domain"],
                )
                if not domain_count or domain_count < min_domain_docs:
                    metrics["skipped"] += 1
                    continue

            metrics["total_cases"] += 1

            vec = await _embed_query(query)
            vec_str = _vec_literal(vec)

            rows = await p.fetch(
                """
                SELECT hs.chunk_id, hs.document_id, hs.content,
                       hs.domain, hs.document_type, hs.title, hs.filename,
                       hs.hybrid_score, hs.semantic_score, hs.fulltext_score
                FROM core.hybrid_search(
                    $1,
                    $2::halfvec(3072),
                    $3,
                    $4,
                    $5,
                    $6,
                    $7
                ) AS hs
                """,
                query,
                vec_str,
                EVAL_TOP_K,
                expected.get("domain"),
                EVAL_SEM_WEIGHT,
                EVAL_FT_WEIGHT,
                EVAL_RRF_K,
            )

            doc_ids = [r["document_id"] for r in rows if r.get("document_id")]
            case_map: dict[str, list[str]] = {}
            if doc_ids:
                doc_uuid_list = [uuid.UUID(str(d)) for d in doc_ids]
                case_rows = await p.fetch(
                    """
                    SELECT cd.document_id::text AS document_id,
                           array_agg(lc.case_number) AS case_numbers
                    FROM legal.case_documents cd
                    JOIN legal.cases lc ON cd.case_id = lc.id
                    WHERE cd.document_id = ANY($1::uuid[])
                    GROUP BY cd.document_id
                    """,
                    doc_uuid_list,
                )
                case_map = {r["document_id"]: r["case_numbers"] for r in case_rows}

            results = []
            for r in rows:
                doc_id = str(r["document_id"]) if r.get("document_id") else None
                content = r.get("content") or ""
                results.append({
                    "document_id": doc_id,
                    "domain": r.get("domain"),
                    "document_type": r.get("document_type"),
                    "title": r.get("title") or r.get("filename"),
                    "case_numbers": case_map.get(doc_id, []),
                    "pii": _check_pii(content),
                })

            total_results += len(results)
            passed, score_info = _score_result(expected, results, min_results)

            if not passed:
                failures.append({
                    "case_id": case_id,
                    "query": query,
                    "expected": expected,
                    "score": score_info,
                    "results": results[:3],
                })

            for result in results:
                for k, v in (result.get("pii") or {}).items():
                    metrics["pii_hits"][k] = metrics["pii_hits"].get(k, 0) + v

            if passed:
                metrics["passed"] += 1
            else:
                metrics["failed"] += 1

        if metrics["total_cases"]:
            metrics["avg_results"] = round(total_results / metrics["total_cases"], 2)

        pass_rate = (metrics["passed"] / max(metrics["total_cases"], 1))
        metrics["pass_rate"] = round(pass_rate, 3)

        if metrics["failed"]:
            severity = "warning" if pass_rate >= 0.6 else "critical"
            await ctx.finding(
                severity,
                "evals",
                f"Eval failures: {metrics['failed']} of {metrics['total_cases']} cases",
                "Review failed evals for retrieval regression",
                {"failures": failures},
            )

        if metrics["pii_hits"]:
            total_pii = sum(metrics["pii_hits"].values())
            if total_pii:
                await ctx.finding(
                    "info",
                    "safety",
                    f"PII patterns detected in eval contexts: {total_pii} hits",
                    "PII may be expected in legal/medical data, but track for exposure risk",
                    metrics["pii_hits"],
                )

        return {
            "summary": (
                f"Eval run: {metrics['passed']}/{metrics['total_cases']} passed, "
                f"{metrics['failed']} failed, {metrics['skipped']} skipped"
            ),
            "metrics": metrics,
        }
