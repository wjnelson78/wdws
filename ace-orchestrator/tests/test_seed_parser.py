import pathlib

import pytest

from orchestrator.services.seed_parser import parse_seed

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
SEED_PATH = REPO_ROOT / "data" / "william_nelson_user_context_seed.md"


@pytest.fixture(scope="session")
def seed_md() -> str:
    return SEED_PATH.read_text(encoding="utf-8")


def test_parses_two_users(seed_md):
    parsed = parse_seed(seed_md)
    identifiers = {u.identifier for u in parsed.users}
    assert identifiers == {"will.nelson", "jessie.nelson"}


def test_will_user_has_authorization_context(seed_md):
    parsed = parse_seed(seed_md)
    will = next(u for u in parsed.users if u.identifier == "will.nelson")
    assert "Pro se federal litigant" in will.body["authorization_context"]
    assert will.body["role"] == "principal"


def test_contexts_attached_to_will(seed_md):
    parsed = parse_seed(seed_md)
    assert all(c.user_identifier == "will.nelson" for c in parsed.contexts)
    assert len(parsed.contexts) == 38


def test_context_types_present(seed_md):
    parsed = parse_seed(seed_md)
    types = {c.context_type for c in parsed.contexts}
    assert types == {
        "counsel",
        "provider",
        "opposing_firm",
        "court_personnel",
        "records_officer",
        "congressional",
        "collaborator",
        "employer",
        "precedent",
        "framework",
        "infrastructure",
    }


def test_carlisle_priority_and_content(seed_md):
    parsed = parse_seed(seed_md)
    carlisle = next(
        c for c in parsed.contexts if c.context_type == "counsel" and c.key == "carlisle"
    )
    assert carlisle.priority == 90
    assert carlisle.content["name"] == "Chris Carlisle"


def test_cases_attached_to_will(seed_md):
    parsed = parse_seed(seed_md)
    assert all(c.user_identifier == "will.nelson" for c in parsed.cases)
    assert len(parsed.cases) == 9


def test_case_with_colon_in_id_parses(seed_md):
    parsed = parse_seed(seed_md)
    ids = {c.case_id for c in parsed.cases}
    assert "3:25-cv-05551-DGE" in ids
    assert "2:26-cv-00776-JCC" in ids


def test_invalid_yaml_in_fence_raises():
    bad = "## context:counsel:bad\n\n```yaml\nfoo: : :\n```\n"
    with pytest.raises(Exception):
        parse_seed(bad)
