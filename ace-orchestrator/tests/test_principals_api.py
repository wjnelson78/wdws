"""Patch 02 §B.5: live test of /v1/principals/* config + onboarding endpoints."""
from __future__ import annotations

import os

import httpx
import pytest

pytestmark = pytest.mark.asyncio

BASE = "http://127.0.0.1:8080"


def _api_key() -> str:
    return os.environ["ORCHESTRATOR_API_KEY"]


@pytest.fixture(scope="session", autouse=True)
def ensure_service_running():
    """Tests in this module assume the orchestrator is running on :8080."""
    try:
        r = httpx.get(f"{BASE}/healthz", timeout=2.0)
        if r.status_code != 200:
            pytest.skip(f"orchestrator not healthy: {r.status_code}")
    except Exception as exc:
        pytest.skip(f"orchestrator not reachable on {BASE}: {exc}")


async def test_get_full_config_for_will():
    headers = {"Authorization": f"Bearer {_api_key()}"}
    async with httpx.AsyncClient(timeout=10.0) as c:
        r = await c.get(f"{BASE}/v1/principals/will.nelson/config", headers=headers)
    assert r.status_code == 200
    body = r.json()
    assert body["user_identifier"] == "will.nelson"
    assert "council_gate.mini_threshold" in body["parameters"]


async def test_get_one_parameter():
    headers = {"Authorization": f"Bearer {_api_key()}"}
    async with httpx.AsyncClient(timeout=10.0) as c:
        r = await c.get(
            f"{BASE}/v1/principals/will.nelson/config/council_gate.mini_threshold",
            headers=headers,
        )
    assert r.status_code == 200
    body = r.json()
    assert body["parameter_path"] == "council_gate.mini_threshold"
    assert body["value"] == 0.4


async def test_unknown_parameter_returns_400():
    headers = {"Authorization": f"Bearer {_api_key()}"}
    async with httpx.AsyncClient(timeout=10.0) as c:
        r = await c.get(
            f"{BASE}/v1/principals/will.nelson/config/no.such.param", headers=headers
        )
    assert r.status_code == 400


async def test_put_then_delete_round_trip():
    headers = {"Authorization": f"Bearer {_api_key()}"}
    path = "council_gate.mini_threshold"
    async with httpx.AsyncClient(timeout=10.0) as c:
        # baseline (default)
        r = await c.get(f"{BASE}/v1/principals/will.nelson/config/{path}", headers=headers)
        baseline = r.json()["value"]

        # set to 0.55
        r = await c.put(
            f"{BASE}/v1/principals/will.nelson/config/{path}",
            headers=headers,
            json={"value": 0.55, "source": "principal"},
        )
        assert r.status_code == 200, r.text

        # verify
        r = await c.get(f"{BASE}/v1/principals/will.nelson/config/{path}", headers=headers)
        assert r.json()["value"] == 0.55
        assert r.json()["source"] == "principal"

        # delete (revert to default)
        r = await c.delete(
            f"{BASE}/v1/principals/will.nelson/config/{path}", headers=headers
        )
        assert r.status_code == 204

        # verify back to baseline
        r = await c.get(f"{BASE}/v1/principals/will.nelson/config/{path}", headers=headers)
        assert r.json()["value"] == baseline


async def test_unknown_principal_returns_404():
    headers = {"Authorization": f"Bearer {_api_key()}"}
    async with httpx.AsyncClient(timeout=10.0) as c:
        r = await c.get(
            f"{BASE}/v1/principals/no.such.user/config", headers=headers
        )
    assert r.status_code == 404
