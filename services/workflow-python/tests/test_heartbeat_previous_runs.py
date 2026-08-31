from __future__ import annotations

import sys
import unittest
import uuid
from pathlib import Path

WORKFLOW_PYTHON = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKFLOW_PYTHON))

from api.heartbeat import HeartbeatState


class FakePool:
    def __init__(self, *, profile: dict | None = None, run: dict | None = None) -> None:
        self.profile = profile
        self.run = run
        self.fetch_args: tuple[object, ...] | None = None
        self.fetch_query = ""

    async def fetchrow(self, query: str, *args: object) -> dict | None:
        if "select r.*" in query:
            return self.run
        if "heartbeat_profiles" in query:
            return self.profile
        raise AssertionError(f"unexpected fetchrow query: {query}")

    async def fetch(self, query: str, *args: object) -> list[dict]:
        self.fetch_query = query
        self.fetch_args = args
        return [
            {
                "run_id": uuid.uuid4(),
                "trigger": "schedule",
                "status": "completed",
                "outcome": "attention",
                "candidate_count": 1,
                "surfaced_count": 1,
                "memory_proposal_count": 0,
                "started_at": None,
                "completed_at": None,
                "items": [
                    {
                        "item_id": uuid.uuid4(),
                        "item_type": "work",
                        "title": "Rust builds are slow",
                        "summary": "Consider shared compilation caching.",
                        "status": "open",
                        "disposition": None,
                        "priority_tier": 1,
                        "due_at": None,
                    }
                ],
            }
        ]


def state(
    pool: FakePool,
    *,
    run_id: uuid.UUID | None = None,
    principal: str = "workflow-heartbeat-run",
) -> HeartbeatState:
    return HeartbeatState(
        pool,
        workflow_name="heartbeat_run",
        workflow_run_id=str(run_id or uuid.uuid4()),
        workflow_task_id=str(uuid.uuid4()),
        workflow_principal=principal,
    )


class PreviousRunsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.profile_id = uuid.uuid4()
        self.current_run_id = uuid.uuid4()
        self.profile = {"profile_id": self.profile_id}
        self.run = {"run_id": self.current_run_id, "profile_id": self.profile_id}

    async def test_schema_is_bounded_and_query_is_privacy_safe(self) -> None:
        pool = FakePool(profile=self.profile, run=self.run)
        result = await state(pool, run_id=self.current_run_id).list_previous_runs(
            profile_id=str(self.profile_id), limit=99
        )

        self.assertEqual(
            set(result[0]),
            {
                "run_id",
                "trigger",
                "status",
                "outcome",
                "candidate_count",
                "surfaced_count",
                "memory_proposal_count",
                "started_at",
                "completed_at",
                "items",
            },
        )
        self.assertEqual(
            set(result[0]["items"][0]),
            {
                "item_id",
                "item_type",
                "title",
                "summary",
                "status",
                "disposition",
                "priority_tier",
                "due_at",
            },
        )
        self.assertEqual(pool.fetch_args[-1], 8)
        query = pool.fetch_query
        self.assertIn("r.run_id <> $2", query)
        self.assertIn("r.status in ('completed', 'partial')", query)
        self.assertIn("o.sensitivity not in ('public', 'internal')", query)
        self.assertIn("limit 25", query)
        self.assertIn("from (", query)
        self.assertNotIn("story_key", query)
        for forbidden in (
            "heartbeat_run_artifacts",
            "heartbeat_deliveries",
            "normalized_payload",
            "error",
            "provider_message_id",
        ):
            self.assertNotIn(forbidden, query)

    async def test_profile_and_principal_are_required(self) -> None:
        pool = FakePool(profile=None, run=self.run)
        with self.assertRaises(PermissionError):
            await state(pool, run_id=self.current_run_id).list_previous_runs(
                profile_id=str(self.profile_id)
            )

        pool = FakePool(profile=self.profile, run=None)
        with self.assertRaises(PermissionError):
            await state(
                pool, run_id=self.current_run_id, principal="workflow-heartbeat-other"
            ).list_previous_runs(profile_id=str(self.profile_id))

    async def test_facade_is_only_available_to_heartbeat_run(self) -> None:
        pool = FakePool(profile=self.profile, run=self.run)
        feedback = HeartbeatState(
            pool,
            workflow_name="heartbeat_feedback",
            workflow_run_id=str(self.current_run_id),
            workflow_task_id=str(uuid.uuid4()),
            workflow_principal="workflow-heartbeat-feedback",
        )
        with self.assertRaises(PermissionError):
            await feedback.list_previous_runs(profile_id=str(self.profile_id))


if __name__ == "__main__":
    unittest.main()
