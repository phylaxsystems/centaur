from __future__ import annotations

import asyncio
import importlib
import json
import sys
import types
from pathlib import Path


def _load_sync():
    repo_root = Path(__file__).resolve().parents[3]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    api_module = sys.modules.setdefault("api", types.ModuleType("api"))

    runtime_control = types.ModuleType("api.runtime_control")
    runtime_control.canonical_json = lambda value: json.dumps(value, sort_keys=True)
    api_module.runtime_control = runtime_control
    sys.modules["api.runtime_control"] = runtime_control

    etl_metrics = types.ModuleType("workflows.etl_metrics")
    for name in (
        "record_etl_items_deleted",
        "record_etl_items_enqueued",
        "record_etl_items_failed",
        "record_etl_items_seen",
        "record_etl_items_upserted",
        "set_etl_active_scopes",
        "set_etl_failed_scopes",
        "set_etl_scope_sync_freshness_seconds",
    ):
        setattr(etl_metrics, name, lambda *_args, **_kwargs: None)
    sys.modules["workflows.etl_metrics"] = etl_metrics

    slack_metrics = types.ModuleType("workflows.slack.metrics")
    for name in (
        "observe_slack_retention_run_duration",
        "record_slack_etl_rate_limit",
        "record_slack_retention_api_rate_limited",
        "record_slack_retention_api_request",
        "record_slack_retention_channel_failure",
        "record_slack_retention_failure",
        "record_slack_retention_messages_processed",
        "record_slack_retention_run",
        "set_slack_retention_last_failure_timestamp",
        "set_slack_retention_watermark_lag_seconds",
    ):
        setattr(slack_metrics, name, lambda *_args, **_kwargs: None)
    sys.modules["workflows.slack.metrics"] = slack_metrics

    workflow_engine = types.ModuleType("api.workflow_engine")
    workflow_engine.WorkflowContext = object
    api_module.workflow_engine = workflow_engine
    sys.modules["api.workflow_engine"] = workflow_engine

    centaur_sdk = sys.modules.setdefault("centaur_sdk", types.ModuleType("centaur_sdk"))
    centaur_sdk.secret = lambda _name, default=None: default

    return importlib.import_module("workflows.slack.sync")


class FakeContext:
    run_id = "wfr_test"
    _pool = object()

    def __init__(self) -> None:
        self.logs: list[tuple[str, dict]] = []

    def log(self, name: str, **fields):
        self.logs.append((name, fields))


class FakeClient:
    def __init__(
        self,
        *,
        cursor: str | None = None,
        channels: list[dict] | None = None,
    ) -> None:
        self.history_calls: list[dict] = []
        self.channel_calls: list[dict] = []
        self.cursor = cursor
        self.channels = channels or [{"id": "C123", "name": "cold-start"}]

    def _etl_access_mode(self):
        return "test"

    def _list_etl_channels(self, *_args, **kwargs):
        self.channel_calls.append(kwargs)
        return self.channels

    def _list_etl_users(self, *_args, **_kwargs):
        return []

    def _sync_etl_channel_history(self, channel_id, **kwargs):
        self.history_calls.append({"channel_id": channel_id, **kwargs})
        return {
            "messages": [
                {
                    "channel_id": channel_id,
                    "timestamp": "1770000000.000100",
                    "thread_ts": "1770000000.000100",
                    "text": "hello",
                }
            ],
            "sync_state": {
                "cursor": self.cursor,
                "watermark": "1770000000.000100",
                "oldest": kwargs["oldest"],
                "latest": None,
            },
        }


async def _noop(*_args, **_kwargs):
    return None


async def _zero(*_args, **_kwargs):
    return 0


async def _zero_purge(*_args, **_kwargs):
    return {
        "company_context_documents": 0,
        "public_channels": 0,
        "private_channels": 0,
    }


def _patch_handler_io(monkeypatch, sync, *, checkpoint=None, client=None):
    calls: dict[str, list] = {
        "checkpoint_success": [],
        "checkpoint_failure": [],
        "enqueued": [],
        "finish": [],
        "run_start": [],
        "widened": [],
    }
    fake_client = client or FakeClient()

    async def fake_load_checkpoint(_pool, _channel_id):
        return checkpoint

    async def fake_upsert_messages(_pool, rows):
        return len(rows)

    async def fake_load_thread_refresh_times(*_args, **_kwargs):
        return {}

    async def fake_update_checkpoint_success(_pool, **kwargs):
        calls["checkpoint_success"].append(kwargs)

    async def fake_update_checkpoint_failure(_pool, **kwargs):
        calls["checkpoint_failure"].append(kwargs)

    async def fake_enqueue_backfill_job(_pool, **kwargs):
        calls["enqueued"].append(kwargs)

    async def fake_record_run_finish(_pool, **kwargs):
        calls["finish"].append(kwargs)

    async def fake_record_run_start(_pool, **kwargs):
        calls["run_start"].append(kwargs)

    async def fake_widen_channel_bootstrap_job(_pool, **kwargs):
        calls["widened"].append(kwargs)
        return False

    monkeypatch.setattr(sync, "_client", lambda: fake_client)
    monkeypatch.setattr(sync, "_upsert_channels", _noop)
    monkeypatch.setattr(sync, "_purge_excluded_channel_data", _zero_purge)
    monkeypatch.setattr(sync, "_upsert_users", _zero)
    monkeypatch.setattr(sync, "_load_checkpoint", fake_load_checkpoint)
    monkeypatch.setattr(sync, "_upsert_messages", fake_upsert_messages)
    monkeypatch.setattr(sync, "load_thread_refresh_times", fake_load_thread_refresh_times)
    monkeypatch.setattr(sync, "_update_checkpoint_success", fake_update_checkpoint_success)
    monkeypatch.setattr(sync, "_update_checkpoint_failure", fake_update_checkpoint_failure)
    monkeypatch.setattr(sync, "enqueue_backfill_job", fake_enqueue_backfill_job)
    monkeypatch.setattr(sync, "emit_slack_checkpoint_metrics", _noop)
    monkeypatch.setattr(sync, "record_run_start", fake_record_run_start)
    monkeypatch.setattr(sync, "record_run_finish", fake_record_run_finish)
    monkeypatch.setattr(
        sync,
        "widen_channel_bootstrap_job",
        fake_widen_channel_bootstrap_job,
    )

    return fake_client, calls


class FakePurgeConnection:
    def __init__(self) -> None:
        self.fetchval_calls: list[tuple[str, tuple]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    def transaction(self):
        return self

    async def fetch(self, query):
        if "FROM slack_sync_channels" in query:
            return [
                {"channel_id": "C_STORED", "channel_name": "Sensitive-Archive"},
                {"channel_id": "C_GENERAL", "channel_name": "general"},
            ]
        if "FROM slack_private_sync_conversations" in query:
            return [
                {
                    "home_team_id": "T1",
                    "conversation_id": "G_PRIVATE",
                    "channel_name": "sensitive-private",
                },
                {
                    "home_team_id": "T1",
                    "conversation_id": "G_STRATEGY",
                    "channel_name": "strategy",
                },
            ]
        raise AssertionError(f"unexpected query: {query}")

    async def fetchval(self, query, *args):
        self.fetchval_calls.append((query, args))
        if "DELETE FROM company_context_documents" in query:
            return 3
        if "DELETE FROM slack_sync_channels" in query:
            return 2
        if "DELETE FROM slack_private_sync_conversations" in query:
            return 1
        raise AssertionError(f"unexpected query: {query}")


class FakePurgePool:
    def __init__(self) -> None:
        self.connection = FakePurgeConnection()

    def acquire(self):
        return self.connection


def test_purge_excluded_channel_data_removes_public_private_and_derived_rows():
    sync = _load_sync()
    pool = FakePurgePool()

    result = asyncio.run(
        sync._purge_excluded_channel_data(
            pool,
            ["sensitive-*"],
            [{"id": "C_DISCOVERED", "name": "sensitive-current"}],
        )
    )

    assert result == {
        "company_context_documents": 3,
        "public_channels": 2,
        "private_channels": 1,
    }
    calls = pool.connection.fetchval_calls
    assert calls[0][1] == (["C_DISCOVERED", "C_STORED", "G_PRIVATE"],)
    assert calls[1][1] == (["C_DISCOVERED", "C_STORED", "G_PRIVATE"],)
    assert calls[2][1] == (["T1"], ["G_PRIVATE"])


def test_cold_start_channel_uses_full_lookback_window(monkeypatch):
    monkeypatch.setenv("SLACK_ETL_ENABLED", "true")
    sync = _load_sync()
    client, calls = _patch_handler_io(monkeypatch, sync)

    monkeypatch.setattr(sync, "_ts_now_minus_days", lambda days: f"days:{days}")

    result = asyncio.run(sync.handler(sync.Input(), FakeContext()))

    assert result["status"] == "completed"
    assert client.channel_calls[0]["include_private_channels"] is False
    assert client.history_calls[0]["oldest"] == "days:30"
    assert calls["run_start"][0]["metadata"]["index_private_channels"] is False
    assert calls["checkpoint_success"] == [
        {
            "channel_id": "C123",
            "watermark_ts": "1770000000.000100",
            "run_id": "slack_sync_wfr_test",
        }
    ]
    assert calls["enqueued"] == []
    assert calls["widened"] == []


def test_watermarked_channel_keeps_incremental_overlap(monkeypatch):
    monkeypatch.setenv("SLACK_ETL_ENABLED", "true")
    sync = _load_sync()
    client, calls = _patch_handler_io(
        monkeypatch,
        sync,
        checkpoint={"watermark_ts": "1771000000.000100", "last_error": ""},
    )

    monkeypatch.setattr(
        sync,
        "_ts_minus_days",
        lambda ts, days: f"minus:{ts}:{days}",
    )
    monkeypatch.setattr(sync, "_ts_now_minus_days", lambda days: f"days:{days}")

    result = asyncio.run(sync.handler(sync.Input(), FakeContext()))

    assert result["status"] == "completed"
    assert client.history_calls[0]["oldest"] == "minus:1771000000.000100:3"
    assert calls["widened"] == [
        {
            "channel_id": "C123",
            "window_oldest": "days:30",
            "lookback_days": 30,
            "thread_lookback_days": 3,
            "run_id": "slack_sync_wfr_test",
            "priority": 150,
        }
    ]


def test_private_channel_flag_is_passed_to_discovery(monkeypatch):
    monkeypatch.setenv("SLACK_ETL_ENABLED", "true")
    monkeypatch.setenv("SLACK_SYNC_INDEX_PRIVATE_CHANNELS", "true")
    sync = _load_sync()
    client, calls = _patch_handler_io(monkeypatch, sync)

    monkeypatch.setattr(sync, "_ts_now_minus_days", lambda days: f"days:{days}")

    result = asyncio.run(sync.handler(sync.Input(), FakeContext()))

    assert result["status"] == "completed"
    assert client.channel_calls[0]["include_private_channels"] is True
    assert calls["run_start"][0]["metadata"]["index_private_channels"] is True


def test_non_member_channel_is_reported_as_skipped_not_failed(monkeypatch):
    monkeypatch.setenv("SLACK_ETL_ENABLED", "true")
    sync = _load_sync()
    client, calls = _patch_handler_io(
        monkeypatch,
        sync,
        client=FakeClient(
            channels=[
                {
                    "id": "C123",
                    "name": "not-joined",
                    "is_member": False,
                }
            ]
        ),
    )

    result = asyncio.run(sync.handler(sync.Input(), FakeContext()))

    assert result["status"] == "skipped"
    assert result["reason"] == "all_channels_skipped"
    assert client.history_calls == []
    assert calls["run_start"] == []
    assert calls["finish"] == []
    assert result["channels_skipped"] == [
        {
            "channel_id": "C123",
            "channel_name": "not-joined",
            "reason": sync.NOT_IN_CHANNEL_SKIP_REASON,
        }
    ]
    assert calls["checkpoint_failure"] == []


def test_non_member_channel_skip_is_recorded_on_run_start_and_finish(monkeypatch):
    monkeypatch.setenv("SLACK_ETL_ENABLED", "true")
    sync = _load_sync()
    client, calls = _patch_handler_io(
        monkeypatch,
        sync,
        client=FakeClient(
            channels=[
                {"id": "C123", "name": "cold-start", "is_member": True},
                {"id": "C456", "name": "not-joined", "is_member": False},
            ]
        ),
    )

    result = asyncio.run(sync.handler(sync.Input(), FakeContext()))

    expected_skipped = [
        {
            "channel_id": "C456",
            "channel_name": "not-joined",
            "reason": sync.NOT_IN_CHANNEL_SKIP_REASON,
        }
    ]
    assert result["status"] == "completed"
    assert result["channels_synced"] == 1
    assert result["channels_skipped"] == 1
    assert [call["channel_id"] for call in client.history_calls] == ["C123"]
    assert calls["run_start"][0]["skipped"] == expected_skipped
    assert calls["finish"][0]["skipped"] == expected_skipped
