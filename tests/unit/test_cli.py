from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

from hhru_platform.domain.entities.crawl_partition import CrawlPartition
from hhru_platform.domain.entities.crawl_run import CrawlRun
from hhru_platform.interfaces.cli.main import main


def test_cli_help_returns_zero(monkeypatch, capsys) -> None:
    monkeypatch.setattr("sys.argv", ["hhru-platform"])
    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "health-check" in captured.out
    assert "create-run" in captured.out
    assert "plan-run" in captured.out
    assert "sync-dictionaries" in captured.out


def test_create_run_cli_prints_created_run(monkeypatch, capsys) -> None:
    created_run = CrawlRun(
        id=uuid4(),
        run_type="weekly_sweep",
        status="created",
        started_at=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
        finished_at=None,
        triggered_by="cli",
        config_snapshot_json={},
        partitions_total=0,
        partitions_done=0,
        partitions_failed=0,
        notes=None,
    )

    @contextmanager
    def fake_session_scope():
        yield object()

    def fake_create_crawl_run(command, repository) -> CrawlRun:
        assert command.run_type == "weekly_sweep"
        assert command.triggered_by == "cli"
        return created_run

    class FakeCrawlRunRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.run.session_scope",
        fake_session_scope,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.run.SqlAlchemyCrawlRunRepository",
        FakeCrawlRunRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.run.create_crawl_run",
        fake_create_crawl_run,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "create-run",
            "--run-type",
            "weekly_sweep",
            "--triggered-by",
            "cli",
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "created crawl_run" in captured.out
    assert f"id={created_run.id}" in captured.out
    assert "status=created" in captured.out


def test_plan_run_cli_prints_planned_partitions(monkeypatch, capsys) -> None:
    created_partition = CrawlPartition(
        id=uuid4(),
        crawl_run_id=uuid4(),
        partition_key="global-default",
        params_json={"planner_policy": "single_partition_v1"},
        status="pending",
        pages_total_expected=None,
        pages_processed=0,
        items_seen=0,
        retry_count=0,
        started_at=None,
        finished_at=None,
        last_error_message=None,
        created_at=datetime(2026, 3, 12, 12, 5, tzinfo=UTC),
    )

    @contextmanager
    def fake_session_scope():
        yield object()

    def fake_plan_sweep(command, crawl_run_repository, crawl_partition_repository, planner_policy):
        assert command.crawl_run_id == created_partition.crawl_run_id
        assert planner_policy.__class__.__name__ == "SinglePartitionPlannerPolicyV1"
        return SimpleNamespace(
            crawl_run_id=created_partition.crawl_run_id,
            created_partitions=[created_partition],
            partitions=[created_partition],
        )

    class FakeCrawlRunRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeCrawlPartitionRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.partition.session_scope",
        fake_session_scope,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.partition.SqlAlchemyCrawlRunRepository",
        FakeCrawlRunRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.partition.SqlAlchemyCrawlPartitionRepository",
        FakeCrawlPartitionRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.partition.plan_sweep",
        fake_plan_sweep,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "plan-run",
            "--run-id",
            str(created_partition.crawl_run_id),
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "planned crawl partitions" in captured.out
    assert f"run_id={created_partition.crawl_run_id}" in captured.out
    assert "partitions_created=1" in captured.out
    assert f"key={created_partition.partition_key}" in captured.out


def test_sync_dictionaries_cli_prints_sync_summary(monkeypatch, capsys) -> None:
    created_sync_run_id = uuid4()

    @contextmanager
    def fake_session_scope():
        yield object()

    class FakeDictionarySyncRunRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeApiRequestLogRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeRawApiPayloadRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeAreaRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeProfessionalRoleRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeDictionaryStore:
        def __init__(
            self,
            area_repository: object,
            professional_role_repository: object,
        ) -> None:
            self.area_repository = area_repository
            self.professional_role_repository = professional_role_repository

    class FakeHHApiClient:
        pass

    def fake_sync_dictionary(
        command,
        api_client,
        sync_run_repository,
        api_request_log_repository,
        raw_api_payload_repository,
        dictionary_store,
    ):
        assert command.dictionary_name == "areas"
        assert api_client.__class__.__name__ == "FakeHHApiClient"
        assert sync_run_repository.__class__.__name__ == "FakeDictionarySyncRunRepository"
        assert api_request_log_repository.__class__.__name__ == "FakeApiRequestLogRepository"
        assert raw_api_payload_repository.__class__.__name__ == "FakeRawApiPayloadRepository"
        assert dictionary_store.__class__.__name__ == "FakeDictionaryStore"
        return SimpleNamespace(
            dictionary_name="areas",
            sync_run_id=created_sync_run_id,
            status="succeeded",
            source_status_code=200,
            created_count=2,
            updated_count=1,
            deactivated_count=0,
            request_log_id=17,
            raw_payload_id=29,
            error_message=None,
        )

    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.dictionary.session_scope",
        fake_session_scope,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.dictionary.SqlAlchemyDictionarySyncRunRepository",
        FakeDictionarySyncRunRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.dictionary.SqlAlchemyApiRequestLogRepository",
        FakeApiRequestLogRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.dictionary.SqlAlchemyRawApiPayloadRepository",
        FakeRawApiPayloadRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.dictionary.SqlAlchemyAreaRepository",
        FakeAreaRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.dictionary.SqlAlchemyProfessionalRoleRepository",
        FakeProfessionalRoleRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.dictionary.SqlAlchemyDictionaryStore",
        FakeDictionaryStore,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.dictionary.HHApiClient",
        FakeHHApiClient,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.dictionary.sync_dictionary",
        fake_sync_dictionary,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "sync-dictionaries",
            "--name",
            "areas",
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "dictionary sync result" in captured.out
    assert "name=areas" in captured.out
    assert f"sync_run_id={created_sync_run_id}" in captured.out
    assert "status=succeeded" in captured.out
    assert "created=2" in captured.out
    assert "updated=1" in captured.out
