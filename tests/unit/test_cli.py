from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

from hhru_platform.application.commands.reconcile_run import ReconcileRunResult
from hhru_platform.application.commands.run_collection_once import RunCollectionOnceResult
from hhru_platform.config.settings import Settings
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
    assert "run-once" in captured.out
    assert "plan-run" in captured.out
    assert "plan-run-v2" in captured.out
    assert "split-partition" in captured.out
    assert "show-run-coverage" in captured.out
    assert "show-run-tree" in captured.out
    assert "sync-dictionaries" in captured.out
    assert "process-list-page" in captured.out
    assert "process-partition-v2" in captured.out
    assert "run-list-engine-v2" in captured.out
    assert "fetch-vacancy-detail" in captured.out
    assert "reconcile-run" in captured.out
    assert "study-detail-payloads" in captured.out
    assert "show-metrics" in captured.out
    assert "serve-metrics" in captured.out


def test_health_check_cli_prints_runtime_config(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.health.get_settings",
        lambda: Settings(
            env="production",
            db_host="db.internal",
            db_port=5432,
            db_name="hhru_platform",
            db_user="hhru",
            db_password="secret",
            redis_host="redis.internal",
            redis_port=6379,
            redis_db=1,
            hh_api_base_url="https://api.hh.ru",
            hh_api_timeout_seconds=15.0,
            hh_api_user_agent="hhru-platform/0.1 (contact: ops@example.com)",
            metrics_host="0.0.0.0",
            metrics_port=8001,
            metrics_state_path=".state/metrics/metrics.json",
            backup_dir=".state/backups",
            backup_retention_days=14,
        ),
    )
    monkeypatch.setattr("sys.argv", ["hhru-platform", "health-check"])

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "env=production" in captured.out
    assert (
        "database_url=postgresql+psycopg://hhru:secret@db.internal:5432/hhru_platform"
        in captured.out
    )
    assert "redis_url=redis://redis.internal:6379/1" in captured.out
    assert "hh_api_user_agent=hhru-platform/0.1 (contact: ops@example.com)" in captured.out
    assert "hh_api_user_agent_live_search_valid=yes" in captured.out
    assert "metrics_state_path=.state/metrics/metrics.json" in captured.out
    assert "backup_retention_days=14" in captured.out


def test_run_once_cli_prints_summary(monkeypatch, capsys) -> None:
    run_id = uuid4()

    def fake_run_collection_once(command, **kwargs) -> RunCollectionOnceResult:
        assert command.sync_dictionaries is True
        assert command.pages_per_partition == 2
        assert command.detail_limit == 3
        assert command.run_type == "weekly_sweep"
        assert command.triggered_by == "cli"
        assert "sync_dictionary_step" in kwargs
        assert "create_crawl_run_step" in kwargs
        assert "plan_run_step" in kwargs
        assert "process_list_page_step" in kwargs
        assert "fetch_vacancy_detail_step" in kwargs
        assert "reconcile_run_step" in kwargs
        return RunCollectionOnceResult(
            status="succeeded",
            run_id=run_id,
            run_type="weekly_sweep",
            triggered_by="cli",
            dictionary_results=(),
            planned_partition_ids=(uuid4(),),
            list_page_results=(),
            detail_results=(),
            reconciliation_result=ReconcileRunResult(
                crawl_run_id=run_id,
                observed_in_run_count=0,
                missing_updated_count=0,
                marked_inactive_count=0,
                run_status="completed",
            ),
            completed_steps=("create_crawl_run", "plan_sweep", "reconcile_run"),
        )

    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.run_once.run_collection_once",
        fake_run_collection_once,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "run-once",
            "--sync-dictionaries",
            "yes",
            "--pages-per-partition",
            "2",
            "--detail-limit",
            "3",
            "--run-type",
            "weekly_sweep",
            "--triggered-by",
            "cli",
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "completed run-once collection" in captured.out
    assert "status=succeeded" in captured.out
    assert f"run_id={run_id}" in captured.out
    assert "partitions_planned=1" in captured.out
    assert "partitions_attempted=0" in captured.out
    assert "list_pages_processed=0" in captured.out
    assert "detail_fetch_attempted=0" in captured.out
    assert "reconciliation_status=completed" in captured.out
    assert "completed_steps=create_crawl_run,plan_sweep,reconcile_run" in captured.out
    assert "failed_step=-" in captured.out


def test_run_once_cli_returns_non_zero_and_prints_failure_summary(monkeypatch, capsys) -> None:
    run_id = uuid4()

    def fake_run_collection_once(command, **kwargs) -> RunCollectionOnceResult:
        return RunCollectionOnceResult(
            status="failed",
            run_id=run_id,
            run_type=command.run_type,
            triggered_by=command.triggered_by,
            dictionary_results=(),
            planned_partition_ids=(uuid4(),),
            list_page_results=(),
            detail_results=(),
            reconciliation_result=None,
            failed_step="process_list_page",
            error_message="Invalid HH API User-Agent for live vacancy search",
            completed_steps=("create_crawl_run", "plan_sweep"),
            skipped_steps=("fetch_vacancy_detail", "reconcile_run"),
        )

    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.run_once.run_collection_once",
        fake_run_collection_once,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "run-once",
            "--pages-per-partition",
            "1",
            "--detail-limit",
            "1",
            "--run-type",
            "weekly_sweep",
            "--triggered-by",
            "cli",
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "failed run-once collection" in captured.out
    assert "status=failed" in captured.out
    assert f"run_id={run_id}" in captured.out
    assert "reconciliation_status=skipped" in captured.out
    assert "completed_steps=create_crawl_run,plan_sweep" in captured.out
    assert "skipped_steps=fetch_vacancy_detail,reconcile_run" in captured.out
    assert "failed_step=process_list_page" in captured.out
    assert "error=Invalid HH API User-Agent for live vacancy search" in captured.out


def test_study_detail_payloads_cli_prints_report_summary(monkeypatch, capsys, tmp_path) -> None:
    report_directory = tmp_path / "detail-study"
    report_json_path = report_directory / "report.json"
    summary_markdown_path = report_directory / "summary.md"
    run_id = uuid4()

    def fake_study_detail_payloads(command, **kwargs):
        assert command.sample_size == 3
        assert command.repeat_fetches == 2
        assert command.pause_seconds == 1.5
        assert command.crawl_run_id == run_id
        assert command.output_dir == report_directory
        assert "resolve_latest_crawl_run_id_step" in kwargs
        assert "load_candidates_step" in kwargs
        assert "load_raw_payload_step" in kwargs
        assert "fetch_detail_step" in kwargs
        return SimpleNamespace(
            crawl_run_id=run_id,
            sample_size_requested=3,
            sample_size_selected=3,
            vacancies_with_search_sample=3,
            vacancies_with_successful_detail=3,
            raw_comparable_pairs=6,
            raw_changed_pairs=1,
            normalized_comparable_pairs=6,
            normalized_changed_pairs=0,
            report_directory=report_directory,
            report_json_path=report_json_path,
            summary_markdown_path=summary_markdown_path,
            recommendation=(
                "Prefer exhaustive list coverage plus selective detail fetches on first_seen, "
                "short_changed, and a TTL refresh."
            ),
            detail_only_research_fields=("description", "key_skills[].name"),
        )

    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.research.study_detail_payloads",
        fake_study_detail_payloads,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "study-detail-payloads",
            "--sample-size",
            "3",
            "--repeat-fetches",
            "2",
            "--pause-seconds",
            "1.5",
            "--crawl-run-id",
            str(run_id),
            "--output-dir",
            str(report_directory),
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "completed detail payload study" in captured.out
    assert f"crawl_run_id={run_id}" in captured.out
    assert "sample_size_selected=3" in captured.out
    assert "raw_changed_pairs=1" in captured.out
    assert "normalized_changed_pairs=0" in captured.out
    assert "detail_only_research_fields=description,key_skills[].name" in captured.out
    assert "recommended_policy=Prefer exhaustive list coverage plus selective detail fetches" in (
        captured.out
    )
    assert f"report_directory={report_directory}" in captured.out


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


def test_plan_run_v2_cli_prints_tree_roots(monkeypatch, capsys) -> None:
    created_partition = CrawlPartition(
        id=uuid4(),
        crawl_run_id=uuid4(),
        partition_key="area:113",
        params_json={"planner_policy": "area_exhaustive_v2", "params": {"area": "113"}},
        status="pending",
        pages_total_expected=None,
        pages_processed=0,
        items_seen=0,
        retry_count=0,
        started_at=None,
        finished_at=None,
        last_error_message=None,
        created_at=datetime(2026, 3, 19, 12, 5, tzinfo=UTC),
        parent_partition_id=None,
        depth=0,
        split_dimension="area",
        split_value="113",
        scope_key="area:113",
        planner_policy_version="v2",
        is_terminal=True,
        is_saturated=False,
        coverage_status="unassessed",
    )

    @contextmanager
    def fake_session_scope():
        yield object()

    def fake_plan_sweep_v2(
        command,
        crawl_run_repository,
        crawl_partition_repository,
        area_repository,
    ):
        assert command.crawl_run_id == created_partition.crawl_run_id
        assert crawl_run_repository.__class__.__name__ == "FakeCrawlRunRepository"
        assert crawl_partition_repository.__class__.__name__ == "FakeCrawlPartitionRepository"
        assert area_repository.__class__.__name__ == "FakeAreaRepository"
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

    class FakeAreaRepository:
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
        "hhru_platform.interfaces.cli.commands.partition.SqlAlchemyAreaRepository",
        FakeAreaRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.partition.plan_sweep_v2",
        fake_plan_sweep_v2,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "plan-run-v2",
            "--run-id",
            str(created_partition.crawl_run_id),
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "planned crawl partitions with planner v2" in captured.out
    assert f"run_id={created_partition.crawl_run_id}" in captured.out
    assert "partitions_created=1" in captured.out
    assert "scope_key=area:113" in captured.out
    assert "depth=0" in captured.out


def test_split_partition_cli_prints_child_partition_summary(monkeypatch, capsys) -> None:
    parent_partition_id = uuid4()
    run_id = uuid4()
    child_partition = CrawlPartition(
        id=uuid4(),
        crawl_run_id=run_id,
        partition_key="area:1",
        params_json={"planner_policy": "area_exhaustive_v2", "params": {"area": "1"}},
        status="pending",
        pages_total_expected=None,
        pages_processed=0,
        items_seen=0,
        retry_count=0,
        started_at=None,
        finished_at=None,
        last_error_message=None,
        created_at=datetime(2026, 3, 19, 12, 6, tzinfo=UTC),
        parent_partition_id=parent_partition_id,
        depth=1,
        split_dimension="area",
        split_value="1",
        scope_key="area:1",
        planner_policy_version="v2",
        is_terminal=True,
        is_saturated=False,
        coverage_status="unassessed",
    )
    parent_partition = CrawlPartition(
        id=parent_partition_id,
        crawl_run_id=run_id,
        partition_key="area:113",
        params_json={"planner_policy": "area_exhaustive_v2", "params": {"area": "113"}},
        status="split_done",
        pages_total_expected=2000,
        pages_processed=1,
        items_seen=100,
        retry_count=0,
        started_at=datetime(2026, 3, 19, 12, 0, tzinfo=UTC),
        finished_at=datetime(2026, 3, 19, 12, 7, tzinfo=UTC),
        last_error_message=None,
        created_at=datetime(2026, 3, 19, 12, 0, tzinfo=UTC),
        parent_partition_id=None,
        depth=0,
        split_dimension="area",
        split_value="113",
        scope_key="area:113",
        planner_policy_version="v2",
        is_terminal=False,
        is_saturated=True,
        coverage_status="split",
    )

    @contextmanager
    def fake_session_scope():
        yield object()

    class FakeCrawlRunRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeCrawlPartitionRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeAreaRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    def fake_split_partition(
        command,
        crawl_partition_repository,
        crawl_run_repository,
        area_repository,
    ):
        assert command.partition_id == parent_partition_id
        assert crawl_partition_repository.__class__.__name__ == "FakeCrawlPartitionRepository"
        assert crawl_run_repository.__class__.__name__ == "FakeCrawlRunRepository"
        assert area_repository.__class__.__name__ == "FakeAreaRepository"
        return SimpleNamespace(
            parent_partition=parent_partition,
            created_children=(child_partition,),
            children=(child_partition,),
            resolution_message=None,
        )

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
        "hhru_platform.interfaces.cli.commands.partition.SqlAlchemyAreaRepository",
        FakeAreaRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.partition.split_partition",
        fake_split_partition,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "split-partition",
            "--partition-id",
            str(parent_partition_id),
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "split crawl partition" in captured.out
    assert f"partition_id={parent_partition_id}" in captured.out
    assert "status=split_done" in captured.out
    assert "children_created=1" in captured.out
    assert "children_total=1" in captured.out
    assert "scope_key=area:1" in captured.out


def test_show_run_coverage_cli_prints_tree_based_summary(monkeypatch, capsys) -> None:
    run_id = uuid4()

    def fake_load_run_coverage_report(crawl_run_id):
        assert crawl_run_id == run_id
        return SimpleNamespace(
            crawl_run=SimpleNamespace(id=run_id, run_type="weekly_sweep", status="created"),
            summary=SimpleNamespace(
                crawl_run_id=run_id,
                run_type="weekly_sweep",
                run_status="created",
                total_partitions=6,
                root_partitions=4,
                terminal_partitions=5,
                covered_terminal_partitions=2,
                pending_partitions=1,
                pending_terminal_partitions=1,
                running_partitions=1,
                split_partitions=1,
                unresolved_partitions=1,
                failed_partitions=0,
                coverage_ratio=0.4,
                is_fully_covered=False,
            ),
            tree_rows=(),
        )

    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.reporting._load_run_coverage_report",
        fake_load_run_coverage_report,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "show-run-coverage",
            "--run-id",
            str(run_id),
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "run coverage summary" in captured.out
    assert f"run_id={run_id}" in captured.out
    assert "root_partitions=4" in captured.out
    assert "covered_terminal_partitions=2" in captured.out
    assert "pending_terminal_partitions=1" in captured.out
    assert "coverage_ratio=0.4000" in captured.out
    assert "fully_covered=no" in captured.out


def test_show_run_tree_cli_prints_nested_partition_rows(monkeypatch, capsys) -> None:
    run_id = uuid4()
    root_partition_id = uuid4()
    child_partition_id = uuid4()

    def fake_load_run_coverage_report(crawl_run_id):
        assert crawl_run_id == run_id
        return SimpleNamespace(
            crawl_run=SimpleNamespace(id=run_id, run_type="weekly_sweep", status="created"),
            summary=SimpleNamespace(),
            tree_rows=(
                SimpleNamespace(
                    partition_id=root_partition_id,
                    parent_partition_id=None,
                    depth=0,
                    scope_key="area:113",
                    status="split_done",
                    coverage_status="split",
                    is_terminal=False,
                    is_saturated=True,
                ),
                SimpleNamespace(
                    partition_id=child_partition_id,
                    parent_partition_id=root_partition_id,
                    depth=1,
                    scope_key="area:1",
                    status="done",
                    coverage_status="covered",
                    is_terminal=True,
                    is_saturated=False,
                ),
            ),
        )

    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.reporting._load_run_coverage_report",
        fake_load_run_coverage_report,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "show-run-tree",
            "--run-id",
            str(run_id),
            "--max-depth",
            "1",
            "--max-rows",
            "10",
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "run partition tree" in captured.out
    assert f"run_id={run_id}" in captured.out
    assert "shown_rows=2" in captured.out
    assert (
        "partition="
        f"{root_partition_id} parent=- depth=0 scope_key=area:113 status=split_done"
        in captured.out
    )
    assert (
        "  partition="
        f"{child_partition_id} parent={root_partition_id} depth=1 scope_key=area:1"
        in captured.out
    )


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
        "hhru_platform.interfaces.cli.commands.dictionary.HHApiClient.from_settings",
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


def test_process_list_page_cli_prints_processing_summary(monkeypatch, capsys) -> None:
    partition_id = uuid4()

    @contextmanager
    def fake_session_scope():
        yield object()

    class FakeCrawlPartitionRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeApiRequestLogRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeRawApiPayloadRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeVacancyRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeVacancySeenEventRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeVacancyCurrentStateRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeHHApiClient:
        pass

    def fake_process_list_page(
        command,
        crawl_partition_repository,
        api_client,
        api_request_log_repository,
        raw_api_payload_repository,
        vacancy_repository,
        vacancy_seen_event_repository,
        vacancy_current_state_repository,
    ):
        assert command.partition_id == partition_id
        assert command.page == 0
        assert crawl_partition_repository.__class__.__name__ == "FakeCrawlPartitionRepository"
        assert api_client.__class__.__name__ == "FakeHHApiClient"
        assert api_request_log_repository.__class__.__name__ == "FakeApiRequestLogRepository"
        assert raw_api_payload_repository.__class__.__name__ == "FakeRawApiPayloadRepository"
        assert vacancy_repository.__class__.__name__ == "FakeVacancyRepository"
        assert vacancy_seen_event_repository.__class__.__name__ == "FakeVacancySeenEventRepository"
        assert (
            vacancy_current_state_repository.__class__.__name__
            == "FakeVacancyCurrentStateRepository"
        )
        return SimpleNamespace(
            partition_id=partition_id,
            partition_status="done",
            page=0,
            pages_total_expected=12,
            vacancies_processed=2,
            vacancies_created=1,
            seen_events_created=2,
            request_log_id=41,
            raw_payload_id=42,
            processed_vacancies=[
                SimpleNamespace(
                    id=uuid4(),
                    hh_vacancy_id="pytest-vacancy-1",
                ),
                SimpleNamespace(
                    id=uuid4(),
                    hh_vacancy_id="pytest-vacancy-2",
                ),
            ],
            error_message=None,
        )

    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.list_page.session_scope",
        fake_session_scope,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.list_page.SqlAlchemyCrawlPartitionRepository",
        FakeCrawlPartitionRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.list_page.SqlAlchemyApiRequestLogRepository",
        FakeApiRequestLogRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.list_page.SqlAlchemyRawApiPayloadRepository",
        FakeRawApiPayloadRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.list_page.SqlAlchemyVacancyRepository",
        FakeVacancyRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.list_page.SqlAlchemyVacancySeenEventRepository",
        FakeVacancySeenEventRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.list_page.SqlAlchemyVacancyCurrentStateRepository",
        FakeVacancyCurrentStateRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.list_page.HHApiClient.from_settings",
        FakeHHApiClient,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.list_page.process_list_page",
        fake_process_list_page,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "process-list-page",
            "--partition-id",
            str(partition_id),
            "--page",
            "0",
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "processed list page" in captured.out
    assert f"partition_id={partition_id}" in captured.out
    assert "status=done" in captured.out
    assert "vacancies_processed=2" in captured.out
    assert "vacancies_created=1" in captured.out
    assert "seen_events_created=2" in captured.out
    assert "hh_vacancy_id=pytest-vacancy-1" in captured.out


def test_process_partition_v2_cli_prints_partition_summary(monkeypatch, capsys) -> None:
    partition_id = uuid4()
    run_id = uuid4()

    def fake_execute_process_partition_v2_step(command, *, api_client, saturation_policy):
        assert command.partition_id == partition_id
        assert api_client.__class__.__name__ == "FakeHHApiClient"
        assert saturation_policy.__class__.__name__ == "FakeSaturationPolicy"
        return SimpleNamespace(
            partition_id=partition_id,
            crawl_run_id=run_id,
            status="succeeded",
            final_partition_status="done",
            final_coverage_status="covered",
            pages_attempted=3,
            pages_processed=3,
            vacancies_found=5,
            vacancies_created=4,
            seen_events_created=5,
            saturated=False,
            children_created_count=0,
            children_total_count=0,
            saturation_reason=None,
            error_message=None,
        )

    class FakeHHApiClient:
        pass

    class FakeSaturationPolicy:
        pass

    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.list_engine._execute_process_partition_v2_step",
        fake_execute_process_partition_v2_step,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.list_engine.HHApiClient.from_settings",
        FakeHHApiClient,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.list_engine.PartitionSaturationPolicyV1",
        FakeSaturationPolicy,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "process-partition-v2",
            "--partition-id",
            str(partition_id),
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "processed crawl partition with list engine v2" in captured.out
    assert f"partition_id={partition_id}" in captured.out
    assert f"run_id={run_id}" in captured.out
    assert "partition_final_status=done" in captured.out
    assert "coverage_status=covered" in captured.out
    assert "pages_processed=3" in captured.out
    assert "vacancies_found=5" in captured.out
    assert "saturated=no" in captured.out


def test_run_list_engine_v2_cli_prints_run_summary(monkeypatch, capsys) -> None:
    run_id = uuid4()
    first_partition_id = uuid4()

    def fake_run_list_engine_v2(
        command,
        crawl_run_repository,
        crawl_partition_repository,
        process_partition_v2_step,
    ):
        assert command.crawl_run_id == run_id
        assert command.partition_limit == 2
        assert crawl_run_repository.__class__.__name__ == "_SessionlessCrawlRunRepository"
        assert (
            crawl_partition_repository.__class__.__name__
            == "_SessionlessCrawlPartitionRepository"
        )
        assert callable(process_partition_v2_step)
        return SimpleNamespace(
            status="succeeded",
            crawl_run_id=run_id,
            partitions_attempted=2,
            partitions_completed=2,
            partitions_failed=0,
            pages_attempted=4,
            pages_processed=4,
            vacancies_found=7,
            vacancies_created=6,
            seen_events_created=7,
            saturated_partitions=1,
            children_created_total=2,
            remaining_pending_terminal_count=0,
            partition_results=(
                SimpleNamespace(
                    partition_id=first_partition_id,
                    final_partition_status="split_done",
                    final_coverage_status="split",
                    pages_processed=1,
                    saturated=True,
                    children_created_count=2,
                ),
            ),
        )

    class FakeHHApiClient:
        pass

    class FakeSaturationPolicy:
        pass

    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.list_engine.run_list_engine_v2",
        fake_run_list_engine_v2,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.list_engine.HHApiClient.from_settings",
        FakeHHApiClient,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.list_engine.PartitionSaturationPolicyV1",
        FakeSaturationPolicy,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "run-list-engine-v2",
            "--run-id",
            str(run_id),
            "--partition-limit",
            "2",
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "completed list engine v2 run" in captured.out
    assert "status=succeeded" in captured.out
    assert f"run_id={run_id}" in captured.out
    assert "partitions_attempted=2" in captured.out
    assert "pages_processed=4" in captured.out
    assert "saturated_partitions=1" in captured.out
    assert "children_created_total=2" in captured.out
    assert f"partition={first_partition_id}" in captured.out


def test_fetch_vacancy_detail_cli_prints_fetch_summary(monkeypatch, capsys) -> None:
    vacancy_id = uuid4()

    @contextmanager
    def fake_session_scope():
        yield object()

    class FakeVacancyRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeDetailFetchAttemptRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeApiRequestLogRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeRawApiPayloadRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeVacancySnapshotRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeVacancyCurrentStateRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeHHApiClient:
        pass

    def fake_fetch_vacancy_detail(
        command,
        vacancy_repository,
        api_client,
        detail_fetch_attempt_repository,
        api_request_log_repository,
        raw_api_payload_repository,
        vacancy_snapshot_repository,
        vacancy_current_state_repository,
    ):
        assert command.vacancy_id == vacancy_id
        assert command.reason == "manual_refetch"
        assert vacancy_repository.__class__.__name__ == "FakeVacancyRepository"
        assert api_client.__class__.__name__ == "FakeHHApiClient"
        assert (
            detail_fetch_attempt_repository.__class__.__name__ == "FakeDetailFetchAttemptRepository"
        )
        assert api_request_log_repository.__class__.__name__ == "FakeApiRequestLogRepository"
        assert raw_api_payload_repository.__class__.__name__ == "FakeRawApiPayloadRepository"
        assert vacancy_snapshot_repository.__class__.__name__ == "FakeVacancySnapshotRepository"
        assert (
            vacancy_current_state_repository.__class__.__name__
            == "FakeVacancyCurrentStateRepository"
        )
        return SimpleNamespace(
            vacancy_id=vacancy_id,
            hh_vacancy_id="pytest-detail-vacancy",
            detail_fetch_status="succeeded",
            snapshot_id=51,
            request_log_id=52,
            raw_payload_id=53,
            detail_fetch_attempt_id=54,
            error_message=None,
        )

    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.detail.session_scope",
        fake_session_scope,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.detail.SqlAlchemyVacancyRepository",
        FakeVacancyRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.detail.SqlAlchemyDetailFetchAttemptRepository",
        FakeDetailFetchAttemptRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.detail.SqlAlchemyApiRequestLogRepository",
        FakeApiRequestLogRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.detail.SqlAlchemyRawApiPayloadRepository",
        FakeRawApiPayloadRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.detail.SqlAlchemyVacancySnapshotRepository",
        FakeVacancySnapshotRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.detail.SqlAlchemyVacancyCurrentStateRepository",
        FakeVacancyCurrentStateRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.detail.HHApiClient.from_settings",
        FakeHHApiClient,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.detail.fetch_vacancy_detail",
        fake_fetch_vacancy_detail,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "fetch-vacancy-detail",
            "--vacancy-id",
            str(vacancy_id),
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "fetched vacancy detail" in captured.out
    assert f"vacancy_id={vacancy_id}" in captured.out
    assert "hh_vacancy_id=pytest-detail-vacancy" in captured.out
    assert "detail_fetch_status=succeeded" in captured.out
    assert "snapshot_id=51" in captured.out
    assert "request_log_id=52" in captured.out
    assert "raw_payload_id=53" in captured.out
    assert "detail_fetch_attempt_id=54" in captured.out


def test_reconcile_run_cli_prints_reconciliation_summary(monkeypatch, capsys) -> None:
    crawl_run_id = uuid4()

    @contextmanager
    def fake_session_scope():
        yield object()

    class FakeCrawlRunRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeCrawlPartitionRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeVacancySeenEventRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeVacancyCurrentStateRepository:
        def __init__(self, session: object) -> None:
            self.session = session

    class FakeReconciliationPolicy:
        pass

    def fake_reconcile_run(
        command,
        crawl_run_repository,
        crawl_partition_repository,
        vacancy_seen_event_repository,
        vacancy_current_state_repository,
        reconciliation_policy,
    ):
        assert command.crawl_run_id == crawl_run_id
        assert crawl_run_repository.__class__.__name__ == "FakeCrawlRunRepository"
        assert crawl_partition_repository.__class__.__name__ == "FakeCrawlPartitionRepository"
        assert vacancy_seen_event_repository.__class__.__name__ == "FakeVacancySeenEventRepository"
        assert (
            vacancy_current_state_repository.__class__.__name__
            == "FakeVacancyCurrentStateRepository"
        )
        assert reconciliation_policy.__class__.__name__ == "FakeReconciliationPolicy"
        return SimpleNamespace(
            crawl_run_id=crawl_run_id,
            observed_in_run_count=12,
            missing_updated_count=7,
            marked_inactive_count=3,
            run_status="completed",
        )

    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.reconcile.session_scope",
        fake_session_scope,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.reconcile.SqlAlchemyCrawlRunRepository",
        FakeCrawlRunRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.reconcile.SqlAlchemyCrawlPartitionRepository",
        FakeCrawlPartitionRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.reconcile.SqlAlchemyVacancySeenEventRepository",
        FakeVacancySeenEventRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.reconcile.SqlAlchemyVacancyCurrentStateRepository",
        FakeVacancyCurrentStateRepository,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.reconcile.MissingRunsReconciliationPolicyV1",
        FakeReconciliationPolicy,
    )
    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.reconcile.reconcile_run",
        fake_reconcile_run,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "reconcile-run",
            "--run-id",
            str(crawl_run_id),
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "reconciled crawl run" in captured.out
    assert f"run_id={crawl_run_id}" in captured.out
    assert "vacancies_observed_in_run=12" in captured.out
    assert "missing_updated=7" in captured.out
    assert "marked_inactive=3" in captured.out
    assert "status=completed" in captured.out


def test_show_metrics_cli_prints_prometheus_snapshot(monkeypatch, capsys) -> None:
    class FakeMetricsRegistry:
        def render_prometheus(self) -> str:
            return (
                "# HELP hhru_operation_total Total number of application operations.\n"
                "# TYPE hhru_operation_total counter\n"
                'hhru_operation_total{operation="create_crawl_run",status="succeeded"} 1\n'
            )

    monkeypatch.setattr(
        "hhru_platform.interfaces.cli.commands.observability.get_metrics_registry",
        lambda: FakeMetricsRegistry(),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "hhru-platform",
            "show-metrics",
        ],
    )

    exit_code = main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "hhru_operation_total" in captured.out
    assert 'operation="create_crawl_run"' in captured.out
