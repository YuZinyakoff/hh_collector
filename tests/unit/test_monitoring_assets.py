from __future__ import annotations

import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_grafana_dashboards_are_valid_json_assets() -> None:
    dashboard_paths = (
        REPO_ROOT / 'monitoring' / 'grafana' / 'dashboards' / 'collector-overview.json',
        REPO_ROOT / 'monitoring' / 'grafana' / 'dashboards' / 'hh-api-ingest-health.json',
    )

    for dashboard_path in dashboard_paths:
        payload = json.loads(dashboard_path.read_text(encoding='utf-8'))

        assert isinstance(payload['title'], str)
        assert payload['title']
        assert isinstance(payload['panels'], list)
        assert payload['panels']
        assert payload['uid']


def test_grafana_provisioning_points_to_prometheus_and_repo_dashboards() -> None:
    datasource_config = (
        REPO_ROOT
        / 'monitoring'
        / 'grafana'
        / 'provisioning'
        / 'datasources'
        / 'prometheus.yml'
    ).read_text(encoding='utf-8')
    dashboards_config = (
        REPO_ROOT
        / 'monitoring'
        / 'grafana'
        / 'provisioning'
        / 'dashboards'
        / 'dashboards.yml'
    ).read_text(encoding='utf-8')

    assert 'url: http://prometheus:9090' in datasource_config
    assert 'uid: prometheus' in datasource_config
    assert 'path: /var/lib/grafana/dashboards' in dashboards_config
