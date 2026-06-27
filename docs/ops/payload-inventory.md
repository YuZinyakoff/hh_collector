# Payload Inventory

This is a read-only operator report for deciding live DB retention policy.

Run it before changing TTLs for `raw_api_payload`, `vacancy_snapshot` or related
history tables:

```bash
cd /opt/hh_collector
make payload-inventory \
  ARGS="--repo-root /opt/hh_collector" \
  | tee /tmp/hhru-payload-inventory-$(date -u +%Y%m%dT%H%M%SZ).log
```

For a faster bounded scan:

```bash
make payload-inventory \
  ARGS="--repo-root /opt/hh_collector --sample-rows 20000 --top-limit 60"
```

What it reports:

- full counts and JSON sizes by `api_request_log.request_type` and
  `raw_api_payload.endpoint_type`;
- sampled top-level keys for raw vacancy detail payloads;
- sampled top-level keys for raw search response pages and search result items;
- sampled top-level keys for detail and short snapshot payloads;
- snapshot churn by `snapshot_type` and `change_reason`;
- whether snapshots carry embedded schema-v2 payloads or depend only on raw
  payload references;
- live `vacancy` and `vacancy_current_state` field coverage.

The expensive JSON key scans are intentionally bounded by recent row samples.
The report is for retention design and anomaly detection; it is not a destructive
housekeeping command and does not authorize deletion by itself.
