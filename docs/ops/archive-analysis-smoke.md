# Archive Analysis Smoke

Goal: prove that Research Archive v1 data can be sampled from S3 or a local
archive directory, loaded into a DataFrame, and plotted.

This is not research analysis. The current corpus is `pilot/test corpus`; see
[data-corpus-boundary.md](/home/yurizinyakov/projects/hh_collector/docs/ops/data-corpus-boundary.md).

Preferred execution place: local workstation or a separate analyst machine.
Do not use the collector VPS for notebooks or exploratory analysis unless you
are debugging the archive pipeline itself. The script downloads a bounded sample
directly from S3 into the local checkout and does not need database access.

## Local Setup

Install analysis-only dependencies outside the production image:

```bash
python -m pip install -e '.[analysis]'
```

## S3 Sample

Requires `.env` with either `HHRU_RESEARCH_ARCHIVE_OFFSITE_S3_*` or fallback
`HHRU_BACKUP_OFFSITE_S3_*` settings.

```bash
PYTHONPATH=src python scripts/analysis/research_archive_dataframe_smoke.py \
  --archive-dir .state/analysis/research-archive-s3-sample \
  --output-dir .state/analysis/research-archive-smoke \
  --download-from-s3 \
  --max-manifests 5 \
  --max-rows 5000
```

Outputs:

- `.state/analysis/research-archive-smoke/summary.json`
- `.state/analysis/research-archive-smoke/sample_rows.csv`
- `.state/analysis/research-archive-smoke/rows_by_dataset.csv`
- `.state/analysis/research-archive-smoke/rows_by_dataset.png`

Local smoke result on 2026-06-05:

- selected `5` manifests and loaded `5000` rows from S3;
- datasets: `bronze/raw_api_payload=1`, `silver/api_request_log=1783`,
  `silver/detail_fetch_attempt=3216`;
- generated `summary.json`, `sample_rows.csv`, `rows_by_dataset.csv` and
  `rows_by_dataset.png`;
- confirmed raw vacancy detail text is available in `payload_json.description`
  as HTML. Example record: HH vacancy `132759971`, `Электрик (ПГТ Сириус)`,
  employer `Р-Фарм`, area `Лазаревское`, with a `2257` character cleaned
  description text.

## Local Archive Sample

If archive chunks are already present locally:

```bash
PYTHONPATH=src python scripts/analysis/research_archive_dataframe_smoke.py \
  --archive-dir .state/archive/research-production-v2 \
  --output-dir .state/analysis/research-archive-smoke \
  --max-manifests 5 \
  --max-rows 5000
```

For a no-pandas smoke in CI or a minimal environment:

```bash
PYTHONPATH=src python scripts/analysis/research_archive_dataframe_smoke.py \
  --archive-dir .state/archive/research-production-v2 \
  --output-dir .state/analysis/research-archive-smoke \
  --max-manifests 5 \
  --max-rows 5000 \
  --summary-only
```

## Notebook

Use [research_archive_analysis_smoke.ipynb](/home/yurizinyakov/projects/hh_collector/notebooks/research_archive_analysis_smoke.ipynb)
as the notebook wrapper around the same script. Keep notebooks small and
reproducible; commit code paths in scripts, not only notebook cells.
