# Project Status And Roadmap

Дата среза: 2026-06-01.

Этот документ является короткой точкой входа после перерывов между сессиями. Детальные runbook-и остаются в соседних ops-документах, но текущий статус и следующий порядок работ фиксируются здесь.

## 0. Как Читать Ops Docs

Если нужно быстро восстановить контекст, читать в таком порядке:

1. `project-status-roadmap.md` - текущий статус, порядок работ и ближайшие решения.
2. `backup-restore-drill.md` - operator runbook для backup, S3 offsite и restore drill.
3. `storage-contours.md` - разделение provider snapshot, PostgreSQL backup и research archive.
4. `research-archive-v1.md` - contract для компактного и анализируемого archive layer.
5. `first-detail-backlog.md` - detail backlog и worker semantics.
6. `observability.md` - alerts, metrics, Prometheus/Grafana contour.

Остальные документы являются detail plans или historical context. Если они конфликтуют
с этим roadmap, приоритет у этого файла.

## 1. Где Мы Сейчас

Проект находится после successful VPS `search-only` baseline, S3 backup/offsite
restore validation, полного drain-а pilot `first-detail` backlog и local/S3
smoke research archive v1.

Уже доказано:

- stateful модель сбора работает на масштабе `high six figures` вакансий;
- planner v2 с `area -> time_window` fallback прошёл full VPS `search-only` baseline;
- прежний WSL memory blocker снят;
- VPS baseline `c7e7d8c6-6813-454c-845e-ca44539da1e8` завершён со статусом `succeeded`, `coverage_ratio=1.0000`, `covered_terminal_partitions=16108/16108`;
- baseline дал `865868` unique vacancies, `2309443` seen events, `872201` short snapshots и `129008` raw payload rows;
- один transient HH `502` был восстановлен через targeted resume: `resumed_failed_partitions=1`, итоговых failed partitions `0`;
- `resume-run-v2` умеет переочередить failed terminal search partitions;
- backup/verify/restore-drill path есть;
- post-baseline backup/verify/restore-drill на VPS прошёл успешно; backup `.state/backups/hhru-platform_hhru_platform_20260515T084422Z.dump`, `1596355292` bytes, sha256 `192cd44693f49bbdcf76832a011b1648b0b5ed1ff748a912eb0c324cb27513cf`;
- retention archive и WebDAV offsite sync проверены на Yandex Disk;
- first-detail backlog foundation реализован: backlog selector, drain command, `detail_worker`, terminal `404`, retry cooldown, metrics, alerts, Grafana panels;
- controlled first-detail worker tick прошёл чисто: `24` successful detail snapshots, `1` terminal_404, `0` retryable failures;
- local detail-worker measurement `100` items прошёл clean: `100/100` successful details, active backlog `766389 -> 766289`, DB delta около `2.28 MB`;
- Alertmanager + `alert-webhook` delivery до Telegram проверены на VPS;
- in-run search transport budget добавлен для `run-once-v2` и `resume-run-v2`: transient failed search partitions переочередятся до лимитов `3` consecutive / `5` total;
- backup offsite для DB dumps проверен end-to-end на Timeweb cold S3: 2.2 GiB dump
  загружен частями за ~82s, повторный запуск idempotent (`skipped_backup_count=1`),
  remote size verification прошёл (`verified_object_count=35`), offsite restore drill
  из S3 copy успешно восстановил core schema в отдельную DB.
- first-detail worker bottleneck найден и исправлен: high-cardinality upstream
  metrics по `/vacancies/<hh_id>` раздували file-backed metrics state и тормозили
  service-mode detail drain;
- после normalizing detail upstream endpoint до `/vacancies/{vacancy_id}` VPS
  measurement показал `~4.5k detail requests/hour` на `scale=1` и
  `~8.9k detail requests/hour` на `scale=2`, без retryable failures, expired
  leases и роста metrics file.
- `scale=3`, `batch=100`, `interval=60` validated как catch-up режим:
  sustained throughput около `14k detail requests/hour`, `expired_leases=0`,
  `failed_states=0`, metrics file остаётся килобайтным.
- pilot first-detail backlog полностью drained на VPS: active/all backlog `0`,
  `848056` successful detail snapshots, `17812` terminal_404, workers stopped.
- post-detail-drain DB backup verified локально: dump
  `.state/backups/hhru-platform_hhru_platform_20260528T112018Z.dump`,
  `13232097458` bytes, sha256
  `46d485f21765df90dec9edbdef1362f5bacfd4848008d48e5941c2c5c456de86`,
  `archive_entry_count=134`;
- post-detail-drain backup uploaded и verified в Timeweb cold S3: `198` data
  parts по `67108864` bytes, manifest uploaded, `verified_object_count=199`;
- research archive v1 local smoke прошёл на VPS как `archive_kind=tool_validation`:
  `6000` rows, `13` chunks, `5212503` data bytes, `13/13` manifests verified,
  local archive size около `5.2M`.
- research archive v1 S3 offsite smoke прошёл на VPS для того же
  `tool_validation` bundle: full sync загрузил `13` data objects, `13` manifests
  и inventory; remote verify подтвердил `13/13` manifests и `27` remote objects;
  bounded readback скачал и полностью проверил `2/2` chunks; повторный full sync
  был idempotent: `candidate_manifest_count=0`, `uploaded_manifest_count=0`,
  `skipped_manifest_count=13`, inventory штатно обновлён.
- bounded settled incremental archive smoke прошёл на VPS на отдельном
  `archive_kind=incremental_validation`: за три запуска raw/request-log cursors
  продвинулись `0 -> 71 -> 81 -> 91`, snapshot/seen-event cursors
  `0 -> 1230 -> 1240 -> 1250`; local verify подтвердил `13/13` manifests и
  `120` rows.
- checkpoint-based complete-coverage audit прошёл isolated VPS S3 smoke:
  до offsite sync/verify он fail-closed вернул `status=incomplete`, после загрузки
  `9` manifests и `2` checkpoints и remote verify `21` objects вернул
  `status=complete`, `issue_count=0`; test bundle лежит под отдельным
  `/hhru-platform/research-archive-smoke/checkpoint-20260601T201007Z`.
- read-only `preview-research-archive-housekeeping` прошёл isolated VPS smoke:
  `coverage_status=complete`, raw cap `81`, snapshot cap `1240`, raw candidates
  `20`, snapshot candidates `0`; первый запрос занял `446861 ms`, поэтому перед
  расширением gate добавлена SQL/index optimization и требуется repeat measurement
  после migration `0005_snapshot_payload_ref_idx`.

Текущий статус не равен full production readiness. Корректная формулировка:
full search coverage operationally validated, backup/offsite restore contour
operationally validated, first-detail pilot backlog drained, research archive v1
local export/verify и S3 upload/verify/readback smoke validated на
`tool_validation` bundle, но production archive cadence, production storage
routine и unattended production routine ещё не доказаны.

Важное ограничение текущего корпуса: данные на VPS являются pilot/test corpus,
полученным из не свежего search snapshot и серии operational experiments. Его
можно использовать для проверки throughput, storage growth, backup/restore и
archive tooling, но нельзя считать canonical production dataset. Перед sustained
production collection нужен clean production start или явно отделённый pilot
corpus.

## 2. Что Ещё Не Доказано

- Полный first-detail drain на масштабе свежего production search snapshot.
  Pilot/test backlog already drained; это не заменяет proof на clean production
  corpus.
- Sustained detail throughput/storage growth на production routine, а не только
  на pilot/test corpus.
- Production-quality Telegram alert payloads: текущие alerts доходят, но мало объясняют причину и scope.
- Backup retention и cleanup routine: backup/offsite contour работает, но retention
  apply smoke ещё нужно проверить на реальном безопасном deletion candidate.
- Production research archive routine: S3 mechanics доказаны на
  `tool_validation` bundle, per-chunk verification receipts прошли VPS smoke,
  non-destructive settled incremental export и checkpoint-based complete-coverage
  audit прошли VPS smoke; wiring audit как обязательного gate перед
  archive-before-delete ещё не закрыт.
- Prometheus retention: фактически применён на VPS, volume в пределах configured
  size limit.
- Многодневная unattended stability на VPS.
- Месячный production режим с backup, housekeeping, offsite archive и operator routine.

## 3. Модули По Готовности

| Контур | Статус | Комментарий |
| --- | --- | --- |
| DB schema / migrations | foundation ready | core operational tables есть, миграции и tests покрывают основной путь |
| Search planner v2 | VPS validated | full VPS baseline снял planner completeness blocker |
| Search runtime | validated, partially hardened | search-only baseline successful; HH `502` показал gap в 5xx retry/classification |
| Resume failed search run | foundation ready | умеет продолжать failed terminal search branches |
| Detail same-run budget | ready as bounded contour | не является completeness guarantee |
| First-detail backlog | VPS catch-up validated on pilot corpus | `scale=3`, `batch=100` validated as supervised catch-up mode |
| Detail worker | foundation ready, supervised scale=3 validated | есть one-shot и loop, пока без multi-day unattended proof |
| Backup / restore drill | VPS validated | post-baseline backup, verify и restore-drill прошли |
| DB backup offsite | S3 end-to-end validated | Timeweb cold S3 upload, idempotency, remote size verify, offsite restore drill и post-detail-drain 13GB upload/verify работают |
| Retention archive / offsite sync | partially validated | legacy retention bundle sync работает; long-term research archive S3 contour валидирован отдельно |
| Research archive v1 | S3 smoke validated | local export/verify, S3 sync, remote verify, bounded readback и idempotency прошли на VPS tool-validation bundle; production cadence ещё open |
| Observability | foundation ready | metrics, dashboards, alert rules есть |
| Alert delivery | foundation ready | delivery до Telegram проверен; payloads нужно сделать информативнее |
| VPS deploy | validated | search-only pilot completed on Timeweb VPS |
| Research enrichment | intentionally out of scope | не начинать до стабилизации collection layer |

## 4. Current Execution Plan

Непосредственный порядок работ после S3 offsite restore drill:

1. Pilot first-detail drain закрыт:
   - active/all backlog `0`;
   - workers stopped;
   - использовать результат как throughput/storage evidence, а не как production
     dataset;
   - для свежего production backlog режим `scale=3`, `batch=100`,
     `interval=60` можно считать supervised catch-up candidate, но не постоянным
     steady mode без search interference proof.
2. Закрыть backup contour hygiene:
   - факт successful VPS offsite restore drill записан;
   - restore-drill DB уже отсутствует на VPS check 2026-05-26;
   - local dump retention уже реализован и на VPS настроен на `14` дней;
   - S3/offsite backup upload/verify/restore-drill проверены;
   - S3/offsite policy decision: automate bounded backup generations, not
     infinite DB dump storage;
   - S3 backup retention delete and sidecar cleanup реализованы как отдельная
     dry-run-first команда `cleanup-backup-offsite`;
   - VPS dry-run прошёл на real S3 state: milestone retained, older unverified
     generation skipped fail-safe, deletion candidates `0`;
   - next: bounded apply smoke только после появления реального безопасного
     deletion candidate.
3. Prometheus retention считается закрытым на 2026-05-26:
   - running flags: `7d` и `8GB`;
   - `hh_collector_prometheus_data`: `5.5G`;
   - volume reset не требуется.
4. Спроектировать research archive v1:
   - contract записан в `research-archive-v1.md`;
   - raw payload archive как canonical `jsonl.gz`;
   - silver/index datasets для анализа без full raw JSON scans;
   - Parquet только как later analytical layer;
   - S3 layout, manifests, inventory и readback safety before delete.
5. Реализовать archive foundation, без research analytics:
   - local export, manifest, inventory реализованы в коде;
   - local validation реализована через `verify-research-archive`;
   - small VPS smoke на pilot corpus с `--limit-per-dataset` прошёл;
   - S3 upload/verify/readback tooling реализован для research archive bundles;
   - VPS S3 smoke на `tool_validation` bundle прошёл: `13/13` manifests,
     `27` objects, `2/2` bounded readbacks, повторный sync idempotent;
   - incremental settled watermark advancement проверен на VPS;
   - checkpoint-based complete coverage audit реализован как non-destructive
     fail-closed report;
   - isolated VPS S3 coverage smoke прошёл: до offsite verify audit был
     `incomplete`, после verify стал `complete` с `issue_count=0`;
   - non-destructive `preview-research-archive-housekeeping` добавлен как первый
     bridge от verified coverage cursors к age-based raw/snapshot candidates;
   - первый isolated VPS preview подтвердил safety semantics, но занял
     `446861 ms`; SQL/index optimization добавлена;
   - next: применить migration `0005_snapshot_payload_ref_idx`, повторить VPS
     preview measurement, затем расширить gate на cascade-sensitive targets до
     первого destructive apply;
   - не делать text features, AI exposure, panels, econometrics или Parquet в
     первом implementation slice.
6. Проверить search interference:
   - controlled search with detail-worker off/on;
   - подтвердить, что detail catch-up не деградирует search coverage и runtime.
7. Зафиксировать clean production start procedure:
   - решить, что делать с pilot/test corpus: оставить как archived evidence,
     снести live DB после verified backup/offsite, или поднять новую production DB;
   - destructive cleanup допустим только после backup, verify, offsite upload,
     offsite verify и restore drill;
   - новый production run должен стартовать из явно чистого состояния или с
     явно помеченной историей, чтобы не смешивать pilot и production metrics/data.
8. После этого переходить к supervised multi-day unattended routine:
   - 3-7 дней без ручного вмешательства;
   - должны штатно работать search/detail/backup/offsite/alerts/retention;
   - контролировать failed states, expired leases, stale scheduler, disk,
     Prometheus volume, logs, metrics file и throughput degradation.
9. После successful multi-day proof запускать sustained production collection
   с нуля: регулярный search, параллельный detail catch-up, backups, offsite,
   alerts, retention и operator runbook.

## 5. Detailed Roadmap

### Stage 1. VPS first-detail measurement

1. Запустить bounded `first-detail` drain на VPS после successful search baseline.
2. Снять detail throughput, retryable failure mix, terminal_404 долю и DB growth.
3. Зафиксировать estimate для cold-start drain и steady-state weekly operation.

Безопасный первый measurement:

```bash
LIMIT=100 make vps-first-detail-measurement
```

Первый measurement 2026-04-28 уже прошёл clean:

- `100/100` successful details;
- `0` terminal_404;
- `0` retryable failures;
- active backlog `766389 -> 766289`;
- DB delta `2277376 bytes`, примерно `22.8 KB` на selected item;
- artifact: `.state/reports/detail-worker-measurement/20260428T111507Z/summary.md`.

Go/no-go:

- retryable failures не растут неконтролируемо;
- cooldown backlog не накапливается неожиданно;
- storage growth попадает в ожидаемый порядок;
- worker можно безопасно остановить и продолжить.

### Stage 2. Alert payload hardening

1. Добавить в Telegram message run id, failed step, error type/message, coverage, pending/failed partitions.
2. Для grouped alerts показывать distinct labels и top failing operations, а не только `alerts: N`.
3. Для backup/offsite alerts показывать последний artifact и operator command.
4. Документировать operator action для scheduler stale, failed partitions, backup stale, first-detail failures.

Status на 2026-05-15:

- Alertmanager route добавлен.
- `alert-webhook` receiver добавлен.
- Telegram delivery на VPS проверен.
- Payloads слишком общие: по `HHRUPlatformOperationFailures` и `HHRUPlatformFailedPartitionsPresent` без CLI/log context тяжело понять root cause.

Go/no-go:

- alerts реально доходят вне Grafana UI;
- alert message даёт run id / scope / следующий operator action.

### Stage 3. Transport / resume hardening

1. Классифицировать HH `5xx` search responses как retryable transport failures.
2. Уточнить terminal status mapping для `completed_with_unresolved` и `completed_with_detail_errors`.
3. Не превращать transient DNS/network outage в потерю почти готового baseline.

Status на 2026-05-15:

- `run-once-v2` переочередит failed search partitions только для transport failures, пока не достигнет `3` consecutive или `5` total failures.
- `resume-run-v2` использует тот же budget при повторном прохождении failed/unresolved branches.
- CLI summary показывает `search_transport_failures_total` и `search_captcha_failures_total`.
- На VPS единичный HH `502` прошёл как `VacancySearchNormalizationError` / `bad_gateway`, а `search_transport_failures_total` остался `0`; это нужно исправить, чтобы 5xx не требовали ручного resume.

Go/no-go:

- единичный transport leaf failure не требует full rerun;
- operator summary объясняет, что именно произошло;
- targeted resume path остаётся штатным.

### Stage 4. Backup offsite gap

1. Команда `sync-backup-offsite` / `make backup-offsite` добавлена для `.state/backups/*.dump`.
2. Проверить upload свежего post-baseline DB backup в offsite storage.
3. Зафиксировать retention policy: сколько backup dumps держим локально и offsite.
4. Remote verification добавлен: manifest exists, all parts exist, remote sizes match.
5. Offsite restore drill command добавлен: download parts from S3, assemble dump,
   verify sha256, restore into separate DB.

Наблюдение 2026-05-15: `export-retention-archive` и `sync-retention-archive-offsite` успешно отработали, но `candidate_bundle_count=0`. Это проверяет retention archive path, а не offsite-копию свежего DB backup.

Наблюдение 2026-05-20: одиночный WebDAV `PUT` свежего DB dump-а размером 2.2 GiB
упирался в timeout/зависание. `sync-backup-offsite` переведён на загрузку dump-а
fixed-size частями с manifest v2 и receipt, который учитывает `chunk_size_bytes` и
`part_count`.

Наблюдение 2026-05-23: Timeweb cold S3 снял WebDAV blocker. Dump `2269000643`
bytes загружен в `34` parts по `67108864` bytes примерно за `82s`. Повторный запуск
не грузил данные заново: `uploaded_backup_count=0`, `skipped_backup_count=1`.
Remote verify прошёл: `verified_object_count=35`. Offsite restore drill из S3 copy
прошёл: все `34` parts скачались, assembled dump прошёл checksum, restore drill
incremented `hhru_restore_drill_run_total{status="succeeded"}`, core tables в
`hhru_platform_restore_drill` проверены.

### Stage 5. VPS supervised detail drain

1. Включить bounded first-detail drain отдельно от baseline.
2. Расширять batch/interval только после измерения storage и failure mix.
3. First-detail claim/lease реализован как prerequisite для нескольких worker-ов:
   - короткая transaction выбирает candidates через row lock / `SKIP LOCKED`;
   - выбранные rows помечаются `running`, `first_detail_lease_owner` и
     `first_detail_lease_expires_at`;
   - fetch идёт уже после commit, чтобы не держать locks на весь network batch;
   - lease timeout возвращает crashed/aborted rows в ready backlog.
4. Следующий шаг: controlled 2-worker measurement на VPS после migration.
5. Считать first-detail backlog trend.

VPS observation 2026-05-23:

- `batch=500`, `interval=60` стабилен по error mix: retryable failures не растут,
  terminal_404 около `1-2%`;
- sustained duration после нескольких часов около `950-1130s` на batch `500`,
  то есть примерно `1.6k-2k selected/hour`;
- restart worker-а не сбросил duration, поэтому это больше похоже на sustained
  upstream/time-of-day latency, чем на локальный leak;
- claim/lease снимает known duplicate-selection blocker, но длительная
  параллельность ещё должна быть проверена controlled 2-worker run-ом.

VPS observation 2026-05-24:

- controlled 2-worker run после claim/lease прошёл safety checks:
  `expired_leases=0`, `failed_states=0`, duplicate selection не наблюдался;
- throughput gain от `scale=2` не подтвердился: observed drain остался около
  `800-900/hour`, то есть не лучше single-worker baseline;
- в client code нет явного общего detail rate limiter: `HHApiClient` использует
  sync `urlopen`, а `fetch_vacancy_detail` делает `5s` sleep только после
  transport failure retry;
- текущая leading hypothesis: HH/upstream/IP/auth/network path имеет общий
  sustained budget, который несколько worker-ов делят между собой.
- этот вывод был superseded 2026-05-25: основным bottleneck оказался
  high-cardinality metrics state, а не внешний upstream budget.

VPS observation 2026-05-25:

- `detail-worker --once --batch-size 100` under profiler completed `100` details
  in about `15s`, so the active fetch path is not inherently limited to
  `~600-1300/hour`;
- sustained service-mode still showed lower throughput (`~1260/hour`) and growing
  gaps, while profile exposed significant `metrics._mutating_state` overhead;
- root cause candidate tightened to high-cardinality upstream metrics:
  detail requests were recorded under real `/vacancies/<hh_id>` endpoints, causing
  the file-backed metrics state to grow with every vacancy. The fix is to collapse
  detail upstream metrics to `/vacancies/{vacancy_id}` and compact existing state
  on read/write.
- after deploying the metrics fix, controlled `scale=1`, `batch=100`,
  `interval=60` measurement showed `2200` requests in about `29m`,
  `4545.1 requests/hour`, `latency_p50=126ms`, `latency_p95=349ms`,
  `gap_p50=0.157s`, `gap_p95=0.418s`, metrics file about `3.1K`;
- controlled `scale=2`, same batch/interval, showed `4434` requests in about
  `30m`, `8875.3 requests/hour`, `latency_p50=125ms`, `latency_p95=319ms`,
  `gap_p50=0.108s`, `gap_p95=0.254s`;
- safety checks after the `scale=2` measurement: `expired_leases=0`,
  `failed_states=0`, metrics file about `3.2K`;
- preliminary production catch-up mode is favorable at `scale=2`; with backlog
  around `674k`, expected cold backlog drain time is roughly `3.1-3.3 days`
  if the rate holds.

VPS observation 2026-05-26:

- controlled `scale=3`, `batch=100`, `interval=60` night run showed `171500`
  requests from `2026-05-25T20:06:36Z` to `2026-05-26T08:01:21Z`,
  `14396.6 requests/hour`, `latency_p50=96ms`, `latency_p95=180ms`,
  `gap_p50=0.111s`, `gap_p95=0.209s`;
- safety checks: `expired_leases=0`, `failed_states=0`, active cooldown backlog
  `0`, metrics file about `4.1K`;
- later live control still held: last `30m` throughput `14014.2 requests/hour`,
  `latency_p50=96ms`, `latency_p95=235ms`, `expired_leases=0`,
  `failed_states=0`, metrics file about `4.1K`;
- `scale=3` is validated as pilot catch-up mode. It should not automatically
  become steady mode after pilot backlog is drained; steady mode should be
  right-sized after measuring fresh production search + detail interference.
- current corpus is pilot/test data. It is useful evidence for system behavior,
  but production collection should start from a clean or explicitly separated
  state.

VPS observation 2026-05-28:

- pilot first-detail backlog fully drained: `active_backlog_size=0`,
  `active_ready_backlog_size=0`, `active_cooldown_backlog_size=0`;
- final pilot detail counts: `848056` succeeded details, `17812` terminal_404,
  `16` failed attempts in attempt history, no current failed backlog;
- detail workers were stopped after drain; cleanup query updated `0` active
  running leases;
- post-drain DB size was about `29.1GB`;
- post-drain local backup verified: `13232097458` bytes, sha256
  `46d485f21765df90dec9edbdef1362f5bacfd4848008d48e5941c2c5c456de86`;
- post-drain backup offsite upload/verify succeeded in Timeweb cold S3:
  `198` data parts, `verified_object_count=199`;
- Archive v1 local smoke passed as `archive_kind=tool_validation`: `6000` rows,
  `13` chunks, `5212503` data bytes, `13/13` manifests verified.

VPS observation 2026-05-31:

- Archive v1 S3 offsite sync passed for the `tool_validation` bundle:
  `uploaded_manifest_count=13`, inventory uploaded;
- remote verify passed: `verified_manifest_count=13`,
  `verified_object_count=27`;
- bounded readback passed for `2/2` selected chunks with size, sha256, gzip JSONL
  parse and row-count checks;
- repeated full sync was idempotent: `candidate_manifest_count=0`,
  `uploaded_manifest_count=0`, `skipped_manifest_count=13`; inventory refresh on
  full sync remained enabled by design.
- S3 DB backup retention dry-run passed against real state:
  post-detail-drain milestone dump verified with `198` parts and
  `verified_object_count=199`, `.offsite.verified.json` created, milestone marker
  applied; cleanup scanned `2` upload receipts, retained the milestone dump,
  skipped the older unverified dump fail-safe and produced `0` deletion
  candidates.

Next experiment plan:

1. Не делить backlog на lanes/run/priority до необходимости production telemetry.
2. Прогнать bounded apply smoke для S3 backup retention delete and sidecar cleanup
   только когда появится реальный безопасный deletion candidate.
3. Применить migration `0005_snapshot_payload_ref_idx`, повторить VPS measurement
   `preview-research-archive-housekeeping` на isolated verified bundle, затем
   расширить coverage gate на cascade-sensitive targets до первого destructive
   apply.
4. Зафиксировать clean production start procedure и решение по pilot/test corpus.
5. Проверить search interference: detail-worker on/off during controlled search
   on fresh production routine.

Go/no-go:

- first-detail backlog убывает быстрее, чем растёт;
- search baseline не деградирует из-за detail drain;
- alert delivery уже включена.
- parallelism не создаёт duplicate detail fetches for the same selected rows.

### Stage 6. Week / Month unattended

1. Провести supervised week.
2. Проверить backup, housekeeping, offsite sync, alerts, disk growth.
3. Только после этого переходить к месячному unattended окну.

## 6. Практический Вывод

Проект уже прошёл главный feasibility risk: собрать near-snapshot hh.ru search-space в рамках одного длинного sweep принципиально возможно.

Следующий риск уже не архитектурный, а операционный: устойчивость к внешним outage, понятная recovery semantics, storage planning, alert delivery и proof, что first-detail backlog можно дренировать без деградации search contour.

Связанные документы:

- [current-readiness.md](/home/yurizinyakov/projects/hh_collector/docs/ops/current-readiness.md)
- [vps-pilot-checklist.md](/home/yurizinyakov/projects/hh_collector/docs/ops/vps-pilot-checklist.md)
- [first-detail-backlog.md](/home/yurizinyakov/projects/hh_collector/docs/ops/first-detail-backlog.md)
- [research-archive-v1.md](/home/yurizinyakov/projects/hh_collector/docs/ops/research-archive-v1.md)
- [hh-api-completeness-implementation-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-completeness-implementation-plan.md)
- [testing-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/testing-plan.md)
