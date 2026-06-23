# Project Status And Roadmap

Дата среза: 2026-06-23.

Этот документ является короткой точкой входа после перерывов между сессиями. Детальные runbook-и остаются в соседних ops-документах, но текущий статус и следующий порядок работ фиксируются здесь.

## 0. Как Читать Ops Docs

Если нужно быстро восстановить контекст, читать в таком порядке:

1. `project-status-roadmap.md` - текущий статус, порядок работ и ближайшие решения.
2. `backup-restore-drill.md` - operator runbook для backup, S3 offsite и restore drill.
3. `storage-contours.md` - разделение provider snapshot, PostgreSQL backup и research archive.
4. `data-corpus-boundary.md` - разделение текущего pilot/test corpus и будущего
   production analytical corpus.
5. `archive-analysis-smoke.md` - минимальный DataFrame/plot smoke для archive chunks.
6. `research-archive-v1.md` - contract для компактного и анализируемого archive layer.
7. `first-detail-backlog.md` - detail backlog и worker semantics.
8. `observability.md` - alerts, metrics, Prometheus/Grafana contour.
9. `current-state-2026-06-23.md` - датированный storage/corpus snapshot.
10. `collection-recovery-2026-06-23.md` - текущий runbook восстановления
    search/detail collection после обнаруженного простоя.

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
  `20`, snapshot candidates `0`; первый запрос занял `446861 ms`. После migration
  `0005_snapshot_payload_ref_idx` и SQL optimization повторный preview занял
  `159 ms` (`2.897s` wall time вместе с Docker startup), raw и snapshot targets
  штатно вернули по `20` кандидатов.
- cascade-sensitive run-tree preview прошёл isolated VPS smoke: при verified
  seen-event cursor `1240` единственный старый run был fail-closed исключён из
  action list: `candidate_count=1`, `coverage_blocked_candidate_count=1`,
  `coverage_safe_candidate_count=0`, `action_count=0`; preview занял `131 ms`.
- canonical production research archive bootstrap завершён в
  `.state/archive/research-production-v2`: zero-row checkpoint подтвердил полный
  catch-up, local verify прошёл для `1557/1557` manifests, `6885371` rows и
  `7508484645` data bytes;
- canonical S3 sync и remote verify завершены под
  `/hhru-platform/research-archive`: uploaded `1557` manifests и `27`
  checkpoints, verified `1557` manifests / `27` checkpoints / `3142` objects,
  bounded readback `2/2` прошёл;
- canonical coverage audit вернул `status=complete`, `issue_count=0` для всех
  пяти incremental datasets; production default-path housekeeping preview
  вернул `status=ready`, `total_candidates=0`, `total_action_count=0`;
- canonical archive directory закреплён на VPS через
  `HHRU_RESEARCH_ARCHIVE_DIR=.state/archive/research-production-v2`;
- supervised `daily-research-archive` driver smoke 2026-06-04 прошёл полностью:
  zero-row export, local verify, idempotent offsite sync, offsite verify,
  coverage audit и read-only housekeeping preview завершились успешно; audit
  подтвердил `28/28` verified production checkpoints.
- `hhru-research-archive.timer` включён на VPS 2026-06-04; первый unattended
  запуск 2026-06-05 завершился успешно: zero-row export, local verify, offsite
  sync, offsite verify, coverage audit и housekeeping preview all succeeded.
  Local verify подтвердил `1557/1557` manifests, offsite verify подтвердил
  `1557/1557` manifests и `29` checkpoints, housekeeping preview вернул
  `coverage_issue_count=0`, `total_candidates=0`, `total_action_count=0`.
- fail-closed daily backup systemd driver прошёл supervised VPS smoke
  2026-06-04: unit завершился с `Result=success`, `ExecMainStatus=0` после
  create, local verify, offsite sync и offsite verify.
- первый unattended daily backup timer run прошёл 2026-06-06: unit завершился с
  `Result=success`, `ExecMainStatus=0`; create, local verify, offsite sync и
  offsite verify all succeeded; backup file
  `.state/backups/hhru-platform_hhru_platform_20260606T003301Z.dump`.
- следующий checked daily backup timer run прошёл 2026-06-08: unit завершился с
  `Result=success`, `ExecMainStatus=0`; backup
  `.state/backups/hhru-platform_hhru_platform_20260608T003156Z.dump` был
  uploaded to S3 and verified as `198` parts + manifest (`199` objects).
- weekly offsite restore drill systemd driver прошёл supervised VPS smoke
  2026-06-04: unit восстановил latest offsite-verified backup во временную DB,
  завершился с `Result=success`, `ExecMainStatus=0` и cleanup step passed.
- первый unattended weekly offsite restore drill timer run прошёл 2026-06-07:
  unit завершился с `Result=success`, `ExecMainStatus=0`; backup
  `.state/backups/hhru-platform_hhru_platform_20260607T004343Z.dump` был
  восстановлен из offsite в temporary DB, cleanup step succeeded, lingering
  restore DB отсутствует.
- checked daily research archive timer run 2026-06-08 прошёл успешно: zero-row
  export, local verify `1557/1557` manifests, idempotent offsite sync
  `uploaded_manifest_count=0`, offsite verify `1557/1557` manifests / `32`
  checkpoints / `3147` objects, coverage `complete`, housekeeping preview
  `total_action_count=0`.
- `3-7`-дневный unattended storage/archive soak прошёл без ручного вмешательства:
  на 2026-06-15 failed units `0`, running app-run containers `0`, daily backup
  runs 2026-06-11..2026-06-15 all succeeded with offsite verify
  `verified_object_count=199`, daily archive runs 2026-06-11..2026-06-15 all
  succeeded with local verify `1557/1557`, offsite verify `1557/1557`, coverage
  `complete` and housekeeping preview `total_action_count=0`; checkpoint cursor
  advanced to `39` verified checkpoints.
- second unattended weekly offsite restore drill passed on 2026-06-14: backup
  `.state/backups/hhru-platform_hhru_platform_20260614T004152Z.dump` was
  restored from S3, `198/198` parts downloaded, `schema_verified=yes`,
  `verified_tables=5/5`, cleanup step succeeded, lingering restore DB absent.
- storage after soak remained stable enough for current retention window:
  filesystem `88G/154G` used (`58%`), `.state/backups=37G`,
  `.state/archive/research-production-v2=7.1G`, `.state/logs=30M`.
- bounded S3 backup retention apply passed on 2026-06-15 after dry-run:
  cleanup scanned `13` receipts, deleted `7` verified generations,
  `1393` remote objects and `28` local sidecars, retained latest `3`, weekly
  checkpoint `20260607` and protected milestone `20260528`; follow-up dry-run
  returned `delete_candidate_count=0`, `retained_generation_count=5`,
  `skipped_generation_count=1` for the old unverified `20260517` generation.
- guarded weekly S3 backup cleanup timer passed on 2026-06-21 after a successful
  weekly offsite restore drill marker: cleanup scanned `12` receipts, deleted
  `5` verified generations, `995` remote objects and `20` local sidecars,
  retained latest `3`, weekly checkpoints and protected milestone; old
  unverified `20260517` remained fail-safe skipped.
- storage/corpus snapshot on 2026-06-23 is fixed in
  `current-state-2026-06-23.md`: host disk `101G/154G` used, `.state=63G`,
  `.state/backups=50G`, PostgreSQL DB `27 GB`, uploaded backup footprint
  `100.70 GiB`, verified backup footprint `98.59 GiB`, research archive
  `1557/1557` manifests and `6885371` rows, live DB `865868` vacancies with
  `848056` successful detail snapshots.
- non-blocking local failure delivery прошёл VPS smoke 2026-06-04: direct
  notifier и systemd template вернули success за `22-44 ms`; Telegram egress
  outage больше не блокирует local monitoring path.
- local archive analysis smoke прошёл 2026-06-05: из S3 sample загружено
  `5000` rows из `5` manifests в DataFrame-ready CSV, построен
  `rows_by_dataset.png`, подтверждено наличие полного vacancy detail text в
  `payload_json.description`.

Текущий статус не равен full unattended production readiness. Корректная
формулировка: full search coverage, backup/offsite restore contour, first-detail
pilot drain и canonical production research archive operationally validated.
Daily research archive pipeline validated как supervised non-destructive routine
и multi-day unattended timer routine. Daily backup timer и weekly restore drill
timer validated как unattended storage routine. Weekly S3 backup cleanup timer
validated after successful restore drill. Общий production search/detail routine
не просто "не доказан": проверка 2026-06-23 показала, что collection не
запущен. `scheduler`/`detail-worker` containers отсутствуют, единственный
`crawl_run` - майский `vps-search-baseline`.

Важное ограничение текущего корпуса: данные на VPS являются pilot/test corpus,
полученным из не свежего search snapshot и серии operational experiments. Его
можно использовать для проверки throughput, storage growth, backup/restore,
archive tooling и DataFrame-readability smoke, но нельзя считать canonical
production analytical dataset. Boundary зафиксирован в `data-corpus-boundary.md`:
перед sustained production collection нужен `corpus_id` / `collection_epoch` или
clean production start. На snapshot `2026-06-23` post-boundary production corpus
с границей `2026-06-01T00:00:00+00:00` равен `0`, потому что новый collection
после мая не выполнялся.

## 2. Что Ещё Не Доказано

- Полный first-detail drain на масштабе свежего production search snapshot.
  Pilot/test backlog already drained; это не заменяет proof на clean production
  corpus.
- Sustained detail throughput/storage growth на production routine, а не только
  на pilot/test corpus.
- Production-quality Telegram alert payloads: текущие alerts доходят, но мало объясняют причину и scope.
- Старый unverified backup generation `20260517` остаётся fail-safe skipped:
  cleanup не удаляет generation без matching successful verification receipt.
- Совместная работа production search/detail с daily archive/backup routine.
  Storage/archive timers прошли multi-day unattended soak, но production
  collection cadence ещё не включалась как часть общего режима.
- Fresh production collection itself: no scheduler/detail-worker runtime was
  active on 2026-06-23, and no crawl runs exist after the May baseline.
  Supervised recovery run `bcf9ef54-27b0-4a90-bd33-728775053ea4` did start
  writing fresh rows on 2026-06-23, but finished as `failed`; diagnose that run
  before starting detail catch-up or background scheduler.
- Generic automatic alert/failure signal для host-side storage services
  прошёл non-blocking local acceptance smoke на VPS. Direct Telegram egress с
  VPS недоступен; внешний proxy/route остаётся отдельным optional transport.
- Safe destructive research housekeeping на реальном production candidate.
  Guarded apply path реализован, но текущий production preview возвращает
  `0` actions, поэтому deletion proof намеренно ещё не выполнялся.
- Регулярный PostgreSQL backup/offsite cadence: daily backup, weekly offsite
  restore drill and guarded weekly S3 cleanup are VPS validated. Следующий
  вопрос здесь не proof механики, а monitoring следующего weekly cycle и решение
  по старому unverified `20260517`.
- Явная production cadence для search collection. Текущий `scheduler-loop`
  является interval-based trigger loop, а не календарной weekly policy, поэтому
  его нельзя считать готовым многомесячным расписанием без отдельного решения.
- Prometheus retention: фактически применён на VPS, volume в пределах configured
  size limit.
- Месячный production режим с backup, housekeeping, offsite archive и operator
  routine.

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
| Backup / restore drill | VPS unattended soak validated | post-baseline backup, verify и restore-drill прошли; daily/weekly systemd drivers прошли supervised smoke; daily backups and weekly restore drills passed unattended soak |
| DB backup offsite | S3 end-to-end validated | Timeweb cold S3 upload, idempotency, remote size verify, offsite restore drill и post-detail-drain 13GB upload/verify работают |
| S3 backup cleanup | VPS validated guarded timer | manual dry-run/apply validated; weekly timer run 2026-06-21 succeeded after recent restore success marker and explicit apply env |
| Retention archive / offsite sync | partially validated | legacy retention bundle sync работает; long-term research archive S3 contour валидирован отдельно |
| Research archive v1 | production bootstrap and multi-day unattended routine validated | canonical local/S3 archive complete; timer enabled; daily archive soak succeeded with complete coverage and zero housekeeping actions |
| Observability | foundation ready | metrics, dashboards, alert rules есть |
| Alert delivery | foundation ready | delivery до Telegram проверен; payloads нужно сделать информативнее |
| VPS deploy | validated | search-only pilot completed on Timeweb VPS |
| Research enrichment | intentionally out of scope | не начинать до стабилизации collection layer |

### 3.1. Насколько Близко До "Включил И Забыл"

Текущий practical status: storage/archive foundation завершён, прошёл
multi-week unattended soak и первый guarded weekly cleanup apply. Вся платформа
ещё не является full unattended production, потому что search/detail production
cadence не закрыта и не проверена вместе с heavy storage/archive jobs.

Для перехода к редкому operator monitoring нужны закрытые gate:

1. Run a supervised fresh production search collection and verify new
   post-boundary rows before calling the project healthy.
2. Зафиксировать реальную production cadence search/detail и проверить её
   совместную работу с archive/backup по CPU, RAM, IO, disk growth и HH failure
   mix. Не использовать текущий hourly scheduler default как production policy
   без отдельного решения.
3. Провести месячное окно с редким monitoring, alert-driven checks и weekly
   operator checklist.

После этих gates корректно будет говорить "включил и мониторю по alerts и
еженедельному checklist". До них запуск на месяцы без наблюдения преждевременен.

## 4. Current Execution Plan

Непосредственный порядок работ после storage timers enablement:

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
   - local dump retention уже реализован; daily backup driver uses a short
     local window because one current dump is about `13 GB`;
   - S3/offsite backup upload/verify/restore-drill проверены;
   - S3/offsite policy decision: automate bounded backup generations, not
     infinite DB dump storage;
   - S3 backup retention delete and sidecar cleanup реализованы как отдельная
     dry-run-first команда `cleanup-backup-offsite`;
   - VPS dry-run прошёл на real S3 state: milestone retained, older unverified
     generation skipped fail-safe, deletion candidates `0`;
   - bounded apply на реальном безопасном candidate прошёл 2026-06-15:
     `deleted_generation_count=7`, `remote_deleted_object_count=1393`,
     follow-up dry-run `delete_candidate_count=0`;
   - guarded weekly systemd cleanup path proved on 2026-06-21: explicit
     `HHRU_BACKUP_OFFSITE_CLEANUP_APPLY=true`, recent restore-drill success
     marker required, `deleted_generation_count=5`,
     `remote_deleted_object_count=995`;
   - next: monitor the next weekly cleanup cycle and decide whether to manually
     handle the old skipped unverified `20260517` generation.
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
   - первый isolated VPS preview занял `446861 ms`; после migration
     `0005_snapshot_payload_ref_idx` и SQL/index optimization повторный preview
     занял `159 ms`;
   - read-only preview расширен на cascade-sensitive finished-run tree:
     завершенные runs с `vacancy_seen_event.id` выше verified cursor считаются
     blocked и не попадают в action list;
   - isolated VPS run-tree preview подтвердил fail-closed блокировку единственного
     старого run с не покрытым seen event;
   - `silver/detail_fetch_attempt` добавлен в incremental checkpoint chain и
     bounded preview;
   - fresh isolated five-dataset VPS smoke прошёл: `11/11` manifests,
     `2` checkpoints, `25` remote objects, `issue_count=0`;
   - отдельный guarded apply path требует `archive_kind=production`, явный
     `--apply`, canonical `/hhru-platform/research-archive` root, повторный
     verified preview внутри транзакции и lock/recheck выбранных run-tree корней;
   - VPS guard-smoke прошёл: запуск без `--apply` завершился fail-closed до
     coverage audit и удаления;
   - первый canonical production bootstrap был прерван Linux OOM-killer при
     `14.7 GiB` RSS экспортера до записи checkpoint; cursor recovery переведён
     с orphan manifests на completed checkpoints, а writer получил `32 MiB`
     byte-buffer ceiling;
   - next: повторить canonical bootstrap bounded checkpoint-батчами в свежем
     локальном archive directory, не синхронизируя orphan bundle;
   - canonical `archive_kind=production` bootstrap завершён: `27` checkpoints,
     local verify `1557/1557`, remote verify `1557` manifests / `27`
     checkpoints / `3142` objects, coverage `complete`, default-path preview
     `ready` с `0` actions;
   - host-side daily archive driver и systemd timer добавлены для
     non-overlapping export/verify/sync/audit/preview cadence; destructive apply
     в automation отсутствует;
   - supervised daily-driver smoke завершён 2026-06-04: все шесть steps
     succeeded, production coverage остался complete;
   - systemd timer включён 2026-06-04; первый unattended daily run прошёл
     2026-06-05; multi-day unattended archive soak passed by 2026-06-15;
   - shared heavy-ops lock и generic systemd failure notifier добавлены вместе с
     daily backup и weekly offsite restore drill drivers/timers;
   - synthetic failure smoke, supervised backup/restore-driver smoke, first
     unattended backup/restore timer runs и `3-7` day storage soak прошли;
   - pre-delete backup/restore drill и первый guarded destructive apply выполнять
     только после появления реального retention candidate;
   - не делать text features, AI exposure, panels, econometrics или Parquet в
     первом implementation slice.
6. Немедленно восстановить collection:
   - диагностировать failed supervised search run
     `bcf9ef54-27b0-4a90-bd33-728775053ea4`;
   - если он resumable, продолжить его через `resume-run-v2` с
     `--detail-limit 0`;
   - если он не resumable, сначала устранить blocker, затем запускать новый
     supervised search-only `run-once-v2` с production marker;
   - проверить появление свежих post-boundary timestamps и coverage report;
   - только после этого запускать detail smoke/catch-up;
   - полный порядок зафиксирован в
     [collection-recovery-2026-06-23.md](/home/yurizinyakov/projects/hh_collector/docs/ops/collection-recovery-2026-06-23.md).
7. Проверить search interference:
   - controlled search with detail-worker off/on;
   - подтвердить, что detail catch-up не деградирует search coverage и runtime.
8. Зафиксировать clean production start procedure:
   - решить, что делать с pilot/test corpus: оставить как archived evidence,
     снести live DB после verified backup/offsite, или поднять новую production DB;
   - destructive cleanup допустим только после backup, verify, offsite upload,
     offsite verify и restore drill;
   - новый production run должен стартовать из явно чистого состояния или с
     явно помеченной историей, чтобы не смешивать pilot и production metrics/data.
9. Storage/archive unattended routine закрыт на 2026-06-23:
   - daily backup, daily archive, offsite sync/verify and weekly restore drill
     работали без ручного вмешательства;
   - guarded weekly S3 backup cleanup passed automatically on 2026-06-21;
   - search/detail production cadence не входила в этот soak и остаётся
     отдельным proof.
10. После search/detail cadence decision запускать sustained production
   collection с нуля или с явно маркированным corpus: регулярный search,
   параллельный detail catch-up, backups, offsite, alerts, retention и operator
   runbook.

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

Updated next experiment plan on 2026-06-23:

1. Не делить backlog на lanes/run/priority до необходимости production telemetry.
2. Monitor the next weekly S3 backup cleanup cycle; automation policy is proven
   by the 2026-06-21 timer run.
3. Зафиксировать clean production start procedure и решение по pilot/test corpus.
4. Зафиксировать production search/detail cadence.
5. Проверить search interference: detail-worker on/off during controlled search
   on fresh production routine.

Go/no-go:

- first-detail backlog убывает быстрее, чем растёт;
- search baseline не деградирует из-за detail drain;
- alert delivery уже включена.
- parallelism не создаёт duplicate detail fetches for the same selected rows.

### Stage 6. Week / Month unattended

1. Storage/archive supervised week completed by 2026-06-15.
2. Backup, archive housekeeping preview, offsite sync/verify, weekly restore
   drill and disk growth checked clean for the storage/archive contour.
3. Guarded weekly backup-retention automation passed on 2026-06-21.
4. Before full month unattended, define the production search/detail calendar.
5. Then transition to a monthly unattended window with alert-driven monitoring
   and weekly operator checklist.

## 6. Практический Вывод

Проект уже прошёл главный feasibility risk: собрать near-snapshot hh.ru search-space в рамках одного длинного sweep принципиально возможно.

Следующий риск уже не архитектурный, а операционный: понятная production
search/detail cadence, устойчивость к внешним outage, alert delivery и proof,
что first-detail backlog можно дренировать без деградации search contour.

Связанные документы:

- [current-readiness.md](/home/yurizinyakov/projects/hh_collector/docs/ops/current-readiness.md)
- [vps-pilot-checklist.md](/home/yurizinyakov/projects/hh_collector/docs/ops/vps-pilot-checklist.md)
- [first-detail-backlog.md](/home/yurizinyakov/projects/hh_collector/docs/ops/first-detail-backlog.md)
- [research-archive-v1.md](/home/yurizinyakov/projects/hh_collector/docs/ops/research-archive-v1.md)
- [unattended-operations.md](/home/yurizinyakov/projects/hh_collector/docs/ops/unattended-operations.md)
- [current-state-2026-06-23.md](/home/yurizinyakov/projects/hh_collector/docs/ops/current-state-2026-06-23.md)
- [hh-api-completeness-implementation-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-completeness-implementation-plan.md)
- [testing-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/testing-plan.md)
