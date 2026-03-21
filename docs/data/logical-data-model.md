# Логическая модель данных: hh.ru data collection platform

**Статус:** Draft v1  
**Фокус:** operational storage и история наблюдений  
**СУБД:** PostgreSQL

---

## 1. Цель

Данный документ описывает логическую модель данных для MVP платформы накопления данных hh.ru.

Модель ориентирована на:
- stateful crawl;
- хранение истории наблюдений;
- selective detail fetching;
- воспроизводимость и поддержку последующего расширения.

---

## 2. Основные принципы модели

1. История и текущее состояние разделены.
2. Сырые данные и нормализованные данные разделены.
3. Seen events не заменяются current state.
4. Snapshot history не заменяется канонической сущностью vacancy.
5. Все ключевые процессы привязаны к `crawl_run`.

---

## 3. Группы сущностей

Модель делится на 5 блоков:

1. Control Plane
2. API Logging / Raw
3. Domain Core
4. History / State Tracking
5. Dictionary / Reference Data

---

## 4. Control Plane

## 4.1. `crawl_run`
Один запуск sweep или служебного процесса.

Поля:
- `id` UUID PK
- `run_type` text
- `status` text
- `triggered_by` text default `'system'`
- `config_snapshot_json` jsonb default `'{}'::jsonb`
- `partitions_total` int default 0
- `partitions_done` int default 0
- `partitions_failed` int default 0
- `started_at` timestamptz default `now()`
- `finished_at` timestamptz null
- `notes` text null

Индексы:
- `idx_crawl_run_status`
- `idx_crawl_run_started_at`

---

## 4.2. `crawl_partition`
Единица пространства обхода.

Поля:
- `id` UUID PK
- `crawl_run_id` UUID FK -> crawl_run.id
- `parent_partition_id` UUID null FK -> crawl_partition.id
- `partition_key` text
- `scope_key` text
- `params_json` jsonb
- `status` text
- `depth` int default 0
- `split_dimension` text null
- `split_value` text null
- `planner_policy_version` text default `'v1'`
- `is_terminal` boolean default true
- `is_saturated` boolean default false
- `coverage_status` text default `'unassessed'`
- `pages_total_expected` int null
- `pages_processed` int default 0
- `items_seen` int default 0
- `retry_count` int default 0
- `started_at` timestamptz null
- `finished_at` timestamptz null
- `last_error_message` text null
- `created_at` timestamptz default `now()`

Ограничения:
- unique (`crawl_run_id`, `partition_key`)
- unique (`crawl_run_id`, `scope_key`)

Индексы:
- `idx_crawl_partition_run_id`
- `idx_crawl_partition_status`
- `idx_crawl_partition_parent_partition_id`
- `idx_crawl_partition_coverage_status`

Семантика planner v2:

- root partitions имеют `parent_partition_id = null` и `depth = 0`;
- child partitions указывают на parent узел того же `crawl_run`;
- `status` описывает lifecycle партиции как operational unit;
- `coverage_status` описывает outcome покрытия search scope;
- `scope_key` задаёт каноническую identity search scope внутри `crawl_run`;
- `done + covered` означает terminal leaf, полностью дочитанный по pagination loop;
- `split_done + split` означает saturated parent, который не считается покрытым сам по себе и делегирует coverage child partitions;
- `unresolved` означает scope, который не удалось сузить текущей split-policy и который нельзя считать покрытым.

Reporting semantics на уровне `crawl_run` выводятся из этих же полей:

- `coverage_ratio = covered_terminal_partitions / terminal_partitions`;
- `pending_terminal_partitions` показывают, сколько leaf scopes ещё не покрыто;
- `split_partitions` показывают, сколько parent scopes делегировали coverage детям;
- `failed` и `unresolved` не должны интерпретироваться как покрытые части дерева.

Tree-aware orchestration v2 читает итог run именно из этих агрегатов:

- `succeeded` возможно только когда `coverage_ratio = 1.0`, `pending_terminal_partitions = 0`, `unresolved_partitions = 0`, `failed_partitions = 0`;
- `completed_with_detail_errors` означает, что list tree покрыт полностью и `reconcile_run` завершён, но часть selective detail fetches завершилась с ошибками;
- `completed_with_unresolved` означает, что дерево не покрыто полностью, но terminal failure в execution path нет;
- `failed` означает failed partitions или критическую orchestration/list ошибку.

Operational continuation semantics:

- `completed_with_unresolved` допускает `resume-run-v2` поверх того же `crawl_run`: status run может снова стать `created`, а unresolved terminal leaves переводятся обратно в `pending` для повторного tree execution;
- `completed_with_detail_errors` не считается list coverage failure и не требует нового run: detail repair выполняется отдельно поверх того же `crawl_run`;
- promotion из `completed_with_detail_errors` в `succeeded` допустим только после того, как derived repair backlog для этого run опустел.
- housekeeping retention допускает удаление старых terminal `crawl_run` по `finished_at`, но active `status=created` не должны затрагиваться.

---

## 5. API Logging / Raw

## 5.1. `api_request_log`
Журнал HTTP-запросов к API.

Поля:
- `id` bigserial PK
- `crawl_run_id` UUID null FK
- `crawl_partition_id` UUID null FK
- `request_type` text
- `endpoint` text
- `method` text default `'GET'`
- `params_json` jsonb default `'{}'::jsonb`
- `request_headers_json` jsonb null
- `status_code` int
- `latency_ms` int
- `attempt` int default 1
- `requested_at` timestamptz default `now()`
- `response_received_at` timestamptz null
- `error_type` text null
- `error_message` text null

Индексы:
- `idx_api_request_log_requested_at`
- `idx_api_request_log_status_code`
- `idx_api_request_log_run_id`
- `idx_api_request_log_partition_id`

---

## 5.2. `raw_api_payload`
Сырой ответ API.

Поля:
- `id` bigserial PK
- `api_request_log_id` bigint FK -> api_request_log.id
- `endpoint_type` text
- `entity_hh_id` text null
- `payload_json` jsonb
- `payload_hash` text
- `received_at` timestamptz default `now()`

Индексы:
- `idx_raw_api_payload_request_log_id`
- `idx_raw_api_payload_entity_hh_id`
- `idx_raw_api_payload_received_at`

Примечание:
На старте можно хранить raw в PostgreSQL. Позже можно вынести payload в объектное хранилище, оставив metadata в БД.

Retention semantics:

- raw payload может чиститься housekeeping-проходом по TTL;
- active `crawl_run.status=created` не должны терять связанные raw rows;
- raw rows, на которые всё ещё ссылаются retained `vacancy_snapshot`, безопаснее сохранять дольше TTL как conservative guardrail.

---

## 6. Domain Core

## 6.1. `employer`
Работодатель.

Поля:
- `id` UUID PK
- `hh_employer_id` text unique
- `name` text
- `alternate_url` text null
- `site_url` text null
- `area_id` UUID null FK -> area.id
- `is_trusted` boolean null
- `raw_first_seen_at` timestamptz
- `raw_last_seen_at` timestamptz
- `created_at` timestamptz
- `updated_at` timestamptz

Индексы:
- `uq_employer_hh_employer_id`
- `idx_employer_name`
- `idx_employer_area_id`

---

## 6.2. `area`
География / регион.

Поля:
- `id` UUID PK
- `hh_area_id` text unique
- `name` text
- `parent_area_id` UUID null FK -> area.id
- `level` int null
- `path_text` text null
- `is_active` boolean default true
- `created_at` timestamptz
- `updated_at` timestamptz

Индексы:
- `uq_area_hh_area_id`
- `idx_area_parent_area_id`

---

## 6.3. `vacancy`
Каноническая сущность вакансии.

Поля:
- `id` UUID PK
- `hh_vacancy_id` text unique
- `employer_id` UUID null FK -> employer.id
- `area_id` UUID null FK -> area.id
- `name_current` text
- `published_at` timestamptz null
- `created_at_hh` timestamptz null
- `archived_at_hh` timestamptz null
- `alternate_url` text null
- `employment_type_code` text null
- `schedule_type_code` text null
- `experience_code` text null
- `source_type` text default 'hh_api'
- `created_at` timestamptz
- `updated_at` timestamptz

Индексы:
- `uq_vacancy_hh_vacancy_id`
- `idx_vacancy_employer_id`
- `idx_vacancy_area_id`
- `idx_vacancy_published_at`

---

## 6.4. `vacancy_professional_role`
Связь вакансии с профессиональными ролями.

Поля:
- `vacancy_id` UUID FK -> vacancy.id
- `professional_role_id` UUID FK -> professional_role.id
- PK (`vacancy_id`, `professional_role_id`)

Индексы:
- `idx_vacancy_prof_role_role_id`

---

## 6.5. `professional_role`
Справочник профессиональных ролей.

Поля:
- `id` UUID PK
- `hh_professional_role_id` text unique
- `name` text
- `category_name` text null
- `is_active` boolean default true
- `created_at` timestamptz
- `updated_at` timestamptz

Индексы:
- `uq_professional_role_hh_professional_role_id`

---

## 7. History / State Tracking

## 7.1. `vacancy_seen_event`
Факт наблюдения вакансии.

Поля:
- `id` bigserial PK
- `vacancy_id` UUID FK -> vacancy.id
- `crawl_run_id` UUID FK -> crawl_run.id
- `crawl_partition_id` UUID FK -> crawl_partition.id
- `seen_at` timestamptz default `now()`
- `list_position` int null
- `short_hash` text
- `short_payload_ref_id` bigint null FK -> raw_api_payload.id

Ограничения:
- unique (`vacancy_id`, `crawl_partition_id`, `seen_at`)

Индексы:
- `idx_vacancy_seen_event_vacancy_id`
- `idx_vacancy_seen_event_run_id`
- `idx_vacancy_seen_event_partition_id`
- `idx_vacancy_seen_event_seen_at`

---

## 7.2. `vacancy_current_state`
Агрегированное текущее состояние наблюдения вакансии.

Поля:
- `vacancy_id` UUID PK FK -> vacancy.id
- `first_seen_at` timestamptz
- `last_seen_at` timestamptz
- `seen_count` int default 1
- `consecutive_missing_runs` int default 0
- `is_probably_inactive` boolean default false
- `last_seen_run_id` UUID null FK -> crawl_run.id
- `last_short_hash` text null
- `last_detail_hash` text null
- `last_detail_fetched_at` timestamptz null
- `detail_fetch_status` text default `'not_requested'`
- `updated_at` timestamptz default `now()`

Индексы:
- `idx_vacancy_current_state_last_seen_at`
- `idx_vacancy_current_state_inactive`
- `idx_vacancy_current_state_detail_status`

---

## 7.3. `vacancy_snapshot`
Исторический snapshot вакансии.

Поля:
- `id` bigserial PK
- `vacancy_id` UUID FK -> vacancy.id
- `snapshot_type` text
- `captured_at` timestamptz default `now()`
- `crawl_run_id` UUID null FK -> crawl_run.id
- `short_hash` text null
- `detail_hash` text null
- `short_payload_ref_id` bigint null FK -> raw_api_payload.id
- `detail_payload_ref_id` bigint null FK -> raw_api_payload.id
- `normalized_json` jsonb null
- `change_reason` text null

Индексы:
- `idx_vacancy_snapshot_vacancy_id`
- `idx_vacancy_snapshot_captured_at`
- `idx_vacancy_snapshot_detail_hash`

Примечание:
В MVP допустимо хранить normalized snapshot в jsonb. Позже можно вынести часть полей в более нормализованную историческую модель.

Retention semantics:

- `vacancy_snapshot` допускает TTL cleanup, но conservative housekeeping сохраняет latest snapshot на каждую vacancy;
- удаление старых snapshot rows не должно ломать `vacancy_current_state`, потому что current state хранится отдельно и не зависит FK от snapshot history;
- если старый `crawl_run` удалён раньше snapshot row, `crawl_run_id` может стать `null` через FK semantics: это допустимо для retention path, но сокращает historical lineage.

---

## 7.4. `detail_fetch_attempt`
Журнал попыток детализации.

Поля:
- `id` bigserial PK
- `vacancy_id` UUID FK -> vacancy.id
- `crawl_run_id` UUID null FK -> crawl_run.id
- `reason` text
- `attempt` int default 1
- `status` text
- `requested_at` timestamptz default `now()`
- `finished_at` timestamptz null
- `error_message` text null

Индексы:
- `idx_detail_fetch_attempt_vacancy_id`
- `idx_detail_fetch_attempt_status`
- `idx_detail_fetch_attempt_requested_at`

Derived repair backlog semantics:

- repair backlog не хранится отдельной таблицей;
- backlog для конкретного `crawl_run` вычисляется как latest `detail_fetch_attempt` per `vacancy_id` внутри этого run, где latest status = `failed`;
- backlog item считается `repaired`, когда более новая попытка для того же `vacancy_id` в этом же `crawl_run` получает status = `succeeded`;
- если после retry latest status снова `failed`, item остаётся в backlog и run сохраняет status `completed_with_detail_errors`;
- reason `repair_backlog` используется для operator-driven retry path и позволяет отделить post-run repair от обычной selective detail policy.

Retention semantics:

- старые `detail_fetch_attempt` могут чиститься по TTL, но conservative housekeeping сохраняет latest attempt на `(vacancy_id, crawl_run_id)`;
- это позволяет уменьшать historical noise, не ломая текущий repair backlog derivation для ещё нужных run-ов.

---

## 8. Dictionary / Reference Data

## 8.1. `dictionary_sync_run`
Журнал синхронизации справочников.

Поля:
- `id` UUID PK
- `dictionary_name` text
- `status` text
- `etag` text null
- `source_status_code` int null
- `notes` text null
- `started_at` timestamptz default `now()`
- `finished_at` timestamptz null

Индексы:
- `idx_dictionary_sync_run_name`
- `idx_dictionary_sync_run_started_at`

---

## 8.2. Дополнительные справочники
Можно завести отдельные таблицы:
- `employment_type`
- `schedule_type`
- `experience_type`
- `currency`
- `vacancy_label`
- `billing_type` и др.

На старте допустимо хранить часть кодов прямо в `vacancy`, если это ускоряет MVP.

---

## 9. Связи между сущностями

Ключевые связи:

- `crawl_run 1:N crawl_partition`
- `crawl_partition 1:N crawl_partition` via `parent_partition_id`
- `crawl_run 1:N api_request_log`
- `crawl_partition 1:N api_request_log`
- `api_request_log 1:1..N raw_api_payload`
- `vacancy 1:1 vacancy_current_state`
- `vacancy 1:N vacancy_seen_event`
- `vacancy 1:N vacancy_snapshot`
- `vacancy 1:N detail_fetch_attempt`
- `employer 1:N vacancy`
- `area 1:N vacancy`
- `vacancy M:N professional_role`

---

## 10. Что является источником истины

- Текущая каноническая вакансия: `vacancy`
- Текущее состояние наблюдения: `vacancy_current_state`
- История наблюдений: `vacancy_seen_event`
- История содержимого: `vacancy_snapshot`
- Сырые данные API: `raw_api_payload`
- История запросов и ошибок: `api_request_log`

---

## 11. Ключевые инварианты

1. Вакансия не удаляется из БД из-за того, что перестала наблюдаться.
2. Seen events append-only.
3. Snapshot history append-only.
4. Current state пересчитывается/upsert-ится, но не подменяет историю.
5. Один и тот же `hh_vacancy_id` соответствует одной записи `vacancy`.
6. Повторный detail fetch не должен ломать исторические snapshots.

---

## 12. Рекомендуемые индексы для MVP

Обязательные индексы:

- `vacancy.hh_vacancy_id`
- `employer.hh_employer_id`
- `area.hh_area_id`
- `professional_role.hh_professional_role_id`
- `area(parent_area_id)`
- `employer(area_id)`
- `vacancy_seen_event(vacancy_id)`
- `vacancy_seen_event(crawl_run_id)`
- `vacancy_seen_event(crawl_partition_id)`
- `vacancy_snapshot(vacancy_id)`
- `vacancy_current_state(last_seen_at desc)`
- `vacancy_professional_role(professional_role_id)`
- `crawl_partition(crawl_run_id)`
- `crawl_partition(status)`
- `api_request_log(requested_at desc)`
- `api_request_log(status_code)`

---

## 13. Что можно упростить в MVP

Допустимые упрощения:

1. `raw_api_payload.payload_json` хранить прямо в PostgreSQL.
2. Часть справочников не выносить сразу в отдельные таблицы.
3. Salary/skills/key_skills пока не нормализовать в отдельные исторические сущности.
4. Нормализованный snapshot хранить в `jsonb`.

---

## 14. Что лучше не упрощать

Не стоит упрощать:

1. `crawl_run`
2. `crawl_partition`
3. `vacancy_seen_event`
4. `vacancy_current_state`
5. `vacancy_snapshot`
6. `api_request_log`

Это backbone всей stateful-системы.
