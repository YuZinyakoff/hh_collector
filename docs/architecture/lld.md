# LLD: Платформа накопления данных hh.ru

**Статус:** Draft v1  
**Основание:** HLD платформы накопления данных hh.ru  
**Фокус:** сервисы, очереди, контракты, сценарии выполнения  
**Этап:** MVP накопления данных

---

## 1. Цель LLD

Данный документ уточняет верхнеуровневую архитектуру до уровня:

- сервисов и воркеров;
- их ответственности;
- форматов внутренних задач;
- сценариев выполнения;
- взаимодействия с PostgreSQL, raw storage и observability.

LLD не фиксирует все SQL-детали на уровне DDL, но определяет логические контуры реализации.

---

## 2. Границы MVP

В MVP входят:

- weekly global sweep;
- stateful tracking наблюдений вакансий;
- selective detail fetching;
- dictionary sync;
- raw storage;
- PostgreSQL как основное operational storage;
- observability;
- admin CLI;
- backup.

В MVP не входят:

- research feature engineering;
- ML/NLP enrichment;
- полноценный web UI;
- multi-node deployment;
- горизонтальное масштабирование по кластерам.

---

## 3. Технологические допущения

Базовый стек v1:

- Python 3.12+
- PostgreSQL
- Redis или Valkey как backend очереди/служебного состояния
- Docker Compose
- Prometheus + Grafana + Loki
- GlitchTip или совместимый lightweight error tracker

Рекомендуемый стиль реализации:

- монорепозиторий;
- единая кодовая база с несколькими entrypoints;
- shared domain library;
- асинхронные HTTP-запросы;
- типизированные модели данных;
- Alembic для миграций.

---

## 4. Логические сервисы

## 4.1. `scheduler`

### Назначение
Запускает регулярные системные процессы по расписанию.

### Ответственность
- создать новый `crawl_run`;
- инициировать weekly sweep;
- инициировать dictionary sync;
- запускать housekeeping jobs;
- запускать backup jobs;
- запускать health-check jobs.

### Вход
- cron/встроенное расписание;
- ручной запуск из CLI.

### Выход
- записи в `crawl_run`;
- публикация задач в очередь.

---

## 4.2. `planner`

### Назначение
Формирует пространство обхода и создаёт crawl partitions.

### Ответственность
- вычислить партиции sweep;
- записать `crawl_partition`;
- поставить `list_page_job` для стартовых страниц;
- при необходимости адаптивно дробить слишком широкие партиции.

### Planner v2 foundation

`planner v1` сохраняется как legacy smoke-path с одной глобальной partition для orchestration-lite сценариев.

`planner v2` является foundation для exhaustive collection architecture:

- initial planning строит не одну global partition, а набор disjoint area-root partitions;
- каждая partition хранится как узел дерева с `parent_partition_id`, `depth`, `scope_key`;
- saturated partition переводится в `split_required`, а после materialized split в `split_done`;
- children получают более узкий area scope и продолжают tree lineage того же `crawl_run`.

Поверх foundation реализован `list engine v2`:

- выбирает `pending` terminal leaves;
- дочитывает non-saturated leaf по страницам до конца;
- интерпретирует `pages_total_expected >= 100` как saturation policy v1;
- для saturated leaf читает первую страницу, фиксирует что scope слишком широк, и materialize'ит child partitions через area split;
- трактует `done + covered` как полно покрытый leaf, а `split_done + split` как coverage delegation на children.

На текущем шаге scheduler/queue orchestration всё ещё не реализованы:

- execution остаётся управляемым через CLI/use-case;
- unattended queue-based recursion остаётся следующим этапом.

Поверх tree execution добавлен минимальный reporting layer:

- `show-run-coverage` считает coverage summary из текущего набора `crawl_partition` этого run;
- `show-run-tree` печатает компактное text-tree представление с `depth`, `scope_key`, `status`, `coverage_status`;
- completion ratio интерпретируется как `covered_terminal_partitions / terminal_partitions`;
- run coverage не хранится отдельной таблицей, а вычисляется из tree semantics на чтении.

### Вход
- `crawl_run_id`
- конфигурация sweep policy
- актуальные справочники

### Выход
- записи `crawl_partition`
- задачи `list_page_job`

---

## 4.3. `list-worker`

### Назначение
Обходит поисковую выдачу hh API.

### Ответственность
- запросить страницу выдачи;
- сохранить raw payload;
- зафиксировать request log;
- извлечь вакансии из списка;
- создать seen events;
- обновить current state;
- создать detail jobs по правилам;
- при наличии следующей страницы продолжить pagination loop внутри того же terminal partition;
- при saturation не считать parent scope покрытым и split'нуть его в child partitions.

### Вход
- `list_page_job` для legacy flow;
- `process_partition_v2(partition_id)` для planner-v2 tree execution.

### Выход
- `api_request_log`
- `raw_api_payload`
- `vacancy_seen_event`
- обновление `vacancy_current_state`
- `detail_fetch_job`
- либо следующий page того же leaf scope;
- либо child partitions для следующего прохода по дереву.

---

## 4.4. `detail-worker`

### Назначение
Запрашивает полные карточки вакансий.

### Ответственность
- получить vacancy detail по ID;
- сохранить raw payload;
- построить snapshot;
- обновить каноническую сущность vacancy;
- обновить detail hash;
- зафиксировать успешную или неуспешную детализацию.

### Вход
`detail_fetch_job`

### Выход
- `api_request_log`
- `raw_api_payload`
- `vacancy_snapshot`
- обновление `vacancy`
- обновление `vacancy_current_state`

---

## 4.5. `dictionary-sync-worker`

### Назначение
Обновляет справочники hh API.

### Ответственность
- загружать справочники;
- использовать ETag/If-None-Match там, где применимо;
- обновлять dictionary tables;
- фиксировать версии/метаданные синхронизации.

### Вход
`dictionary_sync_job`

### Выход
- `dictionary_sync_run`
- обновлённые dictionary tables
- `api_request_log`
- `raw_api_payload`

---

## 4.6. `normalizer`

### Назначение
Нормализует данные, полученные из API, в внутреннюю схему.

### Ответственность
- нормализовать vacancy short/detail payload;
- унифицировать поля;
- нормализовать работодателей, географию, ссылки на справочники;
- строить content hashes.

### Вход
- raw list/detail payload
- справочники
- текущие записи БД

### Выход
- normalized domain objects
- hashes
- подготовленные структуры для upsert

---

## 4.7. `admin-cli`

### Назначение
Операторский интерфейс системы.

### Команды v1
- `create-run`
- `start-sweep`
- `rerun-partition`
- `retry-failed`
- `force-detail <vacancy_id>`
- `sync-dictionaries`
- `mark-run-failed`
- `show-run`
- `show-partition`
- `health-check`

Текущий operator path для planner/list execution:

- `plan-run` для legacy single-partition smoke flow;
- `plan-run-v2` для создания area-root tree;
- `process-list-page` для legacy page-by-page flow;
- `process-partition-v2` для одного terminal leaf с pagination/saturation handling;
- `run-list-engine-v2` для прохода по всем pending terminal leaves внутри `crawl_run`.
- `show-run-coverage` для tree-based coverage summary;
- `show-run-tree` для компактного tree view без SQL.

### Назначение
- ручной запуск;
- диагностика;
- восстановление;
- поддержка эксплуатации без web UI.

---

## 4.8. `housekeeping-worker`

### Назначение
Техническое обслуживание системы.

### Ответственность
- cleanup старых raw/log records по retention;
- пересчёт служебных counters;
- архивирование;
- контроль подвисших jobs;
- mark stale partitions/jobs.

---

## 4.9. `backup-worker`

### Назначение
Резервное копирование.

### Ответственность
- pg_dump / иная backup strategy;
- backup raw storage metadata;
- загрузка в удалённое хранилище;
- логирование результата backup.

---

## 5. Внутренние типы задач

## 5.1. `plan_sweep_job`

```json
{
  "crawl_run_id": "uuid",
  "sweep_policy": "weekly_global_v1"
}
````

## 5.2. `list_page_job`

```json
{
  "crawl_run_id": "uuid",
  "partition_id": "uuid",
  "page": 0,
  "params": {
    "area": 113,
    "professional_role": null,
    "date_from": null,
    "date_to": null
  },
  "attempt": 1
}
```

## 5.3. `detail_fetch_job`

```json
{
  "crawl_run_id": "uuid",
  "vacancy_hh_id": "12345678",
  "reason": "first_seen",
  "attempt": 1
}
```

### Допустимые `reason`

* `first_seen`
* `short_changed`
* `ttl_refresh`
* `manual_refetch`

## 5.4. `dictionary_sync_job`

```json
{
  "dictionary_name": "areas",
  "attempt": 1
}
```

## 5.5. `backup_job`

```json
{
  "backup_type": "postgres_full",
  "started_by": "scheduler"
}
```

---

## 6. Очереди

Минимальный набор очередей:

* `planner`
* `list`
* `detail`
* `dictionary`
* `housekeeping`
* `backup`

### Рекомендуемая политика

* planner: low volume
* list: основная рабочая очередь
* detail: отдельная очередь, чтобы детализация не душила список
* dictionary: редкая системная очередь
* housekeeping: низкий приоритет
* backup: отдельный низкочастотный контур

---

## 7. Правила постановки detail jobs

`detail_fetch_job` создаётся, если выполняется хотя бы одно условие:

1. вакансия наблюдается впервые;
2. short hash изменился;
3. detail snapshot отсутствует;
4. detail snapshot старше TTL;
5. оператор принудительно запросил refresh.

### Рекомендуемый TTL v1

* 14 дней для наблюдаемых активных вакансий

---

## 8. Сценарии выполнения

## 8.1. Weekly sweep

1. Scheduler создаёт `crawl_run`.
2. Planner формирует партиции.
3. На каждую партицию ставится `list_page_job(page=0)`.
4. List worker:

   * делает запрос к hh API;
   * логирует запрос;
   * сохраняет raw payload;
   * создаёт/обновляет сущности вакансий;
   * пишет seen events;
   * создаёт detail jobs;
   * ставит следующую страницу, если она есть.
5. После завершения всех страниц партиция получает статус `done`.
6. После завершения всех партиций `crawl_run` получает статус `done` либо `partial_failed`.

---

## 8.2. Ошибка list page

Если запрос страницы завершился ошибкой:

* request логируется;
* увеличивается attempt;
* применяется backoff;
* job переходит в retry;
* после превышения лимита retry партиция помечается как failed.

---

## 8.3. Ошибка detail fetch

Если detail fetch завершился ошибкой:

* фиксируется `detail_fetch_status=failed`;
* detail job ретраится ограниченное число раз;
* failure не должен валить весь run;
* вакансия продолжает жить в seen-state даже без detail snapshot.

---

## 8.4. Исчезновение вакансии

Если вакансия не встретилась в очередном sweep:

* запись не удаляется;
* seen-state обновляется логикой post-run reconciliation;
* `consecutive_missing_runs += 1`;
* после порога `inactive_threshold_runs` выставляется `is_probably_inactive = true`.

---

## 9. Rate limiting и retries

## 9.1. Глобальные принципы

* централизованный rate limiter;
* ограничение на общий RPS;
* отдельные лимиты для list/detail при необходимости;
* jitter в backoff;
* уважение к 429 и 5xx;
* circuit breaker при массовых ошибках.

## 9.2. Рекомендуемые параметры v1

* global target: 3-5 req/s
* list concurrency: ограниченная
* detail concurrency: ниже, чем list
* max retries: 5
* backoff: exponential + jitter

---

## 10. Contracts между слоями

## 10.1. API client -> workers

Возвращает:

* parsed response
* status code
* headers
* raw body
* latency
* retry metadata

## 10.2. Normalizer -> storage layer

Возвращает:

* normalized vacancy short
* normalized vacancy detail
* normalized employer refs
* short hash
* detail hash

## 10.3. Workers -> observability

Обязательные поля события:

* service_name
* run_id
* partition_id
* vacancy_hh_id (если применимо)
* job_type
* status
* duration_ms
* attempt

---

## 11. State machine

## 11.1. CrawlRun

* `created`
* `planning`
* `running`
* `partial_failed`
* `done`
* `failed`
* `cancelled`

## 11.2. CrawlPartition

* `pending`
* `queued`
* `running`
* `retrying`
* `done`
* `failed`
* `cancelled`

## 11.3. Detail fetch status

* `not_requested`
* `queued`
* `running`
* `done`
* `failed`

---

## 12. Конфигурация

Основные конфиги:

* `SWEEP_SCHEDULE`
* `GLOBAL_RPS_LIMIT`
* `LIST_CONCURRENCY`
* `DETAIL_CONCURRENCY`
* `DETAIL_REFRESH_TTL_DAYS`
* `MAX_RETRIES`
* `RAW_RETENTION_DAYS`
* `LOG_RETENTION_DAYS`
* `INACTIVE_THRESHOLD_RUNS`
* `DB_DSN`
* `REDIS_DSN`
* `HH_API_BASE_URL`
* `HH_USER_AGENT`

---

## 13. Локальная разработка

Требования для local dev:

* docker compose up
* отдельный `.env.local`
* локальная БД
* локальный Redis/Valkey
* mock/stub режим для hh API по необходимости
* миграции одной командой
* команды запуска отдельных worker entrypoints

---

## 14. Критерии готовности MVP

MVP считается готовым, если:

1. weekly sweep можно запустить одной командой;
2. партиции создаются и обрабатываются автоматически;
3. raw payload сохраняется;
4. seen events и vacancy current state корректно обновляются;
5. new/changed vacancies получают detail snapshots;
6. Prometheus/Grafana/Loki доступны;
7. ошибки попадают в error tracker;
8. backup хотя бы PostgreSQL работает;
9. system restart не ломает состояние runs/partitions.
