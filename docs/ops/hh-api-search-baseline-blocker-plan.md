# HH API Search Baseline Blocker Plan

Статус: planner blocker closed, follow-up reliability work pending  
Дата: 2026-04-03

Этот документ фиксирует blocker, найденный на первом полном `search-only` baseline run, и его итоговый статус после следующего длинного rerun.

## 0. Итоговый статус на 2026-04-03

Изначальный blocker из этого документа закрыт.

Подтверждение:

- long rerun `5943c659-cd02-48c6-8296-c4ccbd46be73` прожил `~13.5h`;
- planner больше не упирался в `leaf area saturates and becomes unresolved`;
- `unresolved_partitions=0`;
- `split_partitions=1334`;
- `covered_terminal_partitions=14985`;
- итоговый near-complete corpus: `767451` unique vacancies.

Этот rerun завершился не на planner problem, а на внешнем transport outage:

- `URLError: [Errno -3] Temporary failure in name resolution`

Практический смысл:

- `area -> time_window` fallback validated;
- старый completeness blocker больше не является текущим bottleneck;
- следующий blocker теперь лежит в transport/outage resilience и operator recovery semantics.

## 1. Что именно сломалось

Первый baseline run:

- run id: `ee7a1dd2-e5f7-4c0d-b230-3b02abd1b291`
- log: [20260331T221639Z-search-baseline.log](/home/yurizinyakov/projects/hh_collector/.state/reports/20260331T221639Z-search-baseline.log)

Фактический terminal outcome:

- `status=failed`
- `failed_step=run_list_engine_v2`
- `coverage_ratio=0.0086`
- причина: `active child areas not found for hh_area_id=1; area-based split cannot refine this saturated partition`

Ключевые строки:

- [run failure](/home/yurizinyakov/projects/hh_collector/.state/reports/20260331T221639Z-search-baseline.log#L287)
- [unresolved split](/home/yurizinyakov/projects/hh_collector/.state/reports/20260331T221639Z-search-baseline.log#L282)

Это не HH API captcha/transport failure. Это planner completeness blocker.

## 2. Почему "просто перейти к следующей area" не решает проблему

Текущий planner v2 гарантирует полноту только пока saturated scope можно разбить на более узкие child areas.

Сейчас foundation такая:

- root planning строит `area` partitions;
- при saturation leaf scope split'ится только по child areas;
- если child areas нет, partition получает `unresolved`.

Источники:

- [planner policy](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/policies/planner.py#L49)
- [split fallback to unresolved](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/commands/split_partition.py#L165)

Для `hh_area_id=1` это означает:

- scope `area:1` уже leaf в area-tree;
- он насыщается на policy threshold `100` pages;
- перейти к другой area можно, но это не покрывает `area:1`;
- значит global search coverage остаётся неполным.

Именно поэтому "пошли дальше по дереву" годится для throughput, но не годится для exhaustiveness.

## 3. Почему это не всплыло на preflight

Preflight проверял другое:

- env/ops readiness;
- health, backup, restore drill;
- unit/integration quality gates;
- guarded smoke path;
- один небольшой list-engine run.

Этого хватило, чтобы доказать:

- system path жив;
- HH transport/auth baseline жив;
- planner v2 и list engine не падают на первых шагах.

Но этого не хватило, чтобы доказать:

- что planner tree не встретит saturated leaf без children.

То есть проблема не в том, что preflight был бесполезен. Проблема в том, что он был ops-grade, а не completeness-grade.

## 4. Почему именно time window выглядит первым кандидатом

Важно: это не догма и не единственный возможный split lever. Это первый практичный кандидат.

Почему:

- он даёт disjoint поддиапазоны внутри той же leaf-area;
- он уже совместим с текущим list request contract:
  - [SUPPORTED_LIST_SEARCH_PARAMS](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/commands/process_list_page.py#L44)
- short search layer уже несёт `published_at` и `created_at_hh`, так что у нас есть материал для анализа качества временного split:
  - [vacancy_search.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/dto/vacancy_search.py#L27)

Практический смысл:

- `area` остаётся первым split dimension;
- если area-leaf всё ещё saturated и child areas нет, нужен второй dimension;
- time-window split позволяет продолжить exhaustive coverage внутри hot leaf, а не объявлять его `unresolved`.

## 5. Какие альтернативы есть

Нужно проверять не только `time window`, а несколько вариантов и оценивать их как policy lever.

Реалистичные кандидаты:

1. `area -> time window`
2. `area -> professional_role`
3. `area -> hybrid`, где сначала пробуем один deterministic split dimension, а затем fallback на другой

Почему не стоит начинать с плохих levers:

- `text` не exhaustive;
- `salary` не даёт гарантии полного disjoint coverage;
- ad-hoc random sharding не даёт defendable completeness semantics.

## 6. Execution Plan

### Phase A. Research before code

Этот этап закрыт.

Сделано:

1. ranking split levers на hot leaf areas;
2. сравнение `area -> time window` против `area -> professional_role`;
3. выбор `time_window` как first fallback и `professional_role` как secondary supported dimension.

Артефакты:

- [ranking summary](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T225947Z-77fca9c4-split-dimension-ranking-summary.md)

### Phase B. Minimal implementation

Этот этап тоже закрыт минимальным slice.

Что реализовано:

1. `area` остался primary split;
2. saturated area-leaf without children теперь уходит не в `unresolved`, а в `time_window` children;
3. existing `time_window` partition умеет рекурсивно бисектиться дальше;
4. `unresolved` остаётся только для действительно неделимого time window;
5. planner shape оставляет место для будущего `professional_role` fallback, но не активирует его в `v1`.

Код:

- [planner.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/policies/planner.py)
- [split_partition.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/commands/split_partition.py)

Tests:

- [test_planner_v2.py](/home/yurizinyakov/projects/hh_collector/tests/unit/test_planner_v2.py)

### Phase C. Repeat clean search-only baseline

Этот шаг уже выполнен.

Нужно:

1. clean DB reset;
2. sync `areas`;
3. fresh backup;
4. повторный `run-once-v2 --detail-limit 0`.

Повтор действительно стал честным тестом полноты planner v2 после `time_window` fallback.

## 7. Decision rule

До следующего baseline run нужно ответить только на один вопрос:

- какой secondary split dimension даст defendable exhaustive semantics для hot leaf areas?

Текущие лучшие кандидаты:

- `area -> time window`
- `area -> professional_role`

Практическое решение на сейчас:

- сначала короткий research ranking между ними;
- затем MVP с поддержкой обоих;
- затем baseline run с одним выбранным first fallback;
- в следующем baseline/analysis cycle отдельно оценивать, нужен ли второй dimension уже в активной policy.

## 8. Research Result On 2026-04-01

Короткий ranking-probe уже выполнен:

- summary: [20260331T225947Z-77fca9c4-split-dimension-ranking-summary.md](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T225947Z-77fca9c4-split-dimension-ranking-summary.md)
- raw summary: [20260331T225947Z-77fca9c4-split-dimension-ranking-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T225947Z-77fca9c4-split-dimension-ranking-summary.json)

Что он показал:

- direct active leaf areas under `Россия (113)` сейчас всего `2`: `Москва (1)` и `Санкт-Петербург (2)`;
- обе leaf areas saturate already on baseline first page probe:
  - `Москва`: `168922 found`, `pages=100`
  - `Санкт-Петербург`: `66717 found`, `pages=100`
- `time window` уже на коротких trailing окнах сужает scope сильно лучше:
  - `Москва`: trailing `1h` -> `105 found`, `pages=6`
  - `Санкт-Петербург`: trailing `1h` -> `39 found`, `pages=2`
  - `Санкт-Петербург`: trailing `6h` -> `921 found`, `pages=47`
- sampled `professional_role` splits оказались слабее:
  - `Москва`: top sampled roles всё ещё дают `pages=100`
  - `Санкт-Петербург`: лучший sampled role дошёл до `pages=39`, но в среднем role split weaker/mixed

Важно:

- в sampled `100` vacancies на area multi-role overlap не проявился (`multi_role_share=0.0`);
- значит гипотеза про overlap не доказана этим sample;
- но `professional_role` всё равно пока не выглядит лучшим first fallback, потому что live narrowing заметно хуже, чем у `time window`, а exhaustive disjoint semantics всё ещё не доказаны policy-grade образом.

## 9. Decision

После ranking-probe и minimal implementation решение уже достаточно узкое:

- first fallback for `v1`: `area -> time window`
- secondary supported dimension: `professional_role`

Этот slice уже реализован. Следующий meaningful step теперь не новый planner experiment, а transport/resume hardening после near-complete baseline result.

## 10. Live Smoke After Implementation

Минимальный live smoke тоже уже проведён на новом `crawl_run`:

- run id: `0fe02a53-9c30-41ec-838c-fbabf1c8aba8`

Что подтверждено:

1. `Россия (113)` больше не ломает planner:
   - `process-partition-v2` для root `area:113` дал `split_done`
   - создано `88` child area partitions
2. `Москва (1)` больше не уходит в `unresolved`:
   - `process-partition-v2` для `area:1` дал `split_done`
   - созданы `2` child partitions с `scope_key=time_window:1:...`
3. recursive bisect работает живьём:
   - более свежий московский `time_window` partition тоже дал `split_done`
   - созданы ещё `2` более узких `time_window` child partitions
4. после этих шагов:
   - `split_partitions=3`
   - `unresolved_partitions=0`
   - `failed_partitions=0`

Практический смысл:

- blocker `leaf area saturates and immediately becomes unresolved` закрыт не только тестами, но и live planner path;
- следующий честный шаг теперь уже не planner-validation rerun, а reliability hardening и VPS rerun.
