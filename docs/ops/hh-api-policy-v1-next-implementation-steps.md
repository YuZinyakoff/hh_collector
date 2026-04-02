# HH API Policy V1 Next Implementation Steps

Статус: active queue  
Дата: 2026-03-31

Этот файл фиксирует ближайшие кодовые шаги после внедрения runtime-классификации `captcha` vs `transport`, bounded retry/backoff и baseline-prep hard stop на первом hard failed search partition.

Цель:

- не потерять implementation queue между сессиями;
- отделить оставшиеся code tasks от research tasks;
- держать `policy v1` в defendable, incremental scope.

Отдельное уточнение по полноте policy теперь вынесено сюда:

- [hh-api-completeness-policy-note.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-completeness-policy-note.md)
- [hh-api-completeness-implementation-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-completeness-implementation-plan.md)

Важно:

- immediate queue ниже описывает именно ближайший hardening scope для текущего scheduler draft;
- но после уточнения research goal стало ясно, что этого недостаточно для полной `first-detail completeness`;
- persistent first-detail backlog и отдельный drain contour теперь нужно считать следующим более крупным design/implementation slice, а не "необязательной оптимизацией".

## 1. Search Transport Budget Refinement

Что уже сделано:

- structured propagation search failure classification до orchestration layer;
- `run_list_engine_v2` больше не продолжает blind search после первого hard failed partition;
- `run-once-v2` и `resume-run-v2` теперь возвращают operator-readable transport/captcha failure message.

Что осталось внедрить:

- именно thresholded transport budget для coverage path:
  - `>= 3` consecutive transport failures;
  - либо `>= 5` total transport failures.

Почему это нужно:

- bounded retries уже есть на уровне одного logical request;
- но policy ещё требует run-level stop rule, чтобы scheduler не продолжал blind search loop на degraded upstream;
- это особенно важно после observed `connection reset by peer` bursts на elevated pressure contour.

Практический expected behavior:

- текущий request исчерпывает local retry budget;
- conservative baseline-prep behavior уже есть: hard stop на первом hard failed partition;
- следующий refinement должен разрешить не "stop on first transport error", а policy-aware transport budget.

Code focus:

- [process_list_page.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/commands/process_list_page.py)
- [run_collection_once_v2.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/commands/run_collection_once_v2.py)
- [scheduler_loop.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/commands/scheduler_loop.py)

## 2. Run Status Integration

Что осталось внедрить:

- довести status mapping из operator draft в orchestration/runtime flow;
- отдельно провести:
  - `completed_with_detail_errors` для закрытого list coverage с detail backlog;
  - `completed_with_unresolved` для незакрытого coverage path без hard crash.

Почему это нужно:

- runtime request handling уже различает `captcha` и `transport`;
- но scheduler/operator policy ещё не полностью отражается в terminal statuses run-а;
- без этого operator summary и repair path остаются менее точными, чем policy draft.

Практический expected behavior:

- detail failures после успешного coverage не должны понижать run до общего `failed`;
- unresolved coverage после breaker/cooldown decision должен быть продолжимым через existing repair/resume path;
- CLI и metrics должны показывать именно тот terminal status, который важен оператору.

Code focus:

- [run_collection_once_v2.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/commands/run_collection_once_v2.py)
- [reconcile_run.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/commands/reconcile_run.py)
- [scheduler_loop.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/commands/scheduler_loop.py)
- [detail.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/interfaces/cli/commands/detail.py)
- [run_once.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/interfaces/cli/commands/run_once.py)

## 3. Fresh Research Signal

Свежий `detail payload study` уже обновлён штатным DB-backed прогоном:

- [summary.md](/home/yurizinyakov/projects/hh_collector/.state/reports/detail-payload-study/20260331T134110Z/summary.md)
- [report.json](/home/yurizinyakov/projects/hh_collector/.state/reports/detail-payload-study/20260331T134110Z/report.json)

Результат:

- `10/10` vacancies with successful detail;
- `0/20` raw changed pairs;
- `0/20` normalized changed pairs;
- detail-only research fields: `address.can_edit`, `description`, `key_skills[].name`, `branded_description`.

Практический вывод:

- свежий DB-backed signal подтверждает прежний policy direction;
- `detail_limit=20` остаётся defendable conservative same-run default;
- но этого недостаточно для глобальной гарантии "каждая найденная vacancy хотя бы раз получила успешный `detail`";
- ближайшая implementation queue теперь уже не research-driven, а purely code-driven.

## 4. Recommended Order

Рекомендуемый порядок теперь:

1. Довести thresholded search transport budget до `3 consecutive / 5 total`.
2. Довести run status integration.
3. После этого не переходить сразу к "final policy", а отдельно спроектировать persistent first-detail backlog / drain contour.
4. Уже после этого переводить `policy v1` из draft в implementation-ready operator profile для полной research-completeness цели.
