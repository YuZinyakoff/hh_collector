# HH API Scheduler Policy v1 Draft

## Scope

Этот документ фиксирует первый формальный scheduler/ops draft для HH collection policy.

Он отвечает не на вопрос "как агрессивно выжать API", а на вопрос "какой режим можно защищать операционно месяцами".

Основа:

- `search-first`;
- `application_token` как operator-default auth contour;
- conservative load envelope;
- detail только после полного list coverage;
- отдельная трактовка `captcha` и transport errors.

Источники сигнала:

- [hh-api-collection-policy-draft.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-collection-policy-draft.md)
- [hh-api-collection-strategy-research-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-collection-strategy-research-plan.md)
- [hh-api-completeness-policy-note.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-completeness-policy-note.md)

## Operator Defaults

Рекомендованный scheduler contour:

- auth mode: `application_token`
- header contour: `dual`
- run type: `weekly_sweep`
- scheduler interval: `900s`
- `sync_dictionaries=no` на каждом regular tick
- `detail_limit=20`
- `detail_refresh_ttl_days=30`

Практически это означает:

- scheduler живёт на частом, но не агрессивном tick;
- новый list run не стартует overlap-ом, потому что admission control уже есть;
- повторный search after failure не происходит мгновенно;
- detail path остаётся bounded и не начинает конкурировать с coverage.

Важно:

- этот draft уже выглядит defendable для `search` coverage;
- но `detail_limit=20` здесь пока означает bounded same-run detail budget, а не полную гарантию "хотя бы один detail на каждую найденную vacancy";
- gap по `first-detail completeness` отдельно зафиксирован в [hh-api-completeness-policy-note.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-completeness-policy-note.md).

Recommended launch shape:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main scheduler-loop \
  --interval-seconds 900 \
  --sync-dictionaries no \
  --detail-limit 20 \
  --detail-refresh-ttl-days 30 \
  --run-type weekly_sweep \
  --triggered-by scheduler-loop
```

## Search Policy

Правила для list/search stage:

- приоритет всегда у полного list coverage;
- selective detail не запускается до полного list coverage;
- search contour не должен быть агрессивнее подтверждённого research baseline;
- practical target envelope на одном network path сейчас лучше считать `<= ~80 search req/min`, а не гнаться за burst throughput.

Почему:

- `application_token + dual`, baseline-safe contour уже прошёл `2000` search requests без `403/captcha`;
- aggressive contour `workers=4` не стал defendable baseline;
- при повышении pressure на token-default path раньше всплыл transport noise, чем стабильная captcha boundary.

## Stop Conditions

Search stage:

- stop search on first `captcha_required`;
- не делать fast same-run retry loop после captcha;
- не поднимать workers/concurrency/burst как "лечение" после captcha;
- следующий search probe не раньше следующего regular scheduler tick.

Detail stage:

- detail разрешён только после полного list coverage;
- если часть detail fetch падает, run может честно завершиться как `completed_with_detail_errors`;
- после этого чинить backlog через `retry-failed-details`, а не повторять весь list run.

## Failure Classes

Для operator policy нужны как минимум три класса ошибок.

`search_captcha`

- `status_code=403` вместе с `captcha_required`;
- наличие `captcha_url` / `captcha_url_with_backurl`;
- трактуется как search-specific anti-bot event.

`transport_transient`

- `TimeoutError`
- `TLS handshake timeout`
- `Connection reset by peer`
- `Network is unreachable`
- `RemoteDisconnected`
- другие сетевые ошибки без captcha signal

Трактуется как network/transport instability, а не как anti-bot proof.

`auth_failure`

- `401`
- явный token rejection
- устойчивый `403`, который не выглядит captcha-flow

Трактуется отдельно от transport и отдельно от search captcha.

## Retry And Backoff

### Search Transport

Целевой operator rule:

- для одного logical search request: максимум `2` retries внутри run;
- backoff: `5s`, затем `30s`;
- если после этого request всё ещё падает, не крутить blind loop внутри того же run.

Circuit breaker:

- если в одном run накопилось `>= 3` consecutive search transport failures, остановить новые search requests;
- если в одном run накопилось `>= 5` total search transport failures, тоже остановить новые search requests;
- дальнейшая попытка только на следующем scheduler tick.

Это правило консервативно, но соответствует observed signal: на token-default path elevated pressure уже дал series of connection resets до captcha.

### Search Captcha

При первом `search_captcha`:

- прекратить новые search requests в текущем run;
- не делать `trigger-run-now` сразу после этого;
- ждать следующего regular tick;
- cooldown windows `5m..120m` остаются полезным follow-up study, но не обязательным блокером для `v1`.

### Detail Transport

Для одного detail request:

- максимум `1` retry after `5s`;
- если detail всё равно падает, не блокировать закрытый coverage path;
- оставлять run в `completed_with_detail_errors` и чинить backlog отдельно.

### Dictionary Transport

Практический rule:

- не синкать dictionaries на каждом regular tick;
- если dictionary sync запущен отдельно и падает по transport, допускается `1` retry after `30s`;
- если повтор тоже падает, не смешивать это с list/search contour и разбирать отдельно.

## Status Mapping

Operator-facing interpretation:

- `succeeded`: list coverage закрыт, detail stage не оставил долгов.
- `completed_with_detail_errors`: list coverage закрыт, repair path нужен только для detail backlog.
- `completed_with_unresolved`: проблема в coverage/planner contour, а не в detail backlog.
- `failed`: критическая orchestration/list ошибка либо исчерпанный transport budget в coverage path.

Важно:

runtime теперь уже различает `captcha` и `transport` на command level и делает bounded retries для transport responses:

- [client.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/infrastructure/hh_api/client.py)
- [response_classification.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/infrastructure/hh_api/response_classification.py)
- [process_list_page.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/commands/process_list_page.py)
- [fetch_vacancy_detail.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/commands/fetch_vacancy_detail.py)
- [sync_dictionary.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/commands/sync_dictionary.py)

Что ещё не закрыто полностью:

- conservative baseline-prep stop на первом hard failed search partition уже enforced runtime behavior;
- но thresholded run-level transport budget `3` consecutive / `5` total пока всё ещё остаётся operator rule, а не полной runtime state machine;
- scheduler status promotion до `completed_with_detail_errors` и `completed_with_unresolved` ещё требует отдельной интеграции в orchestration flow.

## Why This Policy Is Adequate

Для research-задачи policy выглядит адекватной, потому что она:

- максимизирует устойчивый list coverage вместо burst throughput;
- не смешивает coverage и detail repair;
- остаётся внутри approved app contour;
- уже доказала viability на длинном baseline slot;
- не требует unstable `workers=4` branch для достижения useful throughput.

Практический load signal на сегодня:

- baseline token run: `2000` search requests за примерно `25.2` минуты, то есть около `79 req/min`, без captcha;
- bounded mixed baseline: clean `120 search + 24 detail`;
- aggressive token pressure probe: `130` search requests без captcha, но с `5` connection resets.

Вывод:

- для месяцев устойчивого сбора нам выгоднее предсказуемый `search-first` contour с bounded detail;
- policy с чуть меньшим throughput, но без burst spikes лучше соответствует вашей research цели, чем попытка жить на границе captcha.

Но это нужно читать узко:

- данный draft уже хорошо защищает `search coverage`;
- для полной research-completeness цели ему ещё не хватает persistent first-detail backlog semantics и отдельного drain contour для вакансий без успешного `detail`.

## Follow-Ups

До полного implementation-grade `v1` остаются два практических кодовых шага:

1. Довести search transport budget от conservative hard stop до policy-target `3` consecutive / `5` total.
2. Довести scheduler/run status mapping до `completed_with_detail_errors` и `completed_with_unresolved`.

Текущая implementation queue вынесена отдельно:

- [hh-api-policy-v1-next-implementation-steps.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-policy-v1-next-implementation-steps.md)

Свежий detail-value signal уже обновлён штатным DB-backed study:

- [detail payload study summary](/home/yurizinyakov/projects/hh_collector/.state/reports/detail-payload-study/20260331T134110Z/summary.md)

Cooldown follow-up не отменён, но больше не главный blocker:

- для него уже есть отдельный driver [hh-api-probe-cooldown-driver.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-probe-cooldown-driver.md).
