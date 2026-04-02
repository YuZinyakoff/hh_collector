# HH API Collection Policy Draft

Статус: provisional  
Дата: 2026-03-29

Этот документ фиксирует не финальную "идеальную" policy, а текущий лучший policy draft по данным research contour.

Отдельное уточнение по полноте research goal и `detail` semantics вынесено сюда:

- [hh-api-completeness-policy-note.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-completeness-policy-note.md)

## 1. Цель policy

Наша цель не в том, чтобы "обойти captcha", а в том, чтобы выбрать устойчивый режим сбора данных, который:

- максимизирует стабильный `search` coverage;
- позволяет в итоге получить хотя бы один успешный `detail` для каждой vacancy, найденной через `search`;
- минимизирует ручное вмешательство;
- использует `detail` только там, где его ценность оправдывает API pressure;
- даёт scheduler понятные stop/retry rules;
- не зависит от proxy как от primary design choice.

Формализованный operator-facing draft на основе этого research summary вынесен отдельно:

- [hh-api-scheduler-policy-v1-draft.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-scheduler-policy-v1-draft.md)

## 2. Что уже можно считать доказанным

На текущий момент есть достаточно оснований считать, что:

- baseline нужно строить вокруг `search-first`, а не вокруг равноправного смешивания `search` и `detail`;
- burst/concurrency опаснее для `GET /vacancies`, чем сам по себе длинный single-stream search contour;
- `historical replay` на одном потоке может проходить длинную последовательность без captcha;
- conservative batched contour `workers=3` + `burst_pause=1s` уже прошёл clean не только `search-only`, но и mixed workload на `search_prefix=100` и `search_prefix=120`;
- более агрессивный contour `workers=4` + `burst_pause=1s` уже не выглядит безопасным baseline:
  - `search-only` на `search_prefix=120` словил первую search-captcha на `119`-м search request;
  - clean rerun `deferred detail after coverage` на том же contour словил первую search-captcha на том же `119`-м search request;
  - серия дальнейших interleaved repeats показала уже не просто "аномалию", а нестабильный mixed signal:
    - два отдельных clean runs `small interleaved detail budget every 5 -> 1 detail` прошли `120 search + 24 detail` без captcha;
    - immediate back-to-back repeat после одного из них, несмотря на одиночный clean preflight probe, словил первую search-captcha уже на общем request `21`;
    - отдельный repeat после более строгого preflight (`3` consecutive clean low-rate probes с интервалом `10s`) дошёл до общего request `141` до первой captcha, при этом все `24` detail requests остались `200`;
  - controlled accumulation chain подтвердила, что outcome действительно зависит от preceding load и recovery gate:
    - `fresh-after-3x-gate` снова прошёл full clean: `120 search + 24 detail`;
    - `immediate-back-to-back` после этого словил первую search-captcha уже на `102`-м search request;
    - `60s cooldown + 3x clean probes` почти вернул contour к boundary: первая search-captcha только на `118`-м search request;
    - `300s cooldown + 3x clean probes` дал почти тот же результат: первая search-captcha на `119`-м search request;
  - следующая narrow recovery-window study `30s / 60s / 120s` при том же `3x clean low-rate probe` gate показала:
    - `30s` недостаточно: recovery run словил первую search-captcha на `119`-м search request;
    - `60s` в том sample дал clean recovery, хотя seed на том же окне до этого словил captcha на `118`-м search request;
    - `120s` тоже прошёл clean и на seed, и на recovery;
  - follow-up repeat study `60s-a -> 120s-a -> 60s-b` изменила рабочий вывод:
    - оба `60s` recovery (`60s-a` и `60s-b`) не восстановились clean и оба словили первую search-captcha на `118`-м search request;
    - `120s-a` recovery, напротив, снова прошёл clean `120 search + 24 detail`, хотя его seed перед этим словил captcha на `119`-м search request;
  - overnight distributed night-driver study на `4` слотах (`01:44`, `03:44`, `05:44`, `07:44` MSK) дополнительно ослабила этот вывод:
    - все `4/4` слота завершились операционно clean и в каждом seed/recovery gate дошёл до `3` clean low-rate probes;
    - ни один seed или recovery run не прошёл full clean `120 search + 24 detail`;
    - seed останавливался на `117-118` успешных search requests, recovery во всех слотах останавливался на `118`;
    - в `2/4` слотах recovery дал только boundary-level uplift `117 -> 118`, без полного clean reset;
  - дневной distributed matrix-cycle `2026-03-30 10:48 -> 16:56` MSK (`control-short -> aggr-short -> control-long -> aggr-long`) дал ещё более смешанную картину:
    - все `4/4` seed runs прошли clean, включая оба aggressive slots на `workers=4`;
    - `control-short`, `aggr-short` и `control-long` recovery тоже прошли full clean `120 search + 24 detail`;
    - только `aggr-long` recovery после `300s` словил первую search-captcha на `119`-м search request, при этом все `24` detail requests остались `200`;
    - значит, `300s` в этом daytime sample не показал преимущества над `120s`, но из-за order/time-of-day confounding ещё не доказывает, что он хуже;
  - crossover distributed matrix-cycle `2026-03-30 20:00 -> 2026-03-31 02:04` MSK (`control-long -> aggr-long -> control-short -> aggr-short`) закрыл ambiguity по order:
    - оба control recovery (`300s` и `120s`) снова прошли clean;
    - оба aggressive seed runs снова прошли clean;
    - оба aggressive recovery, и `300s`, и `120s`, дали одинаковый outcome: `117` успешных search requests, первая search-captcha на `118`-м search request, при этом все `24` detail requests остались `200`;
    - значит, difference между `120s` и `300s` не воспроизвёлся после инверсии order/time-of-day;
  - значит, на `workers=4` interleaved detail выглядит не как lucky one-off, но и не как stable contour: same-day accumulation реально существует, `3x` clean low-rate probes сами по себе не предсказывают usable aggressive state, а fixed recovery window в диапазоне `120s..300s` не является главным explanatory lever;
- после `search`-captcha `detail` и dictionary endpoints могут быть доступны раньше, чем сам `search`, то есть block domain, вероятно, endpoint-specific;
- `deferred selective detail` после полного list coverage выглядит совместимым с устойчивым search contour и в single-stream, и в conservative batched режиме;
- `small interleaved detail budget` больше не выглядит запретным не только в single-stream, но и в conservative batched contour `workers=3` + `burst_pause=1s`, хотя для более агрессивного burst profile он всё ещё не доказан.

Representative artifacts:

- [historical replay without captcha](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260328T114056Z-cbc1c13a-shape-historical-replay-prefix-130-0s-report.json)
- [burst trigger with captcha around request 119](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260328T121503Z-5141116a-captcha-trigger-workers-4-burst0s-app-like-prefix-130-repeat-20260328-report.json)
- [detail available at T+1s after search-captcha](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260328T122445Z-ebea4025-endpoint-recovery-detail-tplus-1s-131559761-report.json)
- [search still blocked at T+1s after search-captcha](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260328T122445Z-ebea4025-endpoint-recovery-search-tplus-1s-report.json)
- [clean deferred detail 110 + 20](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260328T130358Z-71ee287c-mixed-search-after-coverage-historical-prefix-110-detail20-dual-clean-mixed-summary.json)
- [clean small interleaved detail budget every 5 -> 1](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260328T130358Z-71ee287c-mixed-search-small-detail-budget-historical-prefix-130-every5-max26-dual-clean-mixed-summary.json)
- [batched search-only 120 @ workers=3 burst1s](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T131356Z-4d2934cf-batched-search-only-historical-prefix-120-workers-3-burst1s-dual-control-mixed-summary.json)
- [batched deferred detail 120 + 20 @ workers=3 burst1s](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T131356Z-4d2934cf-batched-mixed-search-after-coverage-historical-prefix-120-detail20-workers-3-burst1s-dual-mixed-summary.json)
- [batched small interleaved detail 120 + every5/max24 @ workers=3 burst1s](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T131356Z-4d2934cf-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-3-burst1s-dual-mixed-summary.json)
- [workers=4 control hit captcha at search 119](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T133354Z-a4ad25e1-batched-search-only-historical-prefix-120-workers-4-burst1s-dual-control-mixed-summary.json)
- [workers=4 deferred rerun hit captcha at search 119](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T133541Z-88af08e6-batched-mixed-search-after-coverage-historical-prefix-120-detail20-workers-4-burst1s-dual-rerun-mixed-summary.json)
- [workers=4 interleaved rerun passed 120 search + 24 detail](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T133541Z-88af08e6-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-rerun-mixed-summary.json)
- [workers=4 interleaved repeat1 passed 120 search + 24 detail](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T140729Z-88f8369d-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-repeat1-mixed-summary.json)
- [workers=4 interleaved repeat2 hit captcha at overall request 21](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T140729Z-88f8369d-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-repeat2-mixed-summary.json)
- [workers=4 interleaved repeat3 after stable preflight hit first captcha at overall request 141](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T140922Z-8640bcef-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-repeat3-stable-preflight-mixed-summary.json)
- [workers=4 accumulation matrix summary](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T141915Z-b6f2ab4f-workers4-interleaved-accumulation-matrix-summary.json)
- [workers=4 fresh-after-3x-gate full clean](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T141915Z-b6f2ab4f-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-fresh-after-3x-gate-mixed-summary.json)
- [workers=4 immediate-back-to-back first search captcha at search 102](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T141915Z-b6f2ab4f-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-immediate-back-to-back-mixed-summary.json)
- [workers=4 cooldown 60s + 3x gate first search captcha at search 118](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T141915Z-b6f2ab4f-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-cooldown-60s-after-3x-gate-mixed-summary.json)
- [workers=4 cooldown 300s + 3x gate first search captcha at search 119](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T141915Z-b6f2ab4f-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-cooldown-300s-after-3x-gate-mixed-summary.json)
- [workers=4 recovery-window study summary](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T145035Z-47064365-workers4-recovery-window-study-summary.json)
- [workers=4 window 30s recovery hit first search captcha at search 119](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T145035Z-47064365-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-window-30s-recovery-mixed-summary.json)
- [workers=4 window 60s recovery clean](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T145035Z-47064365-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-window-60s-recovery-mixed-summary.json)
- [workers=4 window 120s recovery clean](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T145035Z-47064365-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-window-120s-recovery-mixed-summary.json)
- [workers=4 repeat60-vs-120 study summary](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T151918Z-b97a8e0b-workers4-repeat60-vs-120-study-summary.json)
- [workers=4 60s-a recovery hit first search captcha at search 118](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T151918Z-b97a8e0b-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-60s-a-recovery-mixed-summary.json)
- [workers=4 120s-a recovery clean](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T151918Z-b97a8e0b-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-120s-a-recovery-mixed-summary.json)
- [workers=4 60s-b recovery hit first search captcha at search 118](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T151918Z-b97a8e0b-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-60s-b-recovery-mixed-summary.json)
- [workers=4 overnight night-driver summary: 4 slots, no clean recovery](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T224442Z-2fbce3a6-night-driver-summary.json)
- [daytime matrix summary: clean aggr-short, boundary aggr-long](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260330T074845Z-6a59de41-night-driver-summary.json)
- [crossover matrix summary: both aggr-long and aggr-short boundary at 117](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260330T170012Z-3512a4b5-night-driver-summary.json)
- [auth baseline fixed repeat: anonymous clean 8/8, token clean 8/8](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T093948Z-f6977155-auth-baseline-fixed-area-1003-page-0-anonymous-report.json)
- [auth baseline sequential paging: anonymous clean 10/10, token clean 10/10](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T094303Z-6f6e6999-auth-baseline-sequential-area-1003-anonymous-report.json)
- [auth baseline historical replay: anonymous clean 130/130](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T095007Z-df01a6d2-auth-baseline-historical-replay-anonymous-report.json)
- [auth baseline historical replay: token 129x200 + 1 TLS handshake timeout, no captcha](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T095007Z-df01a6d2-auth-baseline-historical-replay-application-token-report.json)
- [auth boundary search-only crossover: anonymous clean 180/180 on run A](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T101914Z-6fae3489-auth-boundary-search-only-workers3-burst1s-cap180-anon-a-report.json)
- [auth boundary search-only crossover: token clean 180/180 on run A](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T101914Z-6fae3489-auth-boundary-search-only-workers3-burst1s-cap180-token-a-report.json)
- [auth boundary search-only crossover: token clean 180/180 on run B](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T101914Z-6fae3489-auth-boundary-search-only-workers3-burst1s-cap180-token-b-report.json)
- [auth boundary search-only crossover: anonymous clean 180/180 on run B](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T101914Z-6fae3489-auth-boundary-search-only-workers3-burst1s-cap180-anon-b-report.json)
- [auth long search-only token run: 2000 requests, no captcha/403, 4 transport resets](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T104007Z-51409330-auth-long-search-only-workers3-burst1s-cap2000-token-report.json)
- [auth detail repeat crossover: anonymous clean 20/20 on run A](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T111214Z-9b24a86a-auth-detail-repeat-5idsx4-anon-a-report.json)
- [auth detail repeat crossover: token clean 20/20 on run A](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T111214Z-9b24a86a-auth-detail-repeat-5idsx4-token-a-report.json)
- [auth bounded mixed crossover: anonymous clean 144/144 on run A](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T111544Z-7dc7d042-auth-bounded-mixed-workers3-burst1s-prefix120-every5-max24-anon-a-mixed-summary.json)
- [auth bounded mixed crossover: token clean 144/144 on run A](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T111544Z-7dc7d042-auth-bounded-mixed-workers3-burst1s-prefix120-every5-max24-token-a-mixed-summary.json)
- [auth bounded mixed crossover: token clean 144/144 on run B](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T111544Z-7dc7d042-auth-bounded-mixed-workers3-burst1s-prefix120-every5-max24-token-b-mixed-summary.json)
- [auth bounded mixed crossover: anonymous 143x200 + 1 TLS handshake timeout on run B, no captcha](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T111544Z-7dc7d042-auth-bounded-mixed-workers3-burst1s-prefix120-every5-max24-anon-b-mixed-summary.json)
- [auth long search-only anonymous control: 2000 requests, no captcha/403, 5 transport errors](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T113508Z-d722726e-auth-long-search-only-workers3-burst1s-cap2000-anonymous-report.json)
- [token trigger discovery: workers=4, burst0, prefix130 completed without captcha but with 5 connection resets](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T123815Z-664095fc-trigger-discovery-search-only-workers4-burst0s-prefix130-token-report.json)

## 3. Provisional policy v0

### 3.1. Search baseline

- Базовый режим для scheduler draft: `search-first`.
- Базовый auth contour для scheduler draft: `application_token`. Причина уже не в measured anti-bot advantage, а в operator choice и официально согласованном app contour. По текущим данным token не показал comparative search/detail advantage над `anonymous`, но и не показал policy-grade regression; `anonymous` остаётся fallback/control contour. Search-side auth baseline уже включает не только мягкие сценарии (`fixed repeat`, `sequential paging`, `historical replay`), но и boundary-seeking crossover `workers=3`, `burst_pause=1s`, `cap=180`, плюс long-volume runs `2000` для token и anonymous без `403/captcha`.
- Базовый header contour для research baseline: `dual`.
- Лучший текущий performance/safety contour для policy draft: conservative batched режим `workers=3` + `burst_pause=1s`.
- `workers=4` и `burst_pause=0` точно не переводить в baseline: именно там уже наблюдался reproducible search-captcha trigger.
- `workers=4` и `burst_pause=1s` тоже пока не переводить в baseline: control и deferred на `search_prefix=120` оба словили captcha на search request `119`, а interleaved detail на том же contour теперь выглядит нестабильно и чувствительно к preceding load между runs.
- Для `application_token` practical load envelope сейчас выглядит ещё консервативнее, чем captcha-only reading:
  - baseline contour `workers=3`, `burst_pause=1s` выдержал `2000` search requests без `403/captcha` и со средним sustained throughput порядка `79 req/min`;
  - exploratory `workers=4`, `burst_pause=0`, `search_prefix=130` тоже не дал captcha, но дал `5` transport resets `connection reset by peer` и `p95 latency ~7.6s`;
  - practical implication: на token-default path elevated pressure сначала проявляется как transport instability, а не как reproducible captcha threshold.
- Для агрессивного contour research/operator gate уже можно формулировать строже: single clean probe недостаточен; `30s` оказалось слишком коротким, `60s` нестабилен между повторениями, а ни `120s`, ни `300s` не выглядят defendable clean recovery rule. Crossover matrix-cycle показал, что difference между `120s` и `300s` исчезает после инверсии order, то есть longer wait сам по себе не выглядит explanatory fix.
- Stop condition для search stage: первый `captcha_required`.
- Для production-like runtime search policy нужно разделять два failure mode:
  - `captcha_required`: остановить новые search requests в текущем run;
  - `transport errors`: ограниченный retry/backoff contour без escalation concurrency.

Confidence: medium-high.

### 3.2. Detail baseline

- Detail не должен быть равноправной частью основного ingestion path.
- Базовый режим detail: `deferred selective detail after full list coverage`.
- Здесь важно уточнение semantics: `selective` не означает "detail только у малой доли вакансий вообще".
- Для research completeness `selective detail` должен означать:
  - обязательный first detail для vacancy, которая впервые увидена или ещё ни разу не получила успешный `detail`;
  - опциональный повторный detail только для `short_changed` или `ttl_refresh`.
- Для первого scheduler policy draft detail должен быть жёстко бюджетирован.
- Практичный стартовый budget для safe baseline: `20` deferred detail requests на run в том же network path; этот contour уже прошёл clean и в single-stream, и в batched `workers=3` + `burst_pause=1s`.
- Но `detail_limit=20` в текущем виде нужно трактовать только как bounded same-run budget, а не как глобальную гарантию `first-detail completeness`.
- При этом сам detail endpoint по свежим long probes не выглядит главным throughput bottleneck:
  - sequential `2000` distinct detail requests под `application_token` прошли без `403/captcha` с observed `~179.9 req/min`;
  - conservative batched contour `workers=3`, `burst_pause=1s`, `1200` distinct detail requests тоже прошёл без `403/captcha` с observed `~119.5 req/min`;
  - practical implication: основной gap теперь не "умеет ли detail endpoint жить долго", а "есть ли у нас persistent first-detail backlog policy".
- `small ongoing detail budget` уже выглядит допустимым bounded contour как минимум до `every 5 searches -> 1 detail` и `max 24 details` на `search_prefix=120` в режиме `workers=3` + `burst_pause=1s`.
- Search/detail auth signal на baseline-safe contours не показывает measured причины предпочесть один auth mode другому по anti-bot поведению: мягкий `detail` crossover прошёл clean у обоих auth modes, bounded mixed crossover тоже прошёл без captcha/403, а long-volume `2000` run прошёл и у token, и у anonymous. Поэтому default здесь задаётся operator policy, а не benchmark gain.
- На `workers=4` есть неожиданный сигнал, что `small interleaved detail budget every 5 -> 1 detail` может даже смягчать search pressure относительно pure search burst, но пока это только один clean run, а не policy-grade fact.
- На `workers=4` signal по `small interleaved detail budget every 5 -> 1 detail` уже сильнее, чем один lucky run: были и clean passes, и near-boundary pass до общего request `141`.
- Но этот же contour остаётся нестабильным: immediate back-to-back repeat после clean run словил captcha уже на общем request `21`, хотя одиночный preflight probe был `200`.
- Controlled chain уточнила это правило:
  - `fresh-after-3x-gate` дал full clean run;
  - `immediate-back-to-back` просел до search `102`;
  - `60s + 3x gate` почти вернул contour к boundary;
  - `300s + 3x gate` не дал явно лучшего результата, чем `60s + 3x gate`, в пределах этого sample.
- Narrow recovery-window study сузила rule ещё сильнее:
  - `30s + 3x gate` всё ещё слишком коротко: recovery словил первую search-captcha на search `119`;
  - в первом sample `60s + 3x gate` дал full clean recovery;
  - `120s + 3x gate` тоже дал full clean recovery.
- Follow-up repeat study изменила приоритет:
  - `60s-a + 3x gate` не восстановился clean;
  - `120s-a + 3x gate` восстановился clean;
  - `60s-b + 3x gate` снова не восстановился clean.
- Overnight night-driver study дополнительно ослабила even `120s`:
  - `4/4` distributed slots завершились без единого clean seed/recovery;
  - recovery в лучшем случае возвращался к boundary `118`, а не к full clean `120`;
  - `3x` clean low-rate probes оставались выполнимыми, но не предсказывали clean aggressive recovery.
- Daytime matrix-cycle уточнил это ещё сильнее:
  - `aggr-short` recovery на `120s` прошёл clean;
  - `aggr-long` recovery на `300s` в том же дне остался boundary-level `118`;
  - control slots при этом были clean, но не в тот же час, поэтому remaining confound по времени дня ещё не снят.
- Crossover matrix-cycle снял этот confound:
  - и `aggr-long`, и `aggr-short` recovery дали один и тот же boundary-level outcome `117`;
  - оба control recovery остались clean;
  - следовательно, fixed recovery window между `120s` и `300s` не объясняет recovery behavior на `workers=4`.
- Отсюда practical implication: single clean preflight probe ещё не доказывает truly reset state для агрессивного contour; repeated low-rate gate выглядит полезнее, но fixed recovery window сам по себе не даёт defendable rule. Для operator-grade policy по оси `120s..300s` вопрос можно считать закрытым.
- Несмотря на это, default detail policy пока лучше оставлять deferred: она проще для reasoning, проще для recovery и лучше соответствует текущей orchestration architecture.
- Для полноты research goal этого уже недостаточно само по себе: нужен persistent backlog contour для vacancies без успешного first detail, иначе policy остаётся search-complete, но не fully detail-complete.

Confidence: medium.

### 3.3. Cooldown baseline

- `search`-captcha нужно трактовать как отдельное событие для search contour, а не как доказанный global block для всех endpoint'ов.
- После первого `search`-captcha scheduler не должен продолжать плотные search retries в том же run.
- До завершения cooldown study безопасный draft-rule такой: остановить новые search requests в текущем run и не делать fast in-run retry loop.
- На token-default contour `Phase C` больше не выглядит главным blocker для `v1`: попытка быстро спровоцировать captcha через `workers=4`, `burst_pause=0`, `search_prefix=130` закончилась не captcha, а full run без `403` с `5` transport resets. Это ослабляет смысл новых агрессивных captcha-seeking repeat-ов и поднимает приоритет transport-error policy.
- Для агрессивного research contour provisional operator rule уже выглядит так:
  - не доверять одному clean probe после heavy run;
  - перед новым aggressive run лучше требовать несколько clean low-rate probes;
  - `30s` окно в текущем sample слишком короткое;
  - `60s` окно пока нестабильно: в follow-up repeat study оба recovery на нём остались boundary-level;
  - `120s` после overnight distributed study уже не выглядит достаточным clean recovery gate: оно может вернуть contour к boundary, но не доказало устойчивый reset;
  - `300s` после forward-order и crossover matrix-cycle тоже не показал устойчивого преимущества над `120s`;
  - следующий meaningful step теперь уже не новый cooldown/window repeat, а переход к другим levers или закрытие `workers=4` как non-v1 contour.
- Для unattended режима provisional retry contour всё ещё консервативный: не раньше следующего regular scheduler tick, одним low-rate probe, без burst/concurrency escalation.
- Для transport errors provisional operator rule теперь нужно формулировать отдельно:
  - единичные `timeout/reset/network unreachable` не трактовать как captcha;
  - не повышать workers/burst после transport noise;
  - ограничивать retry budget внутри run и выносить остальное на следующий scheduler tick.
- `detail` и dictionary during search cooldown пока не переводить в production policy автоматически; это остаётся исследовательским операторским контуром до закрытия WS2.

Confidence: low-to-medium.

### 3.4. Network path baseline

- Не использовать proxy rotation как baseline design.
- Не менять сеть одновременно с pacing/header/concurrency.
- Рассматривать второй network path только как fallback lever после завершения baseline/cooldown/mixed workload decision.

Confidence: high.

## 4. Рекомендуемый current mode

На текущий момент strongest candidate для первого реального scheduler policy draft:

- `Mode B. Search-First With Deferred Selective Detail`

Почему:

- он лучше всего совпадает с текущей архитектурой orchestration;
- он лучше всего совпадает с текущими mixed workload results;
- он сохраняет максимальный приоритет за search coverage;
- он позволяет отдельно бюджетировать и чинить detail contour, не разрушая основной search path.

`Mode C. Search Plus Small Ongoing Detail Budget` больше не выглядит только single-stream гипотезой: у него уже есть clean evidence и на conservative batched contour. Но как default policy он всё ещё уступает `Mode B` по простоте и defendability.

Дополнительный нюанс:

- на `workers=4`, `burst_pause=1s` `Mode C` неожиданно показал лучший outcome, чем `Mode A/B` на одном clean rerun;
- несколько следующих repeats и controlled accumulation chain подтвердили, что `Mode C` на этом contour может доходить существенно дальше control, но outcome остаётся нестабильным между runs;
- это делает `Mode C` более интересной research branch, но не делает его default policy без controlled accumulation/cooldown study.

## 5. Что пока нельзя считать зафиксированным

Пока рано фиксировать в production policy:

- точный minimal cooldown для `search` после captcha;
- допустим ли `small ongoing detail budget` при более агрессивном burst/concurrency, например `workers=4`, меньшем `burst_pause` или более плотном detail ratio;
- насколько observed `workers=4` interleaved uplift является реальным pressure-dilution effect, а насколько он зависит от preceding load, same-day accumulation и неполного reset между runs;
- где именно проходит minimal useful recovery window для этого contour: ближе к `120s`, между `60s` и `120s`, или заметно выше;
- может ли `detail` сам по себе словить captcha в отдельном burst profile;
- даёт ли `application token` operational advantage;
- нужен ли второй network path как обязательный fallback.

## 6. What "optimal" means here

Для нас "optimal policy" должна оптимизировать не одну метрику, а набор trade-offs:

1. Максимум стабильных `search`-запросов до первой captcha.
2. Минимум scheduler/operator intervention.
3. Сохранение полезных detail fields без разрушения search contour.
4. Предсказуемое recovery behavior после captcha.
5. Минимум инфраструктурной и юридико-операционной сложности.

Отсюда практический вывод:

- policy с чуть меньшим total throughput, но без burst spikes и с честным `search-first` приоритетом для нас лучше, чем более агрессивный режим, который иногда даёт больше данных, но чаще уходит в captcha.

## 7. Что нужно сделать, чтобы поднять policy до v1

1. Для `workers=4`, `burst_pause=1s`, `every 5 -> 1 detail` больше не тратить ночные слоты на cooldown/window repeat в диапазоне `120s..300s`. Эта ось дала достаточно сигнала: aggressive recovery остаётся нестабильным, а window length здесь не выглядит главным lever.
2. Для `policy v1` считать `workers=4` research-only contour, а не candidate baseline.
3. Зафиксировать отдельный transport-error policy для scheduler/runtime:
   - классификация `timeout/reset/network unreachable` отдельно от `captcha_required`;
   - ограниченный retry/backoff budget;
   - без concurrency escalation после transport noise.
4. `study-detail-payloads` уже обновлён на свежем crawl run:
   - [summary.md](/home/yurizinyakov/projects/hh_collector/.state/reports/detail-payload-study/20260331T134110Z/summary.md)
   - sample `10/10`, raw drift `0/20`, normalized drift `0/20`;
   - detail-only research fields: `address.can_edit`, `description`, `key_skills[].name`, `branded_description`.
5. `Phase C. Cooldown Study` на окнах `5m`, `15m`, `30m`, `60m`, `120m`, `next day` теперь уже не блокирует `v1`, но остаётся полезным follow-up именно для endpoint-specific recovery runbook. Запускать его лучше через [hh-api-probe-cooldown-driver.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-probe-cooldown-driver.md), а не вручную.
6. Проверить penalty accumulation, если captcha случается несколько раз в течение одного дня.
7. Auth baseline на search/detail/mixed baseline contours больше не блокирует policy:
   - мягкий `detail` crossover clean у `anonymous` и `application_token`;
   - bounded mixed crossover clean у обоих auth modes, кроме одного transport timeout на anonymous run B;
   - long-volume `2000` control теперь тоже есть у token и anonymous, оба без captcha/403;
   - сравнительный auth question на baseline contour можно считать достаточно закрытым для `v1`.
8. Pure-search auth question считать низкоприоритетным для новых repeat-ов: при текущем baseline contour уже есть clean crossover до `180` запросов без различия по captcha, long-volume `2000` runs у обоих auth modes без captcha и clean/near-clean detail+mixed auth crossover. Возвращаться к нему только если появится новый risk signal:
   - более длинный real scheduler slot;
   - production-like same-day accumulation;
   - явный transport/auth-specific failure pattern у любого режима.
9. Для `workers=4`, `burst_pause=1s`, `every 5 -> 1 detail` уже можно считать доказанным, что preflight policy имеет значение; теперь имеет смысл возвращаться к нему только если появится новый lever, а не новый repeat по тому же окну. Возможные будущие levers:
   - одиночный clean probe;
   - несколько clean probes подряд;
   - другой auth contour;
   - другой network path;
   - другой detail ratio;
   - immediate back-to-back repeat в том же дне.
10. После этого на `workers=4` менять только один lever за раз:
   - либо уменьшать `burst_pause`;
   - либо повышать плотность interleaved detail;
   - либо сравнивать другой `search_prefix`.
11. Только после этого решать, нужно ли переводить `Mode C` из research hypothesis в allowed production contour для более агрессивных contours.

## 8. Текущий практический вывод

Если бы нужно было выбрать рабочую policy уже сейчас, без ожидания новых прогонов, она была бы такой:

- `search-first`;
- `application_token + dual`;
- conservative batched search baseline: `workers=3`, `burst_pause=1s`;
- no deliberate burst/concurrency escalation;
- stop search on first captcha;
- classify `timeout/reset/network unreachable` separately from captcha and handle them via bounded retry/backoff;
- retry search not earlier than next regular tick;
- selective deferred detail only after list coverage;
- conservative deferred detail budget;
- no proxy / no second network path by default;
- `anonymous` оставлять как fallback/control contour, а не основной default.

Это не обязательно финальная optimal policy, но это лучший defendable baseline из текущих данных.
