# HH API Collection Strategy Research Plan

Статус: in progress  
Дата: 2026-03-28

## 1. Контекст

Нам нужен практичный и воспроизводимый plan исследований, который поможет выбрать устойчивую стратегию сбора данных из HH API так, чтобы:

- captcha не ломала основной search contour;
- сбор не требовал постоянного ручного вмешательства;
- система могла деградировать мягко;
- мы понимали, насколько критичен `detail` по сравнению с `search-only`.

На 2026-03-28 уже известно следующее:

- в soak run `2026-03-22` первый `403` по `GET /vacancies` появился после `119` успешных vacancy-search запросов;
- Phase A закрыта: probe notebook и helper теперь пишут сравнимые `jsonl` и `*-report.json` со сводкой по `search`, `detail`, `dictionary`, `header_mode`, `auth_mode`, `workers`, `pause_seconds`, `request_index_from_run_start`, `seconds_since_previous_request`, `minutes_since_first_captcha`;
- мягкие fixed-repeat запросы в probe-ноутбуке проходили без captcha для `app_like`, `hh_only`, `dual` и для pacing `0.5s-5s`;
- sequential paging, round-robin по `area` и historical replay `130` запросов в исходном порядке прошли без captcha;
- burst replay без inter-batch pause иногда приводит к `captcha_required`, но порог плавающий и не выглядит полностью детерминированным;
- `burst_pause=1s` в протестированных коротких burst-сценариях заметно снижал риск captcha по сравнению с `burst_pause=0`;
- после `search`-captcha `detail` и dictionary endpoints в нескольких независимых recovery-прогонах были доступны раньше, чем `search`, то есть блок не выглядит как жёсткий global API lock;
- текущий production client использует только `User-Agent`, а probe-ноутбук умеет тестировать `User-Agent`, `HH-User-Agent` и `dual`;
- официальный auth contour HH поддерживает как минимум application token и user token; application token уже получен, и первые три auth benchmark runs для search baseline завершены, но auth baseline для detail/mixed contour ещё не закрыт.

## 2. Что считаем успехом

Итогом этого research должен стать не "способ обхода captcha", а операционная стратегия со следующими свойствами:

- есть baseline search profile, который стабильно проходит без частых captcha;
- есть recovery policy при `captcha_required`;
- есть решение, обязателен ли `detail` для наших задач и в каком объёме;
- есть понятные stop/go criteria для возможных экспериментов с другой сетью или proxy;
- scheduler можно настроить под найденный envelope без ежедневной ручной рутины.

## 3. Ограничения и guardrails

- Не считать внешние статьи источником истинных лимитов HH.
- Не закладывать в baseline стратегию автоматическое решение captcha.
- Не смешивать сразу много факторов в одном эксперименте.
- Каждый сценарий должен иметь единый stop condition: первый `captcha_required`.
- Любой рискованный эксперимент с другой сетью или proxy делать только после baseline и cooldown study.

Отдельный юридико-операционный риск:

- официальные условия HH API допускают ограничение отдельных функций и приостановку API-ключа;
- также условия содержат ограничения на формирование другой БД для передачи данных третьим лицам.

Это не блокирует локальный research plan, но должно учитываться до любого масштабирования.

## 4. Что взять из внешней статьи про proxy

Статья ProxyCove полезна не как источник фактов про HH, а как набор гипотез для тестирования:

- антибот может реагировать на IP reputation;
- важны request pacing, session consistency и отсутствие burst-паттерна;
- прокси увеличивают стоимость и усложняют отладку;
- residential/mobile сети часто ведут себя иначе, чем типичный datacenter IP.

Что из этого принимаем в research:

- network path действительно надо рассматривать как фактор;
- но proxy rotation не должна быть первым шагом;
- сначала нужно понять, какой вклад в captcha дают темп, конкурентность, форма запросов и заголовки на одном стабильном IP.

## 5. Ключевые research questions

1. Какой набор заголовков и какой request profile дают самый устойчивый `GET /vacancies`?
2. Даёт ли application token практическое преимущество для vacancy search/detail по сравнению с anonymous mode?
3. Что сильнее влияет на captcha: скорость, конкурентность, deep paging, fan-out по `area` или сеть/IP?
4. После первой captcha блок stateful или краткоживущий, и каков practical cooldown без ручного решения?
5. Восстанавливается ли доступ после manual captcha solve и на какое окно?
6. Можно ли строить пригодный long-term dataset в режиме `search-only`?
7. Если `detail` нужен, то какой policy минимизирует API pressure: immediate, deferred, selective by delta, capped daily budget?
8. Есть ли смысл рассматривать второй network path или proxy как fallback, а не как primary design?

## 6. Workstreams

### WS1. Search Anti-Bot Envelope

Цель:

- найти безопасный operational envelope для `GET /vacancies`.

Факторы:

- auth mode: `anonymous`, `application_token`;
- header mode: `user_agent_only`, `hh_user_agent_only`, `dual`;
- concurrency: `1`, `2`, `4`;
- pacing: `0.25s`, `0.5s`, `1s`, `2s`, `5s`;
- workload shape:
  - fixed request repeat;
  - sequential deep paging in one `area`;
  - round-robin across multiple `area`;
  - replay historical soak sequence.

Основные метрики:

- `requests_until_first_captcha`;
- `wall_clock_until_first_captcha`;
- `status_code_counts`;
- `latency_p50/p95`;
- `request shape` at the transition;
- `error_type` и `error_value`.

Decision rule:

- baseline search profile должен в нескольких повторах давать либо `0 captcha`, либо существенно лучший запас до первой captcha, чем альтернативы.

### WS1A. Auth State Benchmark

Цель:

- проверить, не пропускаем ли мы очевидный supported path через application token.

Что известно из источников:

- официальная документация HH описывает OAuth и application token;
- applicant auth официально добавляет к vacancy payload дополнительные поля вроде `relations`, `negotiations_url`, `suitable_resumes_url`;
- при этом в open-source клиентах для HH vacancy search часто поддерживается anonymous mode, а access token описывается как optional для protected endpoints.

Гипотеза:

- application token может не снимать captcha вовсе;
- но его всё равно надо проверить, потому что это supported official mechanism, а не workaround.

Эксперименты:

- `anonymous` vs `application_token` на одинаковом `fixed request repeat`;
- `anonymous` vs `application_token` на одинаковом `sequential paging`;
- `anonymous` vs `application_token` на одинаковом `historical replay`;
- отдельно сравнить `GET /vacancies` и `GET /vacancies/{id}`.

Основные метрики:

- `requests_until_first_captcha`;
- `status_code_counts`;
- `latency_p50/p95`;
- наличие различий в payload shape;
- наличие дополнительных полей, если будут.

Decision rule:

- если `application_token` не даёт заметного operational выигрыша, не усложнять baseline auth contour;
- если даёт стабильный прирост, включить его в основной search/detail benchmark matrix.

### WS2. Cooldown and Recovery Semantics

Цель:

- понять, что делать после первого `captcha_required`.

Сценарий:

- выбрать reproducible trigger workload, который достаточно быстро приводит к captcha;
- после первого `captcha_required` прекратить burst и делать одиночные low-rate probes через интервалы:
  - `5m`
  - `15m`
  - `30m`
  - `60m`
  - `120m`
  - `overnight`

Отдельные вопросы:

- проходит ли тот же самый `GET /vacancies` позже без manual solve;
- влияет ли captcha на `detail` endpoint;
- влияет ли captcha на dictionary endpoints;
- помогает ли manual solve по документированному `captcha_url`.

Основные метрики:

- `time_to_first_success_after_captcha`;
- `same_request_recovery_status`;
- `detail_endpoint_status_during_search_captcha`;
- `dictionary_endpoint_status_during_search_captcha`.

Decision rule:

- у scheduler должен появиться конкретный cooldown policy, а не абстрактный backoff.

### WS3. Search vs Detail Value Study

Цель:

- понять, можно ли жить в режиме `search-first` или даже `search-only`.

Что уже видно по коду:

- `search` в нормализованном слое уже несёт `id`, `name`, `area`, `published_at`, `created_at`, `alternate_url`, `employment`, `schedule`, `experience`, `employer`, `professional_roles`;
- `detail` добавляет как минимум `description`, `branded_description`, `archived`, `key_skills`, `salary`, `salary_range`;
- short snapshot хранит raw item из search, так что search-only фактически богаче, чем текущая нормализация DTO.

Эксперименты:

- использовать existing CLI `study-detail-payloads`;
- на выборке вакансий сравнить:
  - raw search item;
  - normalized short record;
  - detail payload;
  - repeated detail fetches во времени.

Основные метрики:

- какие поля есть только в detail;
- как часто эти поля реально нужны для downstream research;
- насколько стабилен detail payload при повторных fetch;
- как часто search short payload уже содержит достаточно данных без detail.

Decision rule:

- либо подтверждаем `search-only viable for baseline dataset`;
- либо фиксируем минимальный selective detail policy.

### WS4. Mixed Workload Interaction

Цель:

- проверить, мешает ли detail search contour.

Сценарии:

- `search-only`;
- `search + immediate detail`;
- `search + deferred detail after list coverage`;
- `search + capped detail budget`.

Основные метрики:

- captcha incidence по search;
- captcha incidence по detail;
- throughput;
- coverage;
- operator intervention count.

Decision rule:

- detail не должен ухудшать устойчивость search contour до неприемлемого уровня.

### WS5. Network Path Variation

Цель:

- проверить, насколько результат зависит от IP / network path.

Порядок:

- только после WS1 и WS2;
- сначала сравнить два стабильных network path без ротации;
- только если это действительно нужно, сделать один отдельный scoped proxy experiment.

Что не делать:

- не строить baseline сразу на proxy rotation;
- не смешивать network change одновременно с новым pacing и новым header mode.

Decision rule:

- network path variation рассматривается как fallback lever, а не как primary architecture choice.

## 7. Приоритетный backlog экспериментов

### Progress Snapshot On 2026-03-28

Что уже закрыто:

- `Phase A. Instrumentation Cleanup` завершена.
- `Phase B. Minimal Baseline Matrix` частично завершена:
  - header baseline на мягком fixed-repeat не показал operational разницы между `app_like`, `hh_only`, `dual`;
  - pacing baseline `0.5s`, `1s`, `2s`, `5s` на fixed-repeat не вызвал captcha;
  - shape baseline (`sequential paging`, `round-robin`, `historical replay`) не вызвал captcha;
  - burst/concurrency baseline показал, что риск сидит не в самом факте длинной последовательности запросов, а в burst-форме и конкурентности без паузы между batch windows.
- `Phase C. Cooldown Study` частично завершена:
  - есть reproducible trigger workload: historical replay prefix `130`, `workers=4`, `burst_pause=0`, `header_mode=app_like`;
  - в успешных trigger runs первая captcha приходила примерно на `119-120` запросе;
  - после search-captcha `detail` и `dictionary` были доступны уже в окнах `T+1s`, `T+3s`, `T+5s`, а `search` в тех же окнах всё ещё возвращал `captcha_required`;
  - в отдельном более позднем recovery-run `search` уже восстанавливался примерно к `T+9s`.
- `Phase D. Detail Value Study` пока не обновлялась на свежем crawl run, но предыдущий отчёт уже поддерживает `search-first` и `selective detail`, а не exhaustive detail.
- `Phase E. Mixed Workload Study` запущена и уже дала первый пакет наблюдений:
  - control `search-only` на historical prefix `130` в `anonymous + dual` словил первую search-captcha на `120`-м search-запросе;
  - отдельный rerun `search + detail after coverage` на том же prefix `130` словил первую search-captcha на том же `120`-м search-запросе, то есть deferred detail не ухудшает pre-detail search envelope, но сам detail phase в этом варианте не стартует;
  - exploratory run `search + small detail budget` с `130` search requests и `13` interleaved detail requests прошёл полностью без captcha, но этот конкретный run стартовал вскоре после предыдущего captcha episode;
  - clean independent rerun `search + small detail budget` дал `118` успешных search, `11` успешных detail и первую search-captcha только на общем request `130`, то есть small interleaved detail budget не выглядит катастрофическим для search contour;
  - clean independent `search + detail after coverage` на safe prefix `80 + 20 detail` прошёл полностью без captcha;
  - дальнейшая clean escalation `100 + 20 detail` и `110 + 20 detail` тоже прошла полностью без captcha, то есть deferred detail уже выглядит устойчивым не только на умеренном, но и на достаточно длинном single-stream search prefix;
  - clean independent `search + small detail budget` в более плотном режиме `every 5 searches -> 1 detail` дал `119` успешных search, `23` успешных detail и первую search-captcha только на общем request `143`, то есть even denser interleaved detail budget пока не выглядит главным trigger factor в single-stream режиме.
  - первый ограниченный batched mixed scenario на conservative contour (`workers=3`, `burst_pause=1s`, `search_prefix=80`, `every 10 searches -> 1 detail`) не дал captcha ни в search-only control, ни в mixed run;
  - в этом batched mixed run все `80` search дали `200`, а `detail` дал `7x200 + 1x404 not_found`, что выглядит как stale vacancy artifact, а не как anti-bot signal.
  - новый batched tranche `2026-03-29` на том же conservative contour (`workers=3`, `burst_pause=1s`) прошёл clean для `search_prefix=100`:
    - `search-only control`: `100x200`;
    - `search + detail after coverage`: `100 search + 20 detail`, всё `200`;
    - `search + small detail budget every 5 -> 1`: `100 search + 20 detail`, всё `200`.
  - следующий batched tranche `2026-03-29` прошёл clean и для `search_prefix=120`:
    - `search-only control`: `120x200`;
    - `search + detail after coverage`: `120 search + 20 detail`, всё `200`;
    - `search + small detail budget every 5 -> 1`: `120 search + 24 detail`, всё `200`.
  - на этих двух batched tranches не видно признака, что mixed workload сдвигает search threshold вниз относительно control на contour `workers=3`, `burst_pause=1s`;
  - search latency на mixed runs не показывает системного collapse относительно control, хотя у batched deferred detail `120 + 20` виден один более тяжёлый p95 tail, который нужно перепроверить повторами.
  - отдельная эскалация `2026-03-29` на `workers=4`, `burst_pause=1s`, `search_prefix=120` дала уже более сложную картину:
    - `search-only control` словил первую search-captcha на `119`-м search request;
    - clean rerun `search + detail after coverage` словил первую search-captcha на том же `119`-м search request, то есть deferred detail не ухудшает pre-detail threshold, но и не расширяет его;
    - immediate mixed reruns, запущенные сразу после control captcha episode, были contaminated active-captcha state и не должны использоваться как сравнение;
    - после deferred rerun одиночный recovery probe вернулся в `200` уже на втором low-rate probe примерно через `~10s`;
    - clean rerun `search + small detail budget every 5 -> 1 detail` неожиданно прошёл полностью: `120 search + 24 detail`, всё `200`;
    - следующий clean repeat того же interleaved contour тоже прошёл полностью: `120 search + 24 detail`, всё `200`;
    - immediate back-to-back repeat после этого, несмотря на одиночный clean preflight probe, словил первую search-captcha уже на общем request `21`;
    - отдельный repeat после более строгого preflight (`3` consecutive clean low-rate probes с интервалом `10s`) дошёл до общего request `141` до первой captcha, при этом все `24` detail requests остались `200`;
    - controlled accumulation chain `2026-03-29` уточнила shape этого эффекта:
      - `fresh-after-3x-gate`: full clean `120 search + 24 detail`, всё `200`;
      - `immediate-back-to-back`: первая search-captcha уже на `102`-м search request;
      - `60s cooldown + 3x clean probes`: первая search-captcha только на `118`-м search request, все `24` detail — `200`;
      - `300s cooldown + 3x clean probes`: практически тот же результат, первая search-captcha на `119`-м search request, все `24` detail — `200`;
    - это уже не похоже на lucky one-off: interleaved detail на более агрессивном batched contour действительно может существенно менять search envelope, но outcome остаётся нестабильным и зависит не только от самого workload, но и от recovery gate между runs.

Что пока заблокировано или не доказано:

- `Phase B / auth baseline` заблокирована до получения application token;
- не доказано, что `detail` сам по себе не может словить captcha при отдельном burst-профиле;
- не доказано, что `search` всегда восстанавливается в одно и то же короткое окно после captcha;
- не проверено, накапливается ли penalty при серии captcha в течение одного дня;
- mixed workload в clean single-stream режиме уже неплохо прояснился, а conservative batched contour `workers=3`, `burst_pause=1s` теперь подтверждён не только на `search_prefix=80`, но и на `100` и `120`; следующий вопрос уже не в том, жив ли этот contour вообще, а где именно у него граница;
- на `workers=4`, `burst_pause=1s` картина уже не сводится к вопросу "lucky run или нет": interleaved contour дал несколько сильных runs, но остаётся нестабильным между repeats; теперь главный вопрос в том, насколько outcome зависит от recovery gate и cross-run accumulation;
- controlled accumulation chain уже показала, что `60s + 3x clean probes` почти восстанавливает boundary, а `300s + 3x clean probes` в этом sample не даёт явного дополнительного выигрыша; теперь нужно не расширять окна вверх, а точнее зажать minimal useful window снизу;
- deferred detail теперь подтверждён для `search_prefix=80/100/110` в single-stream и для `100/120` на conservative batched contour, но ещё не доведён до real failure boundary;
- small interleaved detail budget теперь подтверждён для `every 10 searches -> 1 detail` и `every 5 searches -> 1 detail` в single-stream режиме и для `every 10` и `every 5` на conservative batched contour, но не проверен для ещё более плотного ratio или для более агрессивного burst profile.

Рабочий operational вывод на текущий момент:

- baseline надо строить вокруг `search-first`, а не вокруг равноправного смешивания `search` и `detail`;
- cooldown, скорее всего, должен быть endpoint-specific: короткий для `search`, более мягкий или отдельный для `detail` и `dictionary`;
- burst/concurrency сейчас выглядит более опасным фактором, чем сам по себе длинный search-only contour;
- при этом contour `workers=3`, `burst_pause=1s` больше не выглядит merely exploratory: он уже выглядит как реальный candidate baseline, а не как одноразовый lucky run;
- `Mode B. Search-First With Deferred Selective Detail` сейчас выглядит как strongest candidate for first scheduler policy draft;
- `Mode C. Search Plus Small Ongoing Detail Budget` тоже остаётся живой гипотезой уже не только для single-stream режима: batched `workers=3`, `burst_pause=1s` с `every 5 -> 1 detail` тоже пока не дал явного collapse search stability;
- на conservative batched contour small interleaved detail budget уже выглядит допустимым bounded contour;
- на более агрессивном contour `workers=4`, `burst_pause=1s` interleaved detail уже не выглядит просто отдельной anomaly: были и full clean passes, и near-boundary pass до общего request `141`;
- но тот же contour остаётся нестабильным: single clean preflight probe не гарантирует truly reset state, а immediate back-to-back repeat может падать очень рано;
- controlled accumulation chain уточнила следующий decision question:
  - `3x` clean low-rate probe gate реально что-то значит;
  - `60s` recovery window уже выглядит operationally meaningful;
  - `300s` в текущем sample не даёт явного прироста над `60s`;
- narrow recovery-window study `30s / 60s / 120s` уточнила этот вывод:
  - `30s + 3x gate` недостаточно: recovery run словил первую search-captcha на `119`-м search request;
  - в первом sample `60s + 3x gate` дал clean recovery run `120 search + 24 detail`;
  - `120s + 3x gate` тоже дал clean recovery;
- follow-up repeat study `60s-a -> 120s-a -> 60s-b` изменила рабочую картину:
  - оба `60s` recovery остались boundary-level и словили первую search-captcha на `118`-м search request;
  - `120s-a` recovery снова прошёл clean `120 search + 24 detail`;
- overnight distributed night-driver study на `4` слотах (`01:44`, `03:44`, `05:44`, `07:44` MSK) изменила рабочую картину ещё сильнее:
  - все `4/4` слота завершились операционно clean;
  - ни один seed или recovery run не прошёл full clean `120 search + 24 detail`;
  - seed оставался в диапазоне `117-118` search OK, recovery во всех слотах доходил только до `118`;
  - в `2/4` слотах recovery был лучше seed лишь на `+1` search request, то есть дал boundary-level uplift, но не full reset;
- дневной distributed matrix-cycle `2026-03-30 10:48 -> 16:56` MSK (`control-short -> aggr-short -> control-long -> aggr-long`) добавил важный counter-signal:
  - все `4/4` seed runs, включая оба aggressive slots, прошли clean;
  - `control-short`, `aggr-short` и `control-long` recovery тоже прошли clean;
  - только `aggr-long` recovery после `300s` словил первую search-captcha на `119`-м search request;
  - следовательно, `300s` в этом sample не показал преимущества над `120s`, но sample остаётся order-confounded по времени дня;
- crossover distributed matrix-cycle `2026-03-30 20:00 -> 2026-03-31 02:04` MSK (`control-long -> aggr-long -> control-short -> aggr-short`) закрыл этот confound:
  - оба control recovery снова прошли clean;
  - оба aggressive seed runs снова прошли clean;
  - оба aggressive recovery, и `300s`, и `120s`, дали один и тот же outcome: `117` successful search requests, первая search-captcha на `118`-м search request;
  - detail path в обоих aggressive recovery остался clean `24/24`;
- значит, decision question по оси `120s vs 300s` можно считать закрытым: observed difference не воспроизводится после инверсии order/time-of-day, а window length в диапазоне `120s..300s` не выглядит главным explanatory lever;
- пока нет данных за auth advantage, anonymous mode остаётся базовым режимом для экспериментов.

Representative artifacts:

- historical replay без captcha: [20260328T114056Z-cbc1c13a-shape-historical-replay-prefix-130-0s-report.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260328T114056Z-cbc1c13a-shape-historical-replay-prefix-130-0s-report.json)
- burst-trigger с captcha: [20260328T121503Z-5141116a-captcha-trigger-workers-4-burst0s-app-like-prefix-130-repeat-20260328-report.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260328T121503Z-5141116a-captcha-trigger-workers-4-burst0s-app-like-prefix-130-repeat-20260328-report.json)
- post-captcha `detail` at `T+1s`: [20260328T122445Z-ebea4025-endpoint-recovery-detail-tplus-1s-131559761-report.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260328T122445Z-ebea4025-endpoint-recovery-detail-tplus-1s-131559761-report.json)
- post-captcha `search` at `T+1s`: [20260328T122445Z-ebea4025-endpoint-recovery-search-tplus-1s-report.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260328T122445Z-ebea4025-endpoint-recovery-search-tplus-1s-report.json)
- post-captcha `search` recovered around `T+9s`: [20260328T122017Z-92672b4f-post-captcha-early-search-tplus-5s-report.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260328T122017Z-92672b4f-post-captcha-early-search-tplus-5s-report.json)
- clean deferred detail `110 + 20`: [20260328T130358Z-71ee287c-mixed-search-after-coverage-historical-prefix-110-detail20-dual-clean-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260328T130358Z-71ee287c-mixed-search-after-coverage-historical-prefix-110-detail20-dual-clean-mixed-summary.json)
- clean small interleaved detail `every 5 -> 1 detail`: [20260328T130358Z-71ee287c-mixed-search-small-detail-budget-historical-prefix-130-every5-max26-dual-clean-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260328T130358Z-71ee287c-mixed-search-small-detail-budget-historical-prefix-130-every5-max26-dual-clean-mixed-summary.json)
- batched search-only control `80 @ workers=3 burst1s`: [20260328T132137Z-95399482-batched-search-only-historical-prefix-80-workers-3-burst1s-dual-control-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260328T132137Z-95399482-batched-search-only-historical-prefix-80-workers-3-burst1s-dual-control-mixed-summary.json)
- batched mixed `80 + every10/max8 detail @ workers=3 burst1s`: [20260328T132137Z-95399482-batched-mixed-search-small-detail-budget-historical-prefix-80-every10-max8-workers-3-burst1s-dual-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260328T132137Z-95399482-batched-mixed-search-small-detail-budget-historical-prefix-80-every10-max8-workers-3-burst1s-dual-mixed-summary.json)
- batched search-only control `120 @ workers=3 burst1s`: [20260329T131356Z-4d2934cf-batched-search-only-historical-prefix-120-workers-3-burst1s-dual-control-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T131356Z-4d2934cf-batched-search-only-historical-prefix-120-workers-3-burst1s-dual-control-mixed-summary.json)
- batched deferred detail `120 + 20 @ workers=3 burst1s`: [20260329T131356Z-4d2934cf-batched-mixed-search-after-coverage-historical-prefix-120-detail20-workers-3-burst1s-dual-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T131356Z-4d2934cf-batched-mixed-search-after-coverage-historical-prefix-120-detail20-workers-3-burst1s-dual-mixed-summary.json)
- batched small interleaved detail `120 + every5/max24 @ workers=3 burst1s`: [20260329T131356Z-4d2934cf-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-3-burst1s-dual-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T131356Z-4d2934cf-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-3-burst1s-dual-mixed-summary.json)
- workers=4 control `120 @ burst1s` with first search captcha at `119`: [20260329T133354Z-a4ad25e1-batched-search-only-historical-prefix-120-workers-4-burst1s-dual-control-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T133354Z-a4ad25e1-batched-search-only-historical-prefix-120-workers-4-burst1s-dual-control-mixed-summary.json)
- workers=4 deferred rerun `120 + 20 @ burst1s` with first search captcha at `119`: [20260329T133541Z-88af08e6-batched-mixed-search-after-coverage-historical-prefix-120-detail20-workers-4-burst1s-dual-rerun-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T133541Z-88af08e6-batched-mixed-search-after-coverage-historical-prefix-120-detail20-workers-4-burst1s-dual-rerun-mixed-summary.json)
- workers=4 interleaved rerun `120 + every5/max24 @ burst1s` clean: [20260329T133541Z-88af08e6-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-rerun-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T133541Z-88af08e6-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-rerun-mixed-summary.json)
- workers=4 interleaved repeat1 `120 + every5/max24 @ burst1s` clean: [20260329T140729Z-88f8369d-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-repeat1-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T140729Z-88f8369d-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-repeat1-mixed-summary.json)
- workers=4 interleaved repeat2 with single clean preflight but first captcha at overall request `21`: [20260329T140729Z-88f8369d-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-repeat2-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T140729Z-88f8369d-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-repeat2-mixed-summary.json)
- workers=4 interleaved repeat3 after `3x` clean low-rate probes, first captcha at overall request `141`: [20260329T140922Z-8640bcef-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-repeat3-stable-preflight-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T140922Z-8640bcef-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-repeat3-stable-preflight-mixed-summary.json)
- workers=4 accumulation matrix summary: [20260329T141915Z-b6f2ab4f-workers4-interleaved-accumulation-matrix-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T141915Z-b6f2ab4f-workers4-interleaved-accumulation-matrix-summary.json)
- workers=4 fresh-after-3x-gate full clean: [20260329T141915Z-b6f2ab4f-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-fresh-after-3x-gate-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T141915Z-b6f2ab4f-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-fresh-after-3x-gate-mixed-summary.json)
- workers=4 immediate-back-to-back first search captcha at search `102`: [20260329T141915Z-b6f2ab4f-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-immediate-back-to-back-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T141915Z-b6f2ab4f-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-immediate-back-to-back-mixed-summary.json)
- workers=4 cooldown `60s + 3x gate`, first search captcha at search `118`: [20260329T141915Z-b6f2ab4f-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-cooldown-60s-after-3x-gate-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T141915Z-b6f2ab4f-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-cooldown-60s-after-3x-gate-mixed-summary.json)
- workers=4 cooldown `300s + 3x gate`, first search captcha at search `119`: [20260329T141915Z-b6f2ab4f-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-cooldown-300s-after-3x-gate-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T141915Z-b6f2ab4f-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-cooldown-300s-after-3x-gate-mixed-summary.json)
- workers=4 recovery-window study summary `30s / 60s / 120s`: [20260329T145035Z-47064365-workers4-recovery-window-study-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T145035Z-47064365-workers4-recovery-window-study-summary.json)
- workers=4 `30s + 3x gate` recovery with first search captcha at search `119`: [20260329T145035Z-47064365-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-window-30s-recovery-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T145035Z-47064365-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-window-30s-recovery-mixed-summary.json)
- workers=4 `60s + 3x gate` recovery clean: [20260329T145035Z-47064365-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-window-60s-recovery-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T145035Z-47064365-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-window-60s-recovery-mixed-summary.json)
- workers=4 `120s + 3x gate` recovery clean: [20260329T145035Z-47064365-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-window-120s-recovery-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T145035Z-47064365-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-window-120s-recovery-mixed-summary.json)
- workers=4 repeat60-vs-120 study summary: [20260329T151918Z-b97a8e0b-workers4-repeat60-vs-120-study-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T151918Z-b97a8e0b-workers4-repeat60-vs-120-study-summary.json)
- workers=4 `60s-a + 3x gate` recovery with first search captcha at search `118`: [20260329T151918Z-b97a8e0b-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-60s-a-recovery-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T151918Z-b97a8e0b-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-60s-a-recovery-mixed-summary.json)
- workers=4 `120s-a + 3x gate` recovery clean: [20260329T151918Z-b97a8e0b-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-120s-a-recovery-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T151918Z-b97a8e0b-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-120s-a-recovery-mixed-summary.json)
- workers=4 `60s-b + 3x gate` recovery with first search captcha at search `118`: [20260329T151918Z-b97a8e0b-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-60s-b-recovery-mixed-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T151918Z-b97a8e0b-batched-mixed-search-small-detail-budget-historical-prefix-120-every5-max24-workers-4-burst1s-dual-60s-b-recovery-mixed-summary.json)
- workers=4 overnight night-driver summary `4` distributed slots, no clean seed/recovery: [20260329T224442Z-2fbce3a6-night-driver-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260329T224442Z-2fbce3a6-night-driver-summary.json)
- daytime matrix summary `control-short -> aggr-short -> control-long -> aggr-long`: [20260330T074845Z-6a59de41-night-driver-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260330T074845Z-6a59de41-night-driver-summary.json)
- crossover matrix summary `control-long -> aggr-long -> control-short -> aggr-short`: [20260330T170012Z-3512a4b5-night-driver-summary.json](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260330T170012Z-3512a4b5-night-driver-summary.json)

### Phase A. Instrumentation Cleanup

Перед новыми прогонами доработать probe tooling:

- нормализовать captcha detection по `error_type` и `error_value`;
- фиксировать `request_index_from_run_start`;
- фиксировать `seconds_since_previous_request`;
- писать явный `header_mode`;
- для cooldown study добавить пометку `minutes_since_first_captcha`;
- для mixed workload явно различать `search`, `detail`, `dictionary`.

Ожидаемый артефакт:

- обновлённый probe notebook и/или маленькие helper-утилиты, которые дают одинаковый summary для всех сценариев.

### Phase B. Minimal Baseline Matrix

Эксперименты на одном network path:

1. Header baseline:
   - fixed request repeat для `user_agent_only`, `hh_user_agent_only`, `dual`
2. Auth baseline:
   - `anonymous` vs `application_token` на одинаковом сценарии
3. Pacing baseline:
   - sequential probe с паузами `0.5s`, `1s`, `2s`, `5s`
4. Concurrency baseline:
   - replay или burst replay при `workers=1`, `2`, `4`
5. Shape baseline:
   - deep paging vs round-robin vs historical replay

Выход:

- provisional safe profile для search.

### Phase C. Cooldown Study

Эксперименты:

1. Controlled trigger до первой captcha
2. Single probe after `5m`
3. Single probe after `15m`
4. Single probe after `30m`
5. Single probe after `60m`
6. Single probe after `120m`
7. Single probe next day

Отдельно:

- после первой search captcha проверить один `detail` request и один dictionary request.

Выход:

- recovery runbook for scheduler.

### Phase D. Detail Value Study

Эксперименты:

1. Запустить `study-detail-payloads` на свежем crawl run
2. Посмотреть summary и report
3. Выписать fields that are:
   - only in detail;
   - already in raw search item;
   - volatile across repeated detail fetches;
   - critical / useful / optional for downstream research

Выход:

- предварительное решение между `search-only`, `search-first`, `selective-detail`.

### Phase E. Mixed Workload Study

Эксперименты:

1. `search-only`
2. `search + detail after coverage`
3. `search + small detail budget`

Выход:

- policy for production scheduler.

### Phase F. Network Path Study

Эксперименты:

1. Повтор safe search profile на second stable network path
2. Если нужно, один scoped proxy experiment без rotation

Выход:

- понимание, нужен ли network fallback.

## 8. Experiment registry

Для каждого эксперимента фиксировать:

- `experiment_id`
- `date_utc`
- `network_path_id`
- `header_mode`
- `scenario_type`
- `areas`
- `pages`
- `per_page`
- `workers`
- `pause_seconds`
- `total_requests`
- `requests_until_first_captcha`
- `wall_clock_until_first_captcha`
- `first_captcha_request`
- `cooldown_minutes`
- `detail_enabled`
- `notes`

Артефакты сохранять в:

- `.state/reports/hh-api-probe/` для search/cooldown runs;
- `.state/reports/detail-payload-study/` для search-vs-detail;
- отдельную markdown-сводку по итогам каждой фазы.

## 9. Proposed decision framework

После первых фаз нужно выбрать один из трёх operational modes:

### Mode A. Search-Only Baseline

Выбираем, если:

- search стабилен;
- detail даёт ограниченную добавочную ценность;
- search raw payload покрывает большую часть нужных полей.

### Mode B. Search-First With Deferred Selective Detail

Выбираем, если:

- search стабилен;
- detail ценен, но заметно повышает риск captcha;
- detail можно отложить и жёстко бюджетировать.

### Mode C. Search Plus Small Ongoing Detail Budget

Выбираем, если:

- detail реально нужен;
- его можно безопасно встроить в budget без ухудшения search stability.

Network fallback или proxy можно рассматривать только как дополнительный lever поверх одного из этих режимов, а не как замену самой стратегии.

## 10. Что делать дальше

Минимальный practical plan из текущей точки:

1. Запустить `Phase E. Mixed Workload Study` на `anonymous + dual` как на текущем baseline:
   - считать `search + detail after coverage` уже подтверждённым как минимум для `80 + 20`, `100 + 20`, `110 + 20`;
   - считать `search + small detail budget` уже подтверждённым как минимум для `every 10 searches -> 1 detail` и `every 5 searches -> 1 detail`;
   - дальше сравнивать не только total captcha, но и сдвиг search threshold относительно control search-only.
2. Для mixed workload в первую очередь сравнивать:
   - search captcha incidence;
   - search latency degradation;
   - requests until first search captcha;
   - detail success rate;
   - необходимость endpoint-specific cooldown.
3. Следующий практический шаг для `Phase E`:
   - считать conservative batched contour `workers=3`, `burst_pause=1s` уже подтверждённым не только на `search_prefix=80`, но и на `100` и `120`;
   - следующей эскалацией менять только один фактор за раз:
     - на `workers=4`, `burst_pause=1s` больше не тратить слоты на новые short repeat-ы `120s + 3x clean probe`; overnight sample уже показал, что этого недостаточно для defendable clean recovery rule;
     - forward-order matrix `control-short -> aggr-short -> control-long -> aggr-long` уже выполнен и показал, что `300s` не дал явного выигрыша над `120s`, но порядок остаётся confounded по времени дня;
     - crossover matrix в обратном порядке `control-long -> aggr-long -> control-short -> aggr-short` уже выполнен и показал, что observed difference действительно исчезает после инверсии order/time-of-day;
     - значит, следующий шаг уже не новый cooldown/window repeat, а либо уменьшить `burst_pause` при `workers=3`, либо на `workers=4` проверять принципиально другой lever, например более плотный detail ratio, другой auth contour или другой network path;
   - отдельно проверять уже не только first bad contour, но и случаи non-monotonic behavior, где interleaving неожиданно даёт лучший outcome, чем pure search.
   Для overnight research run теперь есть отдельный runbook: [hh-api-probe-night-driver.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-probe-night-driver.md).
4. После mixed workload обновить `study-detail-payloads` на свежем crawl run, чтобы decision по selective detail опирался не только на старый отчёт.
   Текущее состояние на `2026-03-31`:
   - штатный DB-backed study уже выполнен:
     - [summary.md](/home/yurizinyakov/projects/hh_collector/.state/reports/detail-payload-study/20260331T134110Z/summary.md)
     - [report.json](/home/yurizinyakov/projects/hh_collector/.state/reports/detail-payload-study/20260331T134110Z/report.json)
   - sample `10/10`, raw drift `0/20`, normalized drift `0/20`;
   - observed detail-only research fields: `address.can_edit`, `description`, `key_skills[].name`, `branded_description`;
   - вывод не изменился: selective detail policy остаётся сильнее, чем exhaustive detail.
5. Search-side `auth baseline` уже закрыт:
   - fixed repeat: `anonymous` и `application_token` оба clean, без captcha;
   - sequential paging: `anonymous` и `application_token` оба clean, без captcha;
   - historical replay: `anonymous` clean `130/130`, `application_token` без captcha/403, но с одним TLS handshake timeout.
   - boundary-seeking crossover на baseline contour `workers=3`, `burst_pause=1s`, `cap=180`: `anonymous -> token -> token -> anonymous`, все четыре runs clean `180/180`, без captcha/403.
   - long search-only token run на baseline contour `workers=3`, `burst_pause=1s`, `cap=2000`: clean `2000` requests, без captcha/403, но с `4` transport resets (`connection reset by peer`).
   - long search-only anonymous control на baseline contour `workers=3`, `burst_pause=1s`, `cap=2000`: clean `2000` requests, без captcha/403, но с `5` transport errors.
   - token trigger-discovery на повышенном pressure contour `workers=4`, `burst_pause=0s`, `search_prefix=130`: full run `130/130`, без captcha/403, но с `5` transport resets и `p95 latency ~7.6s`; это смещает practical failure mode с captcha на transport instability.
6. Следующий auth шаг теперь уже не новый pure-search replay, а сравнение `anonymous` vs `application_token` на detail/mixed contour:
   - мягкий detail repeat;
   - bounded mixed plan на baseline `workers=3`, `burst_pause=1s`;
   - payload/field differences, если auth реально меняет ответ.
7. Detail/mixed auth baseline теперь тоже частично закрыт:
   - мягкий detail crossover `5 ids x 4 cycles`: `anonymous -> token -> token -> anonymous`, все четыре runs clean `20/20`;
   - bounded mixed crossover `120 search + every5 -> 1 detail + max24`, `workers=3`, `burst_pause=1s`: оба token runs clean `144/144`, anonymous run A clean `144/144`, anonymous run B `143x200 + 1 TLS handshake timeout`, без captcha/403;
   - на baseline-safe detail/mixed contours token пока не показал policy-grade operational advantage.
8. Для `v1` policy auth question на baseline contours уже достаточно сужен:
   - ни token, ни anonymous не показали anti-bot advantage друг над другом;
   - оба режима viable на baseline contours;
   - operator may still prefer token as default for supported app path, even without benchmark advantage.
9. После mixed workload и auth baseline сформулировать первый реальный scheduler policy draft:
   - search baseline envelope;
   - search cooldown after captcha;
   - transport-error retry/backoff policy;
   - allowed detail budget;
   - stop/go criteria для второго network path.

Отдельное уточнение после `policy v1` discussion от 2026-03-31:

- `selective detail` для research-задачи нужно трактовать не как "маленький optional detail budget вообще", а как:
  - обязательный first detail для vacancy, которая впервые увидена или ещё ни разу не получила успешный `detail`;
  - optional refresh только для `short_changed` и `ttl_refresh`;
- это означает, что same-run `detail_limit=20` можно защищать как conservative bounded budget, но нельзя считать достаточным для полной research-completeness policy;
- первый useful detail-throughput baseline теперь уже есть:
  - sequential `2000` distinct detail requests под `application_token`: `1960x200 + 37x404 + 3 connection reset by peer`, без `403/captcha`, `~179.9 req/min`;
  - conservative batched `workers=3`, `burst_pause=1s`, `1200` distinct detail requests: `1180x200 + 20x404`, без `403/captcha`, `~119.5 req/min`;
  - practical implication: detail endpoint сейчас не выглядит главным bottleneck для completeness policy;
- gap и следствия отдельно сведены в [hh-api-completeness-policy-note.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-completeness-policy-note.md).

## 11. Open questions

- Есть ли practical difference между `User-Agent` и `HH-User-Agent` именно для HH vacancy search?
- Search captcha и detail captcha живут в одном block domain или в разных?
- Может ли `detail` сам по себе словить captcha при отдельном burst-профиле?
- Есть ли вообще другой lever, который сможет сделать `workers=4` defendable, или этот contour уже разумно оставить research-only вне `policy v1`?
- Есть ли penalty accumulation, если captcha случается несколько раз за одну сессию или за один день?
- Есть ли у `application token` какие-либо non-performance преимущества для downstream ops, кроме самого факта approved app path?
- Какой минимум полей downstream research действительно требует из detail?
- Можно ли получить приемлемый dataset из search raw payload без систематического detail?
- Нужен ли второй network path как fallback, если baseline на одном IP окажется слишком хрупким?
- Достаточно ли для `v1` conservative captcha rule `stop search until next regular tick`, если на token-default contour раньше начинает проявляться transport noise, а не captcha?

## 12. References

Official:

- HeadHunter API README: https://github.com/hhru/api/blob/master/README.md
- Условия использования API: https://dev.hh.ru/admin/developer_agreement?hhtmFrom=Index
- Errors: https://github.com/hhru/api/blob/master/docs/errors.md
- Vacancy search Redoc: https://api.hh.ru/openapi/redoc#tag/Poisk-vakansij/operation/get-vacancies

External hypothesis source:

- ProxyCove, "Прокси для парсинга job boards": https://proxycove.com/ru/blog/proksi-dlya-parsinga-job-boards

Local project context:

- [hh_api_captcha_probe.ipynb](/home/yurizinyakov/projects/hh_collector/notebooks/hh_api_captcha_probe.ipynb)
- [hh-api-probe-cooldown-driver.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-probe-cooldown-driver.md)
- [hh-api-scheduler-policy-v1-draft.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-scheduler-policy-v1-draft.md)
- [hh-api-policy-v1-next-implementation-steps.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-policy-v1-next-implementation-steps.md)
- [hh-api-completeness-policy-note.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-completeness-policy-note.md)
- [hh-api-collection-policy-draft.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-collection-policy-draft.md)
- [study_detail_payloads.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/commands/study_detail_payloads.py)
- [research.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/interfaces/cli/commands/research.py)

## 13. Application Token Quick Note

Коротко:

- для нашего crawler baseline сначала стоит проверять именно `application token`;
- applicant OAuth и applicant-specific поля не нужно путать с machine-to-machine baseline для research crawler;
- если `application token` не даёт operational выигрыша, baseline можно оставить anonymous.

Практически:

1. Убедиться, что приложение HH зарегистрировано в `dev.hh.ru`.
2. Зайти в `https://dev.hh.ru/admin` под владельцем приложения.
3. Проверить, отображается ли актуальный `access_token` приложения.
4. Если токен ещё ни разу не получали и он не отображается, пройти официальный метод `Авторизация приложения` в OpenAPI и получить его один раз.
5. Сохранить токен отдельно от user OAuth credentials и не смешивать эти режимы в benchmark.

Важно:

- статья на Хабре описывает в первую очередь OAuth-путь через `code`, то есть user-facing auth flow;
- это полезная гипотеза, но не тот auth contour, который надо тестировать первым для unattended crawler.

## 14. First 3 Auth Benchmark Runs

Запускались на одном и том же network path и с одинаковым `User-Agent`.

1. Fixed request repeat:
   - `anonymous` vs `application_token`
   - один и тот же запрос, например `area=1003,page=0,per_page=20`
   - цель: увидеть, меняет ли token базовое поведение даже на мягком сценарии

2. Sequential paging:
   - `anonymous` vs `application_token`
   - один `area`, последовательные страницы, умеренный pacing
   - цель: понять, увеличивает ли token запас до первой captcha в реалистичном list-flow

3. Historical replay:
   - `anonymous` vs `application_token`
   - replay сохранённой historical sequence без burst
   - цель: сравнить auth modes на сценарии, близком к реальному crawler path

Для каждого из трёх прогонов фиксировать:

- `auth_mode`
- `header_mode`
- `requests_until_first_captcha`
- `wall_clock_until_first_captcha`
- `status_counts`
- `first_403_request_shape`
- `payload differences`, если появятся дополнительные поля

Статус на 2026-03-31:

- Fixed request repeat:
  - `anonymous`: clean `8/8`, без captcha;
  - `application_token`: clean `8/8`, без captcha;
  - раннего operational преимущества у token не видно.
- Sequential paging:
  - `anonymous`: clean `10/10`, без captcha;
  - `application_token`: clean `10/10`, без captcha;
  - search threshold не изменился, latency profile смешанный.
- Historical replay:
  - `anonymous`: clean `130/130`, без captcha/403;
  - `application_token`: `129x200 + 1 transport timeout`, без captcha/403;
  - timeout был TLS handshake timeout на одном search request, а не auth-level rejection.
- Boundary-seeking search-only crossover:
  - contour: `workers=3`, `burst_pause=1s`, `cap=180`, order `anonymous -> token -> token -> anonymous`;
  - все четыре runs clean `180/180`, без captcha/403;
  - на baseline search contour token не показал сдвига threshold даже в более длинном A/B.
- Long search-only token run:
  - contour: `workers=3`, `burst_pause=1s`, `cap=2000`;
  - clean `2000` requests, без captcha/403;
  - были `4` transport errors `connection reset by peer`, но не anti-bot response.
- Long search-only anonymous control:
  - contour: `workers=3`, `burst_pause=1s`, `cap=2000`;
  - clean `2000` requests, без captcha/403;
  - были `5` transport errors, но тоже не anti-bot response.
- Token trigger discovery on elevated pressure:
  - contour: `workers=4`, `burst_pause=0s`, `search_prefix=130`;
  - full `130/130`, без captcha/403;
  - были `5` `connection reset by peer`;
  - practical signal: на token-default path повышенный pressure может сначала проявляться как transport instability, а не как captcha threshold.
- Detail crossover:
  - contour: `5 fresh vacancy ids x 4 cycles`, single-stream, `2s` pacing;
  - `anonymous -> token -> token -> anonymous`, все четыре runs clean `20/20`;
  - status-level разницы между auth modes нет.
- Bounded mixed crossover:
  - contour: `120 search + every5 -> 1 detail + max24`, `workers=3`, `burst_pause=1s`;
  - оба token runs clean `144/144`;
  - anonymous run A clean `144/144`, anonymous run B дал один TLS handshake timeout на search, без captcha/403;
  - на этом sample token не доказал anti-bot advantage, но transport noise тоже пока слишком мала для policy switch.

Промежуточный вывод:

- на search-side scenarios `application_token` пока не показал comparative anti-bot преимущества над anonymous;
- при этом сам по себе token уже доказал viability на длинном baseline-slot до `2000` requests;
- на elevated pressure contour token тоже не дал captcha на `130`, но начал генерировать transport resets; значит, для scheduler draft transport-error policy теперь важнее, чем новый captcha-seeking auth repeat;
- на мягком detail и bounded mixed contour token тоже не показал policy-grade преимущества;
- по benchmarks оба режима viable на baseline contour;
- если operator-policy предпочитает approved app path, `application_token` можно считать default без противоречия текущим данным;
- `anonymous` при этом логично оставить как fallback/control contour.
