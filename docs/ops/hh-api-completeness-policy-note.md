# HH API Completeness Policy Note

Статус: active working note  
Дата: 2026-03-31

Этот документ фиксирует уточнение research goal и policy-gap между текущим scheduler draft и фактической задачей проекта.

## 1. Research Goal

Для проекта важны две разные гарантии:

1. Полное `search`-покрытие видимого search-space hh.ru.
2. Хотя бы один успешный `detail` для каждой вакансии, хотя бы раз увиденной через `search`.

Это не одна и та же гарантия.

`search`-coverage отвечает на вопрос "видим ли мы все доступные вакансии в sweep contour".

`first-detail completeness` отвечает на вопрос "получили ли мы полную карточку хотя бы один раз для каждой наблюдённой вакансии".

## 2. Что На Самом Деле Значит Selective Detail

Для текущей research-задачи `selective detail` нужно трактовать не как "редкий detail вообще".

Правильная трактовка:

- `first_detail_mandatory`: если vacancy наблюдается впервые или ещё ни разу не получила успешный `detail`, она обязана попасть в persistent backlog до первого успешного `detail`;
- `refresh_optional`: повторный `detail` нужен только если vacancy живёт долго, изменила short-hash или её пора перепроверить по TTL;
- `detail on every observation` не нужен.

То есть баланс достигается не отказом от полноты, а отказом от повторного detail на каждом sweep.

## 3. Current Assessment

### 3.1. Search Side

Текущий search contour уже выглядит defendable для exhaustive collection:

- planner v2 стартует не с одной global partition, а с набора disjoint area roots;
- saturated scopes split'ятся в child scopes вместо ложного "полного покрытия";
- full run success привязан к tree coverage semantics, а не к факту нескольких прочитанных страниц.

Свежий live capacity snapshot:

- `global_found = 885266`;
- lower-bound `list pages = 44264`;
- lower-bound `terminal leaves = 443`, если считать saturation threshold как practical ceiling `100 pages x 20 items`;
- lower-bound `search-only sweep time ~= 9.34h` при observed baseline `79 search req/min`;
- `5/9` root areas уже saturated на первом уровне.

Артефакт:

- [capacity snapshot](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T140325Z-capacity-snapshot.json)

Вывод:

- текущая `search-first` policy выглядит совместимой с полным `search` coverage;
- для этой части goal главный риск уже не throughput сам по себе, а unresolved branches / transport breaker / resume semantics.

### 3.2. Detail Side

Текущий scheduler draft недостаточен для `first-detail completeness`.

Почему:

- same-run `detail_limit=20` является только bounded budget, а не гарантией first-detail coverage;
- current detail selection строится из vacancies, наблюдённых в данном `crawl_run`, и режется по `limit`;
- run может честно завершиться как `succeeded`, даже если множество впервые увиденных vacancies не получили `detail` просто потому, что не вошли в top-`limit`.

Практически это означает:

- текущий policy draft хорош для bounded same-run detail;
- но он не гарантирует, что каждая найденная vacancy когда-либо получит первый `detail`;
- для fast-moving vacancies это особенно рискованно: они могут исчезнуть до того, как попадут в маленький same-run detail budget.

Свежий live signal по самому detail endpoint уже сильнее, чем раньше:

- sequential `500` distinct detail requests под `application_token`: `491x200 + 9x404`, без `403/captcha`, observed `~211.93 req/min`;
- sequential `2000` distinct detail requests: `1960x200 + 37x404 + 3 connection reset by peer`, без `403/captcha`, observed `~179.9 req/min`;
- conservative batched contour `workers=3`, `burst_pause=1s`, `1200` distinct detail requests: `1180x200 + 20x404`, без `403/captcha`, observed `~119.5 req/min`.

Артефакты:

- [detail drain capacity summary](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T154609Z-detail-drain-capacity-summary.md)
- [detail 2000 sequential summary](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T152326Z-1e70a9f8-detail-throughput-sequential-cap2000-token-summary.json)
- [detail 1200 workers3 burst1s summary](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T153521Z-6f6309ac-detail-throughput-workers3-burst1s-cap1200-token-summary.json)

Это меняет practical reading:

- detail endpoint сейчас не выглядит главным throughput bottleneck;
- главная проблема остаётся не в самом HH detail endpoint, а в backlog semantics и планировании drain contour.

Нижняя грубая оценка one-time detail bootstrap по текущему visible volume теперь уже лучше опирать на measured detail contour:

- `885266` detail requests;
- `~82.01h` при более консервативном measured sequential detail rate `~179.9 req/min`;
- `~123.47h` при operator-like contour `workers=3`, `burst_pause=1s`, `~119.5 req/min`.

Это всё ещё не production estimate, но этого уже достаточно, чтобы показать две вещи:

- cold-start `first-detail completeness` не решается `detail_limit=20`;
- и одновременно detail drain выглядит уже достаточно быстрым, чтобы не считать сам endpoint главным blocker.

## 4. Policy Change Required

До `policy v1` нужно формально развести два detail contour:

1. `mandatory first-detail backlog`
2. `optional refresh backlog`

Recommended priority order:

1. `first_seen_or_missing_detail`
2. `short_changed`
3. `ttl_refresh`

Required operator semantics:

- list coverage остаётся главным приоритетом;
- после закрытия list coverage система должна дренировать backlog vacancies без успешного first detail;
- TTL refresh не должен вытеснять vacancies, которые ещё ни разу не получили успешный `detail`;
- `detail_limit=20` допустим как conservative same-run budget или refresh budget, но не как глобальная гарантия полноты.

## 5. Execution Implication

Из этого следует отдельный execution contour для detail backlog.

Это не обязательно должен быть именно Celery, но по смыслу нужен отдельный background/drain path, который:

- работает поперёк `crawl_run`, а не только внутри одного sweep;
- умеет подхватывать backlog вакансий без успешного first detail;
- не мешает основному `search` contour;
- даёт оператору наблюдаемый backlog size и drain rate.

Возможные формы реализации:

- отдельный scheduler slice;
- отдельный worker;
- отдельный guarded CLI loop;
- queue-backed worker позже.

Ключевое требование не в конкретном фреймворке, а в наличии persistent first-detail drain contour.

## 6. What Must Be Measured Next

Чтобы policy стала не только логически правильной, но и operationally defendable, ещё нужны три числа:

1. Сколько новых vacancies приходит за один полный sweep в steady state.
2. Какой safe sustained throughput у `detail` contour.
3. Успевает ли backlog first-detail дренироваться быстрее, чем растёт.

По пункту `2` первый полезный baseline уже есть: текущие long detail probes дают working envelope примерно `120..180 req/min` без captcha, с редкими `404` и редким transport noise в длинном sequential run.

Отдельно нужно считать два режима:

- cold-start bootstrap;
- steady-state weekly operation.

Decision rule:

- policy годится для research-задачи только если полный `search` sweep стабильно закрывается, а backlog first-detail в steady state имеет нулевой или убывающий тренд.

## 7. Practical Consequence For Current Draft

На текущий момент корректно говорить так:

- текущий draft уже близок к defendable `search coverage policy`;
- текущий draft ещё не является defendable `full research completeness policy`;
- для этого ему не хватает persistent first-detail backlog semantics и capacity estimate для detail drain.

Связанные документы:

- [hh-api-collection-policy-draft.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-collection-policy-draft.md)
- [hh-api-scheduler-policy-v1-draft.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-scheduler-policy-v1-draft.md)
- [hh-api-policy-v1-next-implementation-steps.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-policy-v1-next-implementation-steps.md)
- [hh-api-completeness-implementation-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-completeness-implementation-plan.md)
