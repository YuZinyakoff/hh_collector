# HH API Probe Night Driver

Ночной `driver` нужен не для system soak-test и не для scheduler verification.

Его задача:

- изолированно проверять HH API policy-гипотезы на живом `api.hh.ru`;
- распределять прогоны по времени суток без ручного дежурства;
- оставлять один утренний aggregate summary поверх всех slot runs;
- сохранять все стандартные probe artifacts в `.state/reports/hh-api-probe/`.

Смежные документы:

- [hh-api-collection-policy-draft.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-collection-policy-draft.md)
- [hh-api-collection-strategy-research-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-collection-strategy-research-plan.md)
- [manual-happy-path.md](/home/yurizinyakov/projects/hh_collector/docs/ops/manual-happy-path.md)
- [hh-api-probe-next-night-matrix-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-probe-next-night-matrix-plan.md)

## 1. Что именно делает driver

`driver` это headless Python-обвязка над [hh_api_probe_harness.py](/home/yurizinyakov/projects/hh_collector/notebooks/hh_api_probe_harness.py) и [hh_api_probe_night_driver.py](/home/yurizinyakov/projects/hh_collector/notebooks/hh_api_probe_night_driver.py).

Он работает слотами.

В каждом слоте:

1. делает `seed preflight` через `3x` clean low-rate probes;
2. запускает агрессивный mixed workload `seed run`;
3. ждёт фиксированное recovery window;
4. делает `recovery preflight` через `3x` clean low-rate probes;
5. запускает агрессивный mixed workload `recovery run`;
6. пишет slot-level и session-level summary.

По умолчанию текущий research contour такой:

- `search_prefix=120`
- `every 5 searches -> 1 detail`
- `max_detail_requests=24`
- `workers=4`
- `burst_pause=1s`
- `header_mode=dual`
- `auth_mode=application_token`, если токен сконфигурирован; иначе `anonymous`
- `recovery_window=120s`
- `slot_interval=7200s`

В текущем release driver умеет не только один fixed contour, но и последовательность встроенных slot profiles.

Поддерживаемые профили:

- `default`: текущий основной contour из CLI args;
- `aggr-short`: aggressive contour `workers=<workers>`, `recovery_window=<recovery_window_seconds>`;
- `control-short`: conservative control `workers=<control_workers>`, `recovery_window=<recovery_window_seconds>`;
- `aggr-long`: aggressive contour `workers=<workers>`, `recovery_window=<long_recovery_window_seconds>`;
- `control-long`: conservative control `workers=<control_workers>`, `recovery_window=<long_recovery_window_seconds>`.

## 2. На какие вопросы он отвечает

Ночной driver сейчас нужен в первую очередь для policy research.

Он помогает ответить на такие вопросы:

- стабилен ли aggressive contour в разное время суток;
- достаточно ли `120s + 3x clean probes` как defendable operator gate;
- насколько сильно recovery лучше или хуже `seed` в пределах одной ночи;
- плавает ли число попыток, нужных для `3x clean` gate;
- есть ли same-night accumulation между слотами.

Он не закрывает автоматически другие research questions:

- даёт ли `application token` operational advantage;
- насколько ценен detail payload относительно search-only dataset;
- как выглядит long-window cooldown `5m+`;
- готова ли вся система к unattended production run.

## 3. Что мы считаем успехом за ночь

Ночная research-сессия полезна даже если не все recovery проходят clean.

Минимально полезный результат утром:

- есть хотя бы `2-3` завершённых slot runs;
- по каждому слоту сохранены `jsonl`, `report`, `mixed-summary`;
- есть session aggregate `json` и `md`;
- видно, улучшается ли `recovery` относительно `seed`;
- видно, держится ли `120s + 3x gate` или снова плавает.

Если `recovery_clean_count` стабильно положительный и recovery чаще лучше seed, это хороший сигнал в пользу текущего gate.

Если recovery снова проседает до boundary-level, текущая гипотеза про `120s` ослабляется.

## 4. Как запускать ночью

Запускать лучше detached, чтобы терминал не держал процесс:

```bash
RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_PATH=".state/reports/hh-api-probe/${RUN_TS}-night-driver.log"
PID_PATH=".state/reports/hh-api-probe/night-driver.pid"

nohup ./.venv/bin/python -m notebooks.hh_api_probe_night_driver \
  --slot-interval-seconds 7200 \
  > "$LOG_PATH" 2>&1 &

echo $! | tee "$PID_PATH"
```

По умолчанию первый слот стартует сразу, затем следующие идут через каждые `2` часа.

Если нужно ограничить число слотов, можно добавить:

```bash
--max-slots 3
```

Если нужен matrix-mode со сменой профилей по слотам:

```bash
--slot-profile-sequence control-short,aggr-short,control-long,aggr-long
```

Тогда driver будет циклически чередовать эти профили по слотам.

Если нужно оставить per-request логирование:

```bash
--show-probe-logs
```

## 5. Что проверить сразу после старта

Достаточно двух коротких команд:

```bash
tail -n 20 "$LOG_PATH"
```

```bash
ps -fp "$(cat .state/reports/hh-api-probe/night-driver.pid)"
```

Ожидаемо:

- в логе есть строка `Night driver started. Summary: ...`;
- затем идут сообщения вида `slot-001: starting`, `seed run`, `waiting 120s`, `recovery run`;
- процесс с указанным `PID` жив.

## 6. Как останавливать утром

Останавливать нужно мягко:

```bash
kill -TERM "$(cat .state/reports/hh-api-probe/night-driver.pid)"
```

После этого driver:

- не бросает session без записи состояния;
- заканчивает текущий шаг;
- помечает сессию как `stopped_by_operator`;
- пишет финальные `summary.json` и `summary.md`.

Не нужно делать `kill -9`, если нет явшего зависания процесса.

## 7. Что смотреть утром

Сначала найти последние session summaries:

```bash
ls -1t .state/reports/hh-api-probe/*night-driver-summary.*
```

Обычно достаточно открыть:

- последний `*-night-driver-summary.md`
- последний `*-night-driver-summary.json`
- при необходимости `tail -n 50 "$LOG_PATH"`

### 7.1. Как читать aggregate summary

Главные поля:

- `total_slots`
- `completed_slots`
- `seed_clean_count`
- `recovery_clean_count`
- `recovery_better_than_seed_count`
- `avg_seed_gate_attempts`
- `avg_recovery_gate_attempts`
- `min/max seed_search_ok`
- `min/max recovery_search_ok`

Практическая интерпретация:

- если `recovery_clean_count > 0`, значит хотя бы часть ночи gate реально восстанавливал contour;
- если `recovery_better_than_seed_count` близко к числу завершённых слотов, recovery policy выглядит полезной;
- если `avg_recovery_gate_attempts` растёт, это сигнал дрейфа или accumulation;
- если `min_recovery_search_ok` остаётся на boundary-level `117-119`, current gate ещё не defendable.

### 7.2. Как читать slot table

В markdown-таблице по слотам важны:

- `seed gate`
- `seed ok`
- `seed captcha idx`
- `recovery gate`
- `recovery ok`
- `recovery captcha idx`

Если `recovery ok > seed ok`, слот дал полезный recovery signal.

Если `recovery ok == seed ok`, recovery window, скорее всего, не дал выигрыша.

Если `recovery` clean, а `seed` boundary-level, это сильный аргумент в пользу текущего gate.

В matrix-mode дополнительно сравнивать нужно не только `seed` vs `recovery`, но и разные профили между собой:

- лучше ли `aggr-long`, чем `aggr-short`;
- остаются ли `control-*` clean;
- не объясняется ли просадка просто временем суток, если control тоже деградирует.

## 8. Что это значит для policy

Этот driver не меняет production baseline автоматически.

Он нужен, чтобы двигать research policy между такими состояниями:

- `гипотеза`
- `есть единичный clean sample`
- `есть defendable repeated signal`

Пока default policy всё ещё строится вокруг conservative contour:

- `search-first`
- `application_token + dual`
- `workers=3`
- `burst_pause=1s`
- `deferred selective detail`

Aggressive contour `workers=4` остаётся research-only до накопления более устойчивого ночного сигнала.
