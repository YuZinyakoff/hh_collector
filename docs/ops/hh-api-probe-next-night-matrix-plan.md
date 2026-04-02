# HH API Probe Next Night Matrix Plan

Статус: question closed, no immediate rerun on this axis.

Этот файл фиксирует завершение window-order study и объясняет, почему на оси `120s vs 300s` новый overnight run сейчас не нужен.

Он был нужен после:

- ночи `2026-03-29 -> 2026-03-30`;
- дневного matrix-cycle `2026-03-30 10:48 -> 16:56` MSK.

Контекст:

- overnight night-driver на `workers=4`, `recovery_window=120s` завершил `4/4` слота операционно clean, но без единого clean seed/recovery;
- дневной forward-order matrix `control-short -> aggr-short -> control-long -> aggr-long` дал mixed result:
  - `control-short`, `aggr-short`, `control-long` прошли clean и на `seed`, и на `recovery`;
  - только `aggr-long` recovery после `300s` словил первую search-captcha на `119`-м search request;
- `300s` therefore пока не показал явного преимущества над `120s`;
- baseline policy по-прежнему остаётся на conservative contour `workers=3`, `burst_pause=1s`.
- crossover run `2026-03-30 20:00 -> 2026-03-31 02:04` MSK показал, что:
  - `control-long` и `control-short` recovery остались clean;
  - `aggr-long` и `aggr-short` recovery дали одинаковый boundary-level outcome `117`;
  - значит, difference между `120s` и `300s` не воспроизводится после инверсии order/time-of-day.

Смежные документы:

- [hh-api-probe-night-driver.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-probe-night-driver.md)
- [hh-api-collection-policy-draft.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-collection-policy-draft.md)
- [hh-api-collection-strategy-research-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-collection-strategy-research-plan.md)

## 1. Что этот файл теперь значит

Изначальная цель этого плана была такой:

- сохраняется ли observed difference между `120s` и `300s`, если инвертировать порядок слотов;
- не был ли дневной result просто следствием того, что `aggr-long` попал на более поздний час;
- нужно ли после этого закрывать ветку `workers=4` для `policy v1`.

Теперь на эти вопросы есть достаточный ответ:

- observed difference между `120s` и `300s` не воспроизвёлся;
- controls clean, aggressive recovery boundary-level одинаково на обоих окнах;
- новый cooldown/window rerun по той же оси не нужен.

## 2. Последний выполненный matrix run

Использовались встроенные профили driver в обратном порядке:

1. `control-long`
2. `aggr-long`
3. `control-short`
4. `aggr-short`

Смысл профилей:

- `control-short`: `workers=3`, `recovery_window=120s`
- `aggr-short`: `workers=4`, `recovery_window=120s`
- `control-long`: `workers=3`, `recovery_window=300s`
- `aggr-long`: `workers=4`, `recovery_window=300s`

Дальше driver повторяет эту последовательность циклически.

Это deliberate crossover design: он нужен, чтобы разнести `120s` и `300s` по другим часам и убрать самый явный time-of-day confound из предыдущего daytime sample.

## 3. Команда последнего запуска

```bash
RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_PATH=".state/reports/hh-api-probe/${RUN_TS}-night-driver.log"
PID_PATH=".state/reports/hh-api-probe/night-driver.pid"

nohup ./.venv/bin/python -m notebooks.hh_api_probe_night_driver \
  --slot-profile-sequence control-long,aggr-long,control-short,aggr-short \
  --slot-interval-seconds 7200 \
  > "$LOG_PATH" 2>&1 &

echo $! | tee "$PID_PATH"
```

Утром останавливать мягко:

```bash
kill -TERM "$(cat .state/reports/hh-api-probe/night-driver.pid)"
```

## 4. Итоговая интерпретация

Сильный сигнал получен в сторону закрытия этой оси:

- observed difference между `aggr-short` и `aggr-long` исчез;
- control slots остались clean;
- aggressive recovery на обоих окнах дали один и тот же boundary-level outcome;
- window length между `120s` и `300s` не выглядит explanatory lever.

## 5. Что смотреть в артефактах

1. Последний `*-night-driver-summary.md`
2. Последний `*-night-driver-summary.json`
3. Последний `*-night-driver.log`

Важные вопросы при разборе:

- одинаковы ли `aggr-long` и `aggr-short` после смены порядка;
- остаются ли `control-*` clean;
- растут ли `avg_*_gate_attempts` по мере ночи;
- виден ли дальнейший penalty accumulation по aggressive slots.

## 6. Decision Rule После Этого Запуска

После crossover run следующий шаг уже не новый cooldown repeat.

Тогда приоритет смещается так:

1. Зафиксировать `workers=4` как research-only contour, не кандидат для `policy v1`.
2. Вернуться к устойчивому baseline `workers=3`.
3. Продолжить `detail payload study` и `auth baseline`.
