# HH API Probe Cooldown Driver

## Назначение

`hh_api_probe_cooldown_driver.py` это headless research-driver для `Phase C. Cooldown Study`.

Он нужен, когда нужно не вручную из ноутбука, а воспроизводимо прогнать цепочку:

1. `trigger` до первой `search` captcha;
2. затем одноточечные probes на окнах `t+0`, `5m`, `15m`, `30m`, `60m`, `120m` и дальше при необходимости;
3. на каждом окне проверить:
   - один `search` request;
   - один `detail` request;
   - один dictionary request (`/dictionaries`).

## Что отвечает

Driver помогает ответить на три вопроса:

- восстанавливается ли `search` endpoint после captcha в разумном окне;
- живут ли `detail` и dictionary endpoints раньше `search`, то есть нужен ли endpoint-specific cooldown;
- насколько practical trigger под текущим auth/path вообще приводит к captcha, а не к transport noise.

## Что не отвечает

Driver не заменяет:

- `detail payload study`;
- mixed workload study;
- production scheduler soak.

## Запуск

Базовый запуск:

```bash
./.venv/bin/python -m notebooks.hh_api_probe_cooldown_driver
```

Пример с явными окнами и trigger contour:

```bash
./.venv/bin/python -m notebooks.hh_api_probe_cooldown_driver \
  --trigger-prefix 130 \
  --trigger-workers 4 \
  --trigger-burst-pause-seconds 0 \
  --probe-windows-seconds 0,300,900,1800,3600,7200
```

По умолчанию:

- source sequence берётся из historical replay fixture в `.state/reports/hh-api-probe/`;
- `auth_mode=application_token`, если токен сконфигурирован, иначе `anonymous`;
- header contour `dual`;
- probe windows: `0,300,900,1800,3600,7200`.

## Артефакты

Driver пишет:

- стандартные `jsonl` и `*-report.json` для trigger и каждого probe;
- aggregate summary:
  - `*-cooldown-driver-summary.json`
  - `*-cooldown-driver-summary.md`

## Как читать summary

Главные поля:

- `trigger_requests_until_first_captcha`
- `trigger_transport_error_count`
- `first_search_recovered_window_seconds`
- `first_detail_recovered_window_seconds`
- `first_dictionary_recovered_window_seconds`

Если `trigger_requests_until_first_captcha = null`, driver не смог ввести contour в captcha-state. Это полезный результат само по себе: для данного auth/path practical early failure mode может быть transport instability, а не captcha.

Если `detail` и dictionary clean раньше, чем `search`, это аргумент в пользу endpoint-specific cooldown, а не global stop-the-world.
