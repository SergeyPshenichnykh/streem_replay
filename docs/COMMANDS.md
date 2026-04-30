# Команди / scripts

## Еталонний (baseline) dev‑UI — sticky totals + CS table + interactive

```bash
python scripts/replay_stream_selected_markets_dashboard_sticky_totals.py \
  --discover-targets \
  --start-minutes-before 10 \
  --list-totals-ladder \
  --self-check \
  --no-snapshots-csv \
  --ladder \
  --ladder-max-rows 12 \
  --ticks-above 12 \
  --ticks-below 12 \
  --col-width 52 \
  --cs-cols 3 \
  --delay 0.0025 \
  --balance 1000 \
  --interactive
```

Клавіші (`--interactive`):

- `space` — pause/resume
- `n` (або `т`) — next frame (+250ms)
- `b` (або `и`) — back frame (-250ms)
- `q` (або `й`) — quit

## Stationary totals wrapper

```bash
python scripts/replay_stream_selected_markets_dashboard_stationary_totals.py --help
```

## Smooth UI wrapper (legacy)

```bash
python scripts/replay_stream_selected_markets_dashboard_smooth.py --help
```

