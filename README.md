# betfair_bot

Dev‑UI (terminal) for Betfair strategy development: totals ladders (Under‑only) + Correct Score table, driven by replay stream.

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
```

Dashboard/stream scripts are stdlib‑only.

## Main commands

See `docs/COMMANDS.md`.

## Portability

See `docs/PORTABILITY.md`.

This repo currently contains:
- `scripts/`: replay + market analysis utilities (CSV generation, plotting, dutching metrics).
- `replay/`: output artifacts from replay runs (CSV/logs/plots).
- `bot/`: **work-in-progress** live dutching bot scaffolding.

## Goal
Find "positive" dutching (positive margin) on selected markets and optionally place orders.

### What "positive dutching" means here
Given a set of runner prices, compute:
- `inv_sum = sum(1/odds)`
- `margin = 1/inv_sum - 1`

If `margin > 0` (or `margin_pct > threshold`), it's a candidate.

This bot supports:
- back-dutching: uses `best_lay` quotes and places `BACK` orders
- lay-dutching: uses `best_back` quotes and places `LAY` orders

## Run (current state)
Bot is wired for signal + staking + risk limits, but **live stream + real order placement is not implemented yet**.

- Configure targets: `configs/dutching_bot.example.json`
- Run: `python scripts/run_dutching_bot.py --config configs/dutching_bot.example.json`
  - Replay mode (default): reads `replay/selected_market_snapshots_30m_update_250ms/selected_markets_250ms.csv` if present
  - Debug: add `--max-rows 100000`
  - Run on every market in the replay CSV: add `--all-markets` (disables market_id filtering)
  - Less spam: add `--max-signals 50` (and omit `--print-orders`)
  - Live mode placeholder: add `--live` (will exit with a message)

## Next step to make it real
Implement a `MarketDataStream` that reads live market books from Betfair (betfairlightweight/flumine),
and an `OrderExecutor` that calls `placeOrders`.
