# RektEngine

**R**isk **E**valuation & **K**ill‑off **T**racking Engine — a fast risk
engine for leveraged trading. It tracks positions, computes
profit/loss and liquidation prices on every price tick, and auto-liquidates
positions the moment they breach maintenance margin — the core job every
leveraged exchange has to get right.

## What it does

- **Positions** — open longs or shorts with leverage.
- **Margin modes** — *isolated* (each position risks only its own margin) and
  *cross* (all positions share the wallet).
- **P&L** — unrealized (live) and realized (on close).
- **Liquidation price** — computed for longs and shorts.
- **Auto-liquidation** — force-closes and settles the moment price crosses the line.
- **Throughput** — ~6.2M price updates/sec in pure Python (target was 100k).

## Files

| File | What it is |
|------|-----------|
| `risk_engine.py` | The engine: positions, margin, P&L, liquidation math. Runs correctness asserts + a benchmark under `__main__`. |
| `simulator.py` | Mock market feed → engine → live dashboard → auto-liquidation, over asyncio. Offline, deterministic. |
| `live_feed.py` | Real Binance WebSocket trade stream → the same engine and dashboard. |

## Run it

```bash
python3 risk_engine.py          # self-check + throughput benchmark  (no deps)
python3 simulator.py            # watch a 20x long get liquidated on demand  (no deps)

pip install -r requirements.txt
python3 live_feed.py            # live 20x long BTCUSDT off real market data
python3 live_feed.py ethusdt 50 # 50x long ETHUSDT
```

The engine and simulator need only the standard library; the live feed needs
`websockets`.

## The math

Linear (USDT-margined) perpetual futures:

```
uPnL     = side * size * (mark - entry)      # side: +1 long, -1 short
maint    = size * mark * mmr                 # mmr = maintenance margin rate
long liq  = (size*entry - C) / (size * (1 - mmr))
short liq = (C + size*entry) / (size * (1 + mmr))
```

`C` is the margin backing the position: the position's own margin (isolated),
or wallet + other positions' P&L − their maintenance (cross).

## Not built yet

Persistence (Redis/Postgres), a web/gRPC API, and containerization. The engine
is in-memory, so state resets on restart. `mmr` is a single flat rate; real
exchanges tier it by position size.
