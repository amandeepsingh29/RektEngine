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
| `book.py` | Multi-account loop: holds many accounts, indexes positions by symbol, fans each tick out to only the exposed accounts, liquidates who breached. |
| `simulator.py` | Mock market feed → engine → live dashboard → auto-liquidation, over asyncio. Offline, deterministic. |
| `live_feed.py` | Real Binance WebSocket trade stream → the same engine and dashboard. |
| `api.py` | FastAPI service: REST to create accounts / open positions / query risk, a background auto-feed, and a WebSocket that streams ticks + liquidation events. |
| `dashboard.html` | Single-file live dashboard (vanilla JS, no build): KPIs, positions table with distance-to-liquidation, and a liquidation feed off `/ws`. Served at `/`. |
| `test_api.py` | End-to-end check of the API (REST + WS liquidation), auto-feed disabled for determinism. |

## Run it

```bash
python3 risk_engine.py          # self-check + throughput benchmark  (no deps)
python3 book.py                 # multi-account checks + fan-out benchmark  (no deps)
python3 simulator.py            # watch a 20x long get liquidated on demand  (no deps)

pip install -r requirements.txt
python3 live_feed.py            # live 20x long BTCUSDT off real market data
python3 live_feed.py ethusdt 50 # 50x long ETHUSDT
```

The engine and simulator need only the standard library; the live feed needs
`websockets`.

### The service

```bash
pip install -r requirements.txt
uvicorn api:app --reload         # dashboard at /, API docs at /docs
python3 test_api.py              # end-to-end REST + WebSocket check
```

```
POST /accounts                   create an account
POST /accounts/{id}/positions    open a leveraged position
GET  /accounts/{id}              risk snapshot (equity, P&L, liq prices)
POST /tick                       inject a price tick
WS   /ws                         stream ticks + liquidation events
```

A background task random-walks the price of every symbol with open positions
and streams it over `/ws`. Set `RE_AUTO_FEED=0` to drive prices only via
`POST /tick`.

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
