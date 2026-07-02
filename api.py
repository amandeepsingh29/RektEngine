"""RektEngine HTTP/WebSocket service.

Wraps the multi-account RiskBook in a FastAPI app:

  POST /accounts                     create an account
  POST /accounts/{id}/positions      open a leveraged position
  GET  /accounts/{id}                risk snapshot (equity, P&L, liq prices)
  POST /tick                         inject a price tick (manual / testing)
  WS   /ws                           stream ticks + liquidation events

A background task random-walks the price of every symbol that has open
positions and feeds it through the book, so the WS stream is live without any
external feed. Set AUTO_FEED = False (or env RE_AUTO_FEED=0) to drive prices
only via POST /tick — the deterministic path used by tests.

    uvicorn api:app --reload
"""
from __future__ import annotations

import asyncio
import json
import os
import random
from contextlib import asynccontextmanager
from pathlib import Path

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from book import RiskBook
from persistence import NullStore, PgStore
from risk_engine import LONG, SHORT

AUTO_FEED = os.environ.get("RE_AUTO_FEED", "1") != "0"
FEED_INTERVAL = 0.5  # seconds between auto-feed ticks
DATABASE_URL = os.environ.get("DATABASE_URL")  # e.g. postgresql://localhost/rektengine
REDIS_URL = os.environ.get("REDIS_URL")        # e.g. redis://localhost:6379
CHANNEL = "rekt:events"

book = RiskBook()
prices: dict[str, float] = {}          # symbol -> current mark
clients: set[WebSocket] = set()
store = NullStore()                    # replaced with PgStore at startup if configured
redis_client = None                    # set at startup if REDIS_URL is reachable


# --- request bodies --------------------------------------------------------
class NewAccount(BaseModel):
    id: str
    balance: float = 10_000.0
    cross: bool = True


class NewPosition(BaseModel):
    symbol: str
    side: str            # "long" | "short"
    size: float
    entry: float
    leverage: float
    mmr: float = 0.005


class Tick(BaseModel):
    symbol: str
    price: float


# --- helpers ---------------------------------------------------------------
def snapshot(acct_id: str) -> dict:
    acct = book.accounts.get(acct_id)
    if acct is None:
        raise HTTPException(404, f"no account {acct_id!r}")
    positions = []
    for sym, p in acct.positions.items():
        mark = acct.marks[sym]
        positions.append({
            "symbol": sym,
            "side": "long" if p.side == LONG else "short",
            "size": p.size, "entry": p.entry, "leverage": p.leverage,
            "mark": mark,
            "upnl": p.upnl(mark),
            "liq_price": acct.liq_price(sym),
        })
    ratio = acct.margin_ratio()
    return {
        "id": acct_id, "cross": acct.cross,
        "balance": acct.balance, "equity": acct.equity(),
        # inf means no open maintenance requirement -> no liquidation risk
        "margin_ratio": None if ratio == float("inf") else ratio,
        "realized": acct.realized,
        "positions": positions,
    }


async def broadcast_local(msg: dict):
    """Send to WebSocket clients connected to THIS process."""
    dead = []
    for ws in clients:
        try:
            await ws.send_json(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


async def publish(msg: dict):
    """Fan an event out to every worker (via Redis) or, with no Redis, to the
    local clients directly."""
    if redis_client is not None:
        await redis_client.publish(CHANNEL, json.dumps(msg))
    else:
        await broadcast_local(msg)


async def _subscribe():
    """Relay events published to Redis onto this process's WS clients."""
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(CHANNEL)
    async for message in pubsub.listen():
        if message["type"] == "message":
            await broadcast_local(json.loads(message["data"]))


async def process_tick(symbol: str, price: float) -> list[dict]:
    prices[symbol] = price
    raw = book.on_tick(symbol, price)          # [(acct_id, symbol, exit_price, pnl)]

    # persist liquidations: log each, then drop closed positions and update wallets
    closed: dict[str, list[str]] = {}
    for acct_id, sym, px, pnl in raw:
        await store.record_liquidation(acct_id, sym, px, pnl)
        closed.setdefault(acct_id, []).append(sym)
    for acct_id, symbols in closed.items():
        await store.delete_positions(acct_id, symbols)
        await store.upsert_account(acct_id, book.accounts[acct_id])

    events = [{"acct_id": a, "symbol": s, "exit_price": px, "realized_pnl": pnl}
              for a, s, px, pnl in raw]
    await publish({"type": "tick", "symbol": symbol, "price": price})
    for ev in events:
        await publish({"type": "liquidation", **ev})
    return events


async def _auto_feed():
    # ponytail: naive independent random walk per symbol; good enough to make
    # the stream live. Swap in live_feed's Binance source for real prices.
    while True:
        await asyncio.sleep(FEED_INTERVAL)
        for symbol in list(book._by_symbol):
            if not book.holders(symbol):
                continue
            price = round(prices.get(symbol, 100.0) * (1 + random.gauss(0, 0.003)), 2)
            await process_tick(symbol, price)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global store, redis_client
    if DATABASE_URL:
        try:
            store = await PgStore.connect(DATABASE_URL)
            await store.load_into(book, prices)
            print(f"[persistence] Postgres connected; loaded {len(book.accounts)} accounts")
        except Exception as e:
            print(f"[persistence] Postgres unavailable ({e}); running in-memory")
            store = NullStore()

    tasks = []
    if REDIS_URL:
        try:
            redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
            await redis_client.ping()
            tasks.append(asyncio.create_task(_subscribe()))
            print("[events] Redis connected; broadcasting via pub/sub")
        except Exception as e:
            print(f"[events] Redis unavailable ({e}); broadcasting in-process")
            redis_client = None

    if AUTO_FEED:
        tasks.append(asyncio.create_task(_auto_feed()))
    yield
    for t in tasks:
        t.cancel()
    if redis_client is not None:
        await redis_client.aclose()
    await store.close()


app = FastAPI(title="RektEngine", lifespan=lifespan)


# --- routes ----------------------------------------------------------------
@app.post("/accounts")
async def create_account(body: NewAccount):
    if body.id in book.accounts:
        raise HTTPException(409, f"account {body.id!r} exists")
    acct = book.add_account(body.id, body.balance, body.cross)
    await store.upsert_account(body.id, acct)
    return {"id": body.id, "balance": body.balance, "cross": body.cross}


@app.post("/accounts/{acct_id}/positions")
async def open_position(acct_id: str, body: NewPosition):
    if acct_id not in book.accounts:
        raise HTTPException(404, f"no account {acct_id!r}")
    side = {"long": LONG, "short": SHORT}.get(body.side.lower())
    if side is None:
        raise HTTPException(422, "side must be 'long' or 'short'")
    pos = book.open(acct_id, body.symbol, side, body.size, body.entry,
                    body.leverage, body.mmr)
    prices.setdefault(body.symbol, body.entry)
    await store.upsert_position(acct_id, pos)
    await store.upsert_account(acct_id, book.accounts[acct_id])  # isolated margin moved
    return snapshot(acct_id)


@app.get("/accounts")
def list_accounts():
    return [snapshot(a) for a in book.accounts]


@app.get("/accounts/{acct_id}")
def get_account(acct_id: str):
    return snapshot(acct_id)


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return Path(__file__).with_name("dashboard.html").read_text()


@app.post("/tick")
async def tick(body: Tick):
    events = await process_tick(body.symbol, body.price)
    return {"symbol": body.symbol, "price": body.price, "liquidations": events}


@app.websocket("/ws")
async def ws_stream(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            await ws.receive_text()   # ignore input; keeps the socket open
    except WebSocketDisconnect:
        pass
    finally:
        clients.discard(ws)
