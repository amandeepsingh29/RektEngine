"""Live market feed: Binance WebSocket trade stream -> risk engine.

Connects to Binance's public trade stream (no API key needed), opens a
leveraged position at the first real tick, and tracks its risk live —
auto-liquidating if the market crosses the liquidation price.

    python3 live_feed.py                 # 20x long BTCUSDT
    python3 live_feed.py ethusdt 50      # 50x long ETHUSDT

This is the real-data counterpart to simulator.py; the engine and the
dashboard are identical — only the price source changed. A live liquidation
needs a real adverse move, so it may not fire in a short session; use
simulator.py when you want to watch one happen on demand.
"""
import asyncio
import json
import sys

import websockets

from risk_engine import Account, LONG
from simulator import render

STREAM = "wss://stream.binance.com:9443/ws/{symbol}@trade"


async def run(symbol: str = "btcusdt", leverage: float = 20.0):
    url = STREAM.format(symbol=symbol)
    acct = Account(balance=10_000, cross=False)
    key = symbol.upper()
    opened = False

    print(f"connecting to {url} ...")
    # ponytail: single connection, no auto-reconnect. Wrap in a retry loop
    # if you need it to survive dropped sockets.
    async with websockets.connect(url) as ws:
        async for raw in ws:
            price = float(json.loads(raw)["p"])
            if not opened:  # anchor the position to the first live price
                acct.open(key, LONG, 1.0, price, leverage)
                print(f"opened {leverage:g}x LONG {key} @ {price:.2f}  "
                      f"liq={acct.liq_price(key):.2f}")
                opened = True
                continue
            events = acct.apply_mark(key, price)
            if events:
                _, exit_px, pnl = events[0]
                print(f"\nLIQUIDATED {key} @ {exit_px:.2f}  realized PnL {pnl:.2f}"
                      f"  wallet now {acct.balance:.2f}")
                return
            render(acct, key, price)


if __name__ == "__main__":
    sym = sys.argv[1] if len(sys.argv) > 1 else "btcusdt"
    lev = float(sys.argv[2]) if len(sys.argv) > 2 else 20.0
    try:
        asyncio.run(run(sym, lev))
    except KeyboardInterrupt:
        print("\nstopped")
