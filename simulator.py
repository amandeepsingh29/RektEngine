"""Market simulator: mock price feed -> risk engine -> auto-liquidation.

Simulates a live exchange. Stdlib asyncio stands in for a WebSocket feed: a
producer coroutine streams price ticks onto a queue, the engine applies each
one and renders a dashboard. A downward random walk eventually breaches the
isolated long's liquidation price and the engine auto-liquidates it.

    python3 simulator.py

Swap the producer for a real `websockets` client (Binance/Bybit trade
stream) and nothing else changes — the engine only sees (symbol, price).
"""
import asyncio
import random

from risk_engine import Account, LONG


async def price_feed(q: asyncio.Queue, symbol: str, start: float, steps: int):
    """Stand-in for a WebSocket market feed: streams `steps` ticks, then closes."""
    price = start
    for _ in range(steps):
        price *= 1 + random.gauss(-0.0015, 0.004)  # slight downward drift
        await q.put((symbol, round(price, 2)))
        await asyncio.sleep(0.02)
    await q.put(None)  # sentinel: feed closed


def render(acct: Account, symbol: str, price: float):
    p = acct.positions.get(symbol)
    liq = acct.liq_price(symbol)
    dist = (price - liq) / price * 100
    print(
        f"\r{symbol} {price:>10.2f} | uPnL {p.upnl(price):>9.2f} "
        f"| equity {acct.equity():>10.2f} | liq {liq:>9.2f} "
        f"| ratio {acct.margin_ratio():>6.2f} | to-liq {dist:>6.2f}%   ",
        end="", flush=True,
    )


async def run():
    acct = Account(balance=10_000, cross=False)
    acct.open("BTC", LONG, 1.0, 30_000.0, 20.0)  # 20x long, tight liq band
    print(f"opened 20x LONG BTC @ 30000  liq={acct.liq_price('BTC'):.2f}")

    q: asyncio.Queue = asyncio.Queue()
    asyncio.create_task(price_feed(q, "BTC", 30_000.0, 500))

    while (tick := await q.get()) is not None:
        symbol, price = tick
        events = acct.apply_mark(symbol, price)
        if events:
            _, exit_px, pnl = events[0]
            print(f"\nLIQUIDATED {symbol} @ {exit_px:.2f}  realized PnL {pnl:.2f}"
                  f"  wallet now {acct.balance:.2f}")
            return
        render(acct, symbol, price)
    print("\nfeed closed — position survived")


if __name__ == "__main__":
    asyncio.run(run())
