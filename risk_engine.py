"""Real-time leveraged risk engine — core.

Linear (USDT-margined) perpetual futures model. Single-account, in-memory.
The hot path is `apply_mark`: on each price tick it recomputes risk and
liquidates if the account/position breached maintenance margin.

Money math (per position, linear perps):
    uPnL      = side * size * (mark - entry)          side: +1 long, -1 short
    notional  = size * mark
    maint     = notional * mmr                        mmr = maintenance margin rate

Liquidation buffer C (available margin backing the position):
    isolated:  C = position.margin                    (only its own margin)
    cross:     C = balance + Σ uPnL(others) - Σ maint(others)

Liquidation price solves  C + uPnL(P) = maint(P):
    long:   P = (size*entry - C) / (size * (1 - mmr))
    short:  P = (C + size*entry) / (size * (1 + mmr))

MMR is a calibration knob: real exchanges use notional-tiered maintenance
margin (bigger positions -> higher mmr). One flat rate here; swap in a tier
table if you need Binance-accurate liq prices.
"""
from __future__ import annotations
from dataclasses import dataclass, field

LONG, SHORT = 1, -1


@dataclass
class Position:
    symbol: str
    side: int          # +1 long, -1 short
    size: float        # base qty, > 0
    entry: float
    leverage: float
    mmr: float = 0.005  # maintenance margin rate (0.5%)
    margin: float = 0.0  # allocated margin (isolated); reference only for cross

    def upnl(self, mark: float) -> float:
        return self.side * self.size * (mark - self.entry)

    def maint(self, mark: float) -> float:
        return self.size * mark * self.mmr


@dataclass
class Account:
    balance: float                       # wallet balance (realized funds)
    cross: bool = True
    positions: dict[str, Position] = field(default_factory=dict)
    marks: dict[str, float] = field(default_factory=dict)
    realized: float = 0.0

    # --- position management ---------------------------------------------
    def open(self, symbol, side, size, entry, leverage, mmr=0.005) -> Position:
        margin = size * entry / leverage
        p = Position(symbol, side, size, entry, leverage, mmr, margin)
        if not self.cross:
            self.balance -= margin      # isolated: lock margin out of wallet
        self.positions[symbol] = p
        self.marks[symbol] = entry
        return p

    def close(self, symbol, price) -> float:
        """Close at `price`, realize PnL, return realized amount for this close."""
        p = self.positions.pop(symbol)
        pnl = p.upnl(price)
        # isolated: return the locked margin plus/minus pnl, floored at 0 (bankruptcy)
        credit = max(0.0, p.margin + pnl) if not self.cross else pnl
        self.balance += credit
        self.realized += pnl
        self.marks.pop(symbol, None)
        return pnl

    # --- risk ------------------------------------------------------------
    def unrealized(self) -> float:
        return sum(p.upnl(self.marks[s]) for s, p in self.positions.items())

    def equity(self) -> float:
        return self.balance + self.unrealized()

    def maintenance(self) -> float:
        return sum(p.maint(self.marks[s]) for s, p in self.positions.items())

    def margin_ratio(self) -> float:
        """equity / maintenance. <= 1.0 means liquidation territory."""
        m = self.maintenance()
        return float("inf") if m == 0 else self.equity() / m

    def liq_price(self, symbol) -> float:
        """Mark price of `symbol` at which it (isolated) or the account (cross)
        gets liquidated, holding all other positions' marks fixed."""
        p = self.positions[symbol]
        if self.cross:
            c = self.balance
            for s, o in self.positions.items():
                if s == symbol:
                    continue
                c += o.upnl(self.marks[s]) - o.maint(self.marks[s])
        else:
            c = p.margin
        if p.side == LONG:
            return (p.size * p.entry - c) / (p.size * (1 - p.mmr))
        return (c + p.size * p.entry) / (p.size * (1 + p.mmr))

    # --- hot path --------------------------------------------------------
    def apply_mark(self, symbol, price):
        """Process one price tick. Returns list of (symbol, price, pnl)
        liquidation events (usually empty)."""
        self.marks[symbol] = price
        events = []
        if self.cross:
            if self.positions and self.equity() <= self.maintenance():
                for s in list(self.positions):
                    events.append((s, self.marks[s], self.close(s, self.marks[s])))
        else:
            p = self.positions.get(symbol)
            if p is not None:
                liq = self.liq_price(symbol)
                breached = price <= liq if p.side == LONG else price >= liq
                if breached:
                    events.append((symbol, price, self.close(symbol, price)))
        return events


# ---------------------------------------------------------------------------
# Runnable checks: correctness asserts + throughput benchmark.
# ---------------------------------------------------------------------------
def _self_check():
    # Isolated long 10x: entry 100, size 1, mmr 0.5% -> liq ~90.45
    a = Account(balance=1000, cross=False)
    a.open("BTC", LONG, 1.0, 100.0, 10.0)
    assert abs(a.liq_price("BTC") - 90.4523) < 1e-3, a.liq_price("BTC")
    assert a.apply_mark("BTC", 95.0) == []          # not liquidated yet
    ev = a.apply_mark("BTC", 90.0)                   # crosses liq price
    assert ev and ev[0][0] == "BTC", ev
    assert "BTC" not in a.positions

    # Isolated short 10x: entry 100 -> liq ~109.45
    b = Account(balance=1000, cross=False)
    b.open("ETH", SHORT, 1.0, 100.0, 10.0)
    assert abs(b.liq_price("ETH") - 109.4527) < 1e-3, b.liq_price("ETH")
    assert b.apply_mark("ETH", 105.0) == []
    assert b.apply_mark("ETH", 110.0)               # liquidated

    # PnL signs: long profits when price rises
    c = Account(balance=1000, cross=False)
    p = c.open("BTC", LONG, 2.0, 100.0, 5.0)
    assert p.upnl(110.0) == 20.0
    assert p.upnl(90.0) == -20.0

    # Cross: two positions share the wallet; a winner delays the loser's liq.
    d = Account(balance=1000, cross=True)
    d.open("BTC", LONG, 10.0, 100.0, 10.0)   # margin ~100
    liq_alone = d.liq_price("BTC")
    d.open("ETH", LONG, 10.0, 100.0, 10.0)
    d.apply_mark("ETH", 130.0)               # ETH in profit -> raises BTC buffer
    assert d.liq_price("BTC") < liq_alone, (d.liq_price("BTC"), liq_alone)

    # Cross liquidation fires on account equity, not a single position's liq line
    e = Account(balance=100, cross=True)
    e.open("BTC", LONG, 10.0, 100.0, 10.0)   # notional 1000, only 100 wallet
    ev = e.apply_mark("BTC", 91.0)           # equity ~10, maint ~4.55 -> survives
    assert ev == [], (e.equity(), e.maintenance())
    ev = e.apply_mark("BTC", 89.0)           # equity ~ -10 < maint -> liquidate
    assert ev, (e.equity(), e.maintenance())
    print("self-check: OK")


def _benchmark(n=2_000_000):
    import time, random
    a = Account(balance=1_000_000, cross=False)
    a.open("BTC", LONG, 1.0, 100_000.0, 5.0)  # 5x, wide liq band so it survives
    # keep price near entry so we measure the risk path, not repeated re-opens
    prices = [100_000.0 + random.uniform(-500, 500) for _ in range(10_000)]
    apply = a.apply_mark
    t0 = time.perf_counter()
    for i in range(n):
        apply("BTC", prices[i % 10_000])
    dt = time.perf_counter() - t0
    rate = n / dt
    print(f"benchmark: {n:,} updates in {dt:.3f}s -> {rate:,.0f} updates/sec")
    assert rate > 100_000, f"below target: {rate:,.0f}/s"


if __name__ == "__main__":
    _self_check()
    _benchmark()
