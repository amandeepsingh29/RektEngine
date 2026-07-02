"""RiskBook — the multi-account engine loop.

A real exchange holds thousands of accounts and must, on every price tick,
liquidate exactly those whose positions crossed maintenance margin — without
scanning everyone. RiskBook does that:

  - accounts are indexed by symbol (`_by_symbol`), so a BTC tick only touches
    accounts actually holding BTC exposure;
  - `on_tick(symbol, price)` fans the tick out to those accounts, reusing each
    Account's own liquidation logic, and returns the liquidation events;
  - the index self-heals as positions close (including cross accounts, where
    one liquidation closes every position the account holds).

Why index-by-symbol matters: a BTC move only changes the equity of accounts
with BTC exposure, so rechecking anyone else is wasted work. This is the
difference between O(accounts) and O(holders-of-this-symbol) per tick.
"""
from __future__ import annotations

from risk_engine import Account


class RiskBook:
    def __init__(self):
        self.accounts: dict[str, Account] = {}
        self._by_symbol: dict[str, set[str]] = {}  # symbol -> account ids holding it

    def add_account(self, acct_id: str, balance: float, cross: bool = True) -> Account:
        acct = Account(balance=balance, cross=cross)
        self.accounts[acct_id] = acct
        return acct

    def open(self, acct_id, symbol, side, size, entry, leverage, mmr=0.005):
        pos = self.accounts[acct_id].open(symbol, side, size, entry, leverage, mmr)
        self._by_symbol.setdefault(symbol, set()).add(acct_id)
        return pos

    def on_tick(self, symbol, price):
        """Route one price tick to every account exposed to `symbol`.
        Returns a list of (acct_id, symbol, exit_price, realized_pnl) events."""
        holders = self._by_symbol.get(symbol)
        if not holders:
            return []
        events = []
        for acct_id in list(holders):          # list(): index mutates during close
            acct = self.accounts[acct_id]
            held = set(acct.positions)
            evs = acct.apply_mark(symbol, price)
            if evs:
                # a cross liquidation closes every symbol the account held;
                # drop the account from each symbol's index.
                for s in held - set(acct.positions):
                    self._by_symbol[s].discard(acct_id)
                events.extend((acct_id, s, px, pnl) for s, px, pnl in evs)
        return events

    def holders(self, symbol) -> int:
        return len(self._by_symbol.get(symbol, ()))


# ---------------------------------------------------------------------------
# Runnable checks: correctness asserts + fan-out throughput benchmark.
# ---------------------------------------------------------------------------
def _self_check():
    from risk_engine import LONG, SHORT

    book = RiskBook()
    # Two isolated BTC longs at different leverage: the 20x liquidates first.
    book.add_account("risky", 10_000, cross=False)
    book.add_account("safe", 10_000, cross=False)
    book.open("risky", "BTC", LONG, 1.0, 100.0, 20.0)   # liq ~95.2
    book.open("safe", "BTC", LONG, 1.0, 100.0, 5.0)     # liq ~80.4

    ev = book.on_tick("BTC", 94.0)
    assert len(ev) == 1 and ev[0][0] == "risky", ev     # only the 20x dies
    assert "BTC" not in book.accounts["risky"].positions
    assert "BTC" in book.accounts["safe"].positions
    assert book.holders("BTC") == 1                      # index shed the dead one

    # Symbol isolation: an ETH tick must not touch a BTC-only account.
    book2 = RiskBook()
    btc_only = book2.add_account("a", 10_000, cross=False)
    book2.open("a", "BTC", LONG, 1.0, 100.0, 20.0)
    book2.add_account("b", 10_000, cross=False)
    book2.open("b", "ETH", SHORT, 1.0, 100.0, 20.0)
    assert book2.on_tick("ETH", 50.0) == []             # ETH crash, but 'a' holds BTC
    assert btc_only.marks["BTC"] == 100.0               # 'a' never re-marked
    assert book2.on_tick("ETH", 105.0)                  # short liq ~104.48, breached

    # Cross account: a tick liquidates the whole account and clears every index.
    book3 = RiskBook()
    book3.add_account("c", 100, cross=True)
    book3.open("c", "BTC", LONG, 5.0, 100.0, 10.0)
    book3.open("c", "ETH", LONG, 5.0, 100.0, 10.0)
    ev = book3.on_tick("BTC", 60.0)                      # BTC crash sinks account
    closed = {e[1] for e in ev}
    assert closed == {"BTC", "ETH"}, ev                 # both positions closed
    assert book3.holders("BTC") == 0 and book3.holders("ETH") == 0
    print("self-check: OK")


def _benchmark(n_accounts=5_000, n_ticks=200):
    import time, random

    book = RiskBook()
    from risk_engine import LONG
    for i in range(n_accounts):
        book.add_account(str(i), 1_000_000, cross=False)
        book.open(str(i), "BTC", LONG, 1.0, 100_000.0, 5.0)  # wide band, survives
    prices = [100_000.0 + random.uniform(-500, 500) for _ in range(n_ticks)]

    t0 = time.perf_counter()
    for px in prices:
        book.on_tick("BTC", px)
    dt = time.perf_counter() - t0

    pos_updates = n_ticks * n_accounts
    print(f"benchmark: {n_ticks} ticks x {n_accounts:,} accounts "
          f"= {pos_updates:,} position-updates in {dt:.3f}s "
          f"-> {pos_updates / dt:,.0f} updates/sec")
    assert pos_updates / dt > 100_000, "below target"


if __name__ == "__main__":
    _self_check()
    _benchmark()
