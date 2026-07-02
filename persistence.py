"""Postgres persistence for the risk book.

Durable state: accounts, their open positions, and an append-only log of
liquidations. On startup `load_into` rebuilds the in-memory RiskBook so the
engine survives a restart.

Two stores share one interface so the API works with or without a database:
  - PgStore  — writes through to Postgres (asyncpg pool).
  - NullStore — no-ops; used when DATABASE_URL is unset or Postgres is down.

Rebuild note: positions are reconstructed directly rather than via
`RiskBook.open`, because open() deducts isolated margin from the wallet — and
the stored balance already reflects that deduction. Re-running it would double
charge. See [[risk_engine]] Account.open.
"""
from __future__ import annotations

import asyncpg

from book import RiskBook
from risk_engine import Position

SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    id       TEXT PRIMARY KEY,
    balance  DOUBLE PRECISION NOT NULL,
    cross_margin BOOLEAN NOT NULL,
    realized DOUBLE PRECISION NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS positions (
    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    symbol     TEXT NOT NULL,
    side       SMALLINT NOT NULL,
    size       DOUBLE PRECISION NOT NULL,
    entry      DOUBLE PRECISION NOT NULL,
    leverage   DOUBLE PRECISION NOT NULL,
    mmr        DOUBLE PRECISION NOT NULL,
    margin     DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (account_id, symbol)
);
CREATE TABLE IF NOT EXISTS liquidations (
    id           BIGSERIAL PRIMARY KEY,
    account_id   TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    exit_price   DOUBLE PRECISION NOT NULL,
    realized_pnl DOUBLE PRECISION NOT NULL,
    ts           TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


class PgStore:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @classmethod
    async def connect(cls, dsn: str) -> "PgStore":
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
        async with pool.acquire() as con:
            await con.execute(SCHEMA)
        return cls(pool)

    async def close(self):
        await self.pool.close()

    async def load_into(self, book: RiskBook, prices: dict[str, float]):
        async with self.pool.acquire() as con:
            for r in await con.fetch("SELECT id, balance, cross_margin, realized FROM accounts"):
                acct = book.add_account(r["id"], r["balance"], r["cross_margin"])
                acct.realized = r["realized"]
            for r in await con.fetch("SELECT * FROM positions"):
                acct = book.accounts[r["account_id"]]
                # reconstruct directly — do NOT call book.open (see module docstring)
                acct.positions[r["symbol"]] = Position(
                    r["symbol"], r["side"], r["size"], r["entry"],
                    r["leverage"], r["mmr"], r["margin"])
                acct.marks[r["symbol"]] = r["entry"]
                book._by_symbol.setdefault(r["symbol"], set()).add(r["account_id"])
                prices.setdefault(r["symbol"], r["entry"])

    async def upsert_account(self, acct_id: str, acct):
        await self.pool.execute(
            """INSERT INTO accounts (id, balance, cross_margin, realized)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (id) DO UPDATE
               SET balance = $2, realized = $4""",
            acct_id, acct.balance, acct.cross, acct.realized)

    async def upsert_position(self, acct_id: str, p: Position):
        await self.pool.execute(
            """INSERT INTO positions
               (account_id, symbol, side, size, entry, leverage, mmr, margin)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (account_id, symbol) DO UPDATE
               SET side=$3, size=$4, entry=$5, leverage=$6, mmr=$7, margin=$8""",
            acct_id, p.symbol, p.side, p.size, p.entry, p.leverage, p.mmr, p.margin)

    async def delete_positions(self, acct_id: str, symbols: list[str]):
        await self.pool.execute(
            "DELETE FROM positions WHERE account_id=$1 AND symbol = ANY($2::text[])",
            acct_id, symbols)

    async def record_liquidation(self, acct_id, symbol, exit_price, pnl):
        await self.pool.execute(
            """INSERT INTO liquidations (account_id, symbol, exit_price, realized_pnl)
               VALUES ($1, $2, $3, $4)""",
            acct_id, symbol, exit_price, pnl)


class NullStore:
    """No-op store: the engine runs fully in-memory."""
    async def close(self): pass
    async def load_into(self, book, prices): pass
    async def upsert_account(self, acct_id, acct): pass
    async def upsert_position(self, acct_id, p): pass
    async def delete_positions(self, acct_id, symbols): pass
    async def record_liquidation(self, acct_id, symbol, exit_price, pnl): pass
