"""End-to-end check of the API: REST create/open/query + a WS liquidation.

Auto-feed is disabled so prices move only via POST /tick — deterministic.

    python3 test_api.py
"""
import os

os.environ["RE_AUTO_FEED"] = "0"

from fastapi.testclient import TestClient  # noqa: E402

import api  # noqa: E402


def main():
    with TestClient(api.app) as c:
        # create an isolated account
        assert c.post("/accounts",
                      json={"id": "u1", "balance": 10_000, "cross": False}
                      ).status_code == 200

        # open a 20x long BTC @ 100  -> liq ~95.2, margin 5 locked
        r = c.post("/accounts/u1/positions",
                   json={"symbol": "BTC", "side": "long", "size": 1,
                         "entry": 100, "leverage": 20})
        assert r.status_code == 200, r.text
        pos = r.json()["positions"][0]
        assert 94 < pos["liq_price"] < 96, pos

        # snapshot: isolated locks 5 margin, equity 9995 at entry mark
        snap = c.get("/accounts/u1").json()
        assert snap["balance"] == 9995 and snap["equity"] == 9995, snap

        # open a WS stream, then drive a tick that liquidates over it
        with c.websocket_connect("/ws") as ws:
            r = c.post("/tick", json={"symbol": "BTC", "price": 94.0})
            liqs = r.json()["liquidations"]
            assert len(liqs) == 1 and liqs[0]["acct_id"] == "u1", r.json()

            assert ws.receive_json()["type"] == "tick"
            liq_msg = ws.receive_json()
            assert liq_msg["type"] == "liquidation" and liq_msg["symbol"] == "BTC", liq_msg

        # position is gone after liquidation
        assert c.get("/accounts/u1").json()["positions"] == []

        # unknown account -> 404
        assert c.get("/accounts/nope").status_code == 404

    print("api test: OK")


if __name__ == "__main__":
    main()
