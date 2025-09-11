#!/usr/bin/env python3
"""
binance_futures_imbalance.py
Realtime tracker for Binance Futures BTCUSDT aggTrade stream.
Calculates buy/sell imbalance for 15m, 30m, 1h windows and writes alerts to CSV
when imbalance_pct >= 40% or <= -40%.
"""

import asyncio
import json
import csv
import signal
import sys
from collections import deque
from datetime import datetime, timezone
import websockets  # pip install websockets

# Websocket endpoint for Binance Futures aggTrade BTCUSDT
WS_ENDPOINT = "wss://fstream.binance.com/ws/btcusdt@aggTrade"

# Rolling windows in seconds
WINDOWS = {
    "15m": 15 * 60,
    "30m": 30 * 60,
    "1h": 60 * 60,
}

# Imbalance threshold (fraction), e.g. 0.4 => 40%
IMBALANCE_THRESHOLD = 0.4

# CSV file for alerts
ALERTS_CSV = "alerts.csv"

# How often (in seconds) to evaluate windows (can be 1)
EVAL_INTERVAL = 1.0

# Trade buffer: store tuples (trade_time_ms, qty_float, side_str, price_str, agg_trade_id)
# side_str is "buy" for aggressive buy, "sell" for aggressive sell
trade_buffer = deque()

# Graceful shutdown flag
stop_event = asyncio.Event()


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def save_alert_row(row):
    """Append a row (dict) to the CSV file. Creates header if file doesn't exist."""
    header = ["timestamp", "window", "buy_qty", "sell_qty", "imbalance_pct", "total_trades", "last_trade_price", "last_trade_id"]
    # Try to write header only if file is new
    try:
        with open(ALERTS_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            # If file is empty, write header
            f.seek(0, 2)
            if f.tell() == 0:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        print(f"[{iso_now()}] Failed to write alert to CSV: {e}")


def classify_side_from_agg(maker_flag):
    """
    For Binance aggTrade:
      m == True  -> buyer is maker -> aggressor was the seller (so it's a sell)
      m == False -> buyer is taker -> aggressor was the buyer (so it's a buy)
    Return "buy" or "sell".
    """
    return "sell" if maker_flag else "buy"


def prune_old_trades(now_ms):
    """Remove trades older than the largest window from the left of deque."""
    max_window = max(WINDOWS.values()) * 1000
    threshold = now_ms - max_window
    while trade_buffer and trade_buffer[0][0] < threshold:
        trade_buffer.popleft()


def compute_window_stats(window_seconds, now_ms):
    """Compute buy and sell quantities for trades within last window_seconds."""
    window_ms = window_seconds * 1000
    threshold = now_ms - window_ms
    buy_qty = 0.0
    sell_qty = 0.0
    total_trades = 0
    last_trade_price = None
    last_trade_id = None

    # iterate from right to left (newest first) until threshold
    for t_ms, qty, side, price, trade_id in reversed(trade_buffer):
        if t_ms < threshold:
            break
        total_trades += 1
        if side == "buy":
            buy_qty += qty
        else:
            sell_qty += qty
        if last_trade_price is None:
            last_trade_price = price
            last_trade_id = trade_id

    return {
        "buy_qty": buy_qty,
        "sell_qty": sell_qty,
        "total_trades": total_trades,
        "last_trade_price": last_trade_price,
        "last_trade_id": last_trade_id,
    }


async def evaluate_and_alert():
    """Periodically evaluate windows and write alert rows when threshold crossed."""
    while not stop_event.is_set():
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        prune_old_trades(now_ms)

        for name, secs in WINDOWS.items():
            stats = compute_window_stats(secs, now_ms)
            buy = stats["buy_qty"]
            sell = stats["sell_qty"]
            total = stats["total_trades"]
            if buy + sell == 0:
                imbalance = 0.0
            else:
                imbalance = (buy - sell) / (buy + sell)  # -1..1

            imbalance_pct = imbalance * 100.0

            if abs(imbalance) >= IMBALANCE_THRESHOLD:
                # Compose alert row and save
                row = {
                    "timestamp": iso_now(),
                    "window": name,
                    "buy_qty": f"{buy:.8f}",
                    "sell_qty": f"{sell:.8f}",
                    "imbalance_pct": f"{imbalance_pct:.4f}",
                    "total_trades": total,
                    "last_trade_price": stats["last_trade_price"],
                    "last_trade_id": stats["last_trade_id"],
                }
                print(f"[ALERT {iso_now()}] window={name} imbalance_pct={imbalance_pct:.2f}% buys={buy:.6f} sells={sell:.6f} trades={total}")
                save_alert_row(row)
        await asyncio.sleep(EVAL_INTERVAL)


async def handle_ws():
    """Connect to websocket and ingest aggTrade messages into trade_buffer."""
    reconnect_delay = 1
    while not stop_event.is_set():
        try:
            print(f"[{iso_now()}] Connecting to {WS_ENDPOINT}")
            async with websockets.connect(WS_ENDPOINT, ping_interval=20, ping_timeout=10) as ws:
                print(f"[{iso_now()}] Connected.")
                reconnect_delay = 1  # reset
                async for raw in ws:
                    if stop_event.is_set():
                        break
                    try:
                        msg = json.loads(raw)
                        # AggTrade message structure (example):
                        # {
                        #   "e": "aggTrade",
                        #   "E": 123456789,      # event time
                        #   "s": "BTCUSDT",
                        #   "a": 12345,          # aggregate trade id
                        #   "p": "0.001",        # price
                        #   "q": "100",          # quantity
                        #   "f": 100,            # first trade id
                        #   "l": 105,            # last trade id
                        #   "T": 123456785,      # trade time
                        #   "m": true,           # whether buyer is maker
                        #   "M": true            # ignore
                        # }
                        if msg.get("e") != "aggTrade":
                            continue
                        trade_time_ms = int(msg.get("T", msg.get("E", 0)))
                        price = msg.get("p")
                        qty = float(msg.get("q", 0.0))
                        agg_id = msg.get("a")
                        maker_flag = msg.get("m", False)
                        side = classify_side_from_agg(maker_flag)
                        trade_buffer.append((trade_time_ms, qty, side, price, agg_id))

                        # Keep buffer size bounded; prune to avoid memory blowup
                        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                        prune_old_trades(now_ms)
                    except Exception as e:
                        print(f"[{iso_now()}] Error parsing message: {e}")
            # if loop exits normally, likely stop_event
        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.InvalidStatusCode) as e:
            print(f"[{iso_now()}] Websocket connection error: {e}. Reconnecting in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)
        except Exception as e:
            print(f"[{iso_now()}] Unexpected error: {e}. Reconnecting in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)


def _signal_handler(signame):
    print(f"[{iso_now()}] Received signal {signame}. Shutting down...")
    stop_event.set()


async def main():
    # setup signal handlers
    loop = asyncio.get_running_loop()
    for sig in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(getattr(signal, sig), lambda s=sig: _signal_handler(s))
        except NotImplementedError:
            # Windows may throw; it's ok
            pass

    tasks = [
        asyncio.create_task(handle_ws()),
        asyncio.create_task(evaluate_and_alert()),
    ]
    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    # Cancel remaining tasks
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    print(f"[{iso_now()}] Exited.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted by user. Exiting.")
        sys.exit(0)
