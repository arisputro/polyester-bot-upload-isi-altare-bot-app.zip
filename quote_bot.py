#!/usr/bin/env python3
"""Polyester testnet quoting bot (BTC-USDT) — TESTNET ONLY.

Keeps one post-only bid and one post-only ask around the mid, following the
official Polyester "Build a trading bot" pattern. This is an independent
adaptation of that pattern (own structure, own risk rules), not a copy of the
guide's script.

What it does, per cycle:
  - Reads the pair rules from GetSpotConfig (tick size, step size, minimums,
    status) and quotes only while the pair is ENABLED or POST_ONLY.
  - Prices a bid one tick below the mid and an ask one tick above it, both
    post-only GTC, one quote per side, client order IDs prefixed ``qb1-``.
  - Places both sides in one batch create; moves a drifted quote (>= 2 ticks)
    with a batch replace (new client order ID); cancels a side whose
    inventory cap is hit.
  - Follows its own orders on the private order stream (snapshot + units),
    logs every fill, and reconciles open orders before resending after an
    unknown outcome (same request_id replay, never a second quote).
  - Arms the dead-man switch (30 s) and refreshes it every 10 s with a fresh
    request_id; clean shutdown cancels all open orders, then disarms it.

Own safety rails (on top of the guide):
  - One dedicated Subaccount only (POLYESTER_SUBACCOUNT_ID). Never trade from
    the Main account with this script.
  - Max net inventory +/-$100 notional: the side that would add to a full
    book stops quoting until inventory falls back.
  - Equity stop: if Subaccount equity falls more than 10% below the equity
    at session start, the bot stops, cancels everything, and logs EQUITY STOP.
  - Testnet only. Never mainnet. Paper funds have no monetary value.

Credentials: the API Key ID and Ed25519 private key hex are read ONLY from
the environment (POLYESTER_API_KEY_ID / POLYESTER_API_KEY_PRIVATE). They are
never logged, never written to any file, and never printed.

Usage:
  python3 quote_bot.py            # live (needs key + funded Subaccount)
  python3 quote_bot.py --dry-run  # logic test on a scripted fake book:
                                  # no network, no key, deterministic

Env vars:
  POLYESTER_API_KEY_ID, POLYESTER_API_KEY_PRIVATE (live only)
  POLYESTER_SUBACCOUNT_ID   Subaccount the bot trades for (strongly advised)
  POLYESTER_PAIR            default BTC-USDT
  QB_NOTIONAL               quote size in USD per side (default 20)
  QB_REQUOTE_TICKS          drift that triggers a replace (default 2)
  QB_MAX_INVENTORY          max net inventory in USD notional (default 100)
  QB_EQUITY_STOP_PCT        equity drop that stops the bot (default 10)
  QB_LOG_FILE               default ~/workspace/polyester/quote_bot.log
"""
from __future__ import annotations

import os
import sys

# --- Work around an httpx/proxy quirk before anything imports polyester. ---
# The VM's NO_PROXY list contains bare IPv6 literals (e.g. "::1") which httpx
# cannot parse into URL patterns ("Invalid port: ':1]'"). Drop every entry
# containing ":"; localhost/127.0.0.1/198.19.0.x remain, and the egress proxy
# itself is addressed by hostname so it is unaffected. No values are logged.
for _name in ("NO_PROXY", "no_proxy"):
    _raw = os.environ.get(_name, "")
    if _raw:
        os.environ[_name] = ",".join(
            item.strip() for item in _raw.split(",")
            if item.strip() and ":" not in item
        )

import logging
import signal
import threading
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR
from pathlib import Path

PAIR_DEFAULT = "BTC-USDT"
WORKDIR = Path(__file__).resolve().parent
LOG_FILE_DEFAULT = WORKDIR / "quote_bot.log"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
@dataclass
class Config:
    pair: str = PAIR_DEFAULT
    notional_per_side: Decimal = Decimal("20")
    requote_ticks: int = 2
    max_inventory_notional: Decimal = Decimal("100")
    equity_stop_pct: Decimal = Decimal("10")
    dms_timeout_sec: int = 30
    heartbeat_sec: int = 10
    cycle_sec: float = 2.0
    rules_refresh_sec: int = 60
    equity_check_sec: int = 30
    summary_sec: int = 3600
    log_file: Path = LOG_FILE_DEFAULT
    subaccount_id: str | None = None

    @classmethod
    def from_env(cls) -> "Config":
        def dec(name: str, default: str) -> Decimal:
            try:
                return Decimal(os.environ.get(name, default))
            except Exception:
                return Decimal(default)

        return cls(
            pair=os.environ.get("POLYESTER_PAIR", PAIR_DEFAULT),
            notional_per_side=dec("QB_NOTIONAL", "20"),
            requote_ticks=int(os.environ.get("QB_REQUOTE_TICKS", "2")),
            max_inventory_notional=dec("QB_MAX_INVENTORY", "100"),
            equity_stop_pct=dec("QB_EQUITY_STOP_PCT", "10"),
            log_file=Path(os.environ.get("QB_LOG_FILE", str(LOG_FILE_DEFAULT))),
            subaccount_id=os.environ.get("POLYESTER_SUBACCOUNT_ID") or None,
        )


def setup_logging(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("quote_bot")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s",
                            datefmt="%Y-%m-%dT%H:%M:%S")
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    logger.addHandler(stream)
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    return logger


# ---------------------------------------------------------------------------
# Decimal helpers (never floats for prices or quantities)
# ---------------------------------------------------------------------------
def floor_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    return (price / tick).to_integral_value(ROUND_FLOOR) * tick


def ceil_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    return (price / tick).to_integral_value(ROUND_CEILING) * tick


def floor_to_step(qty: Decimal, step: Decimal) -> Decimal:
    return (qty / step).to_integral_value(ROUND_FLOOR) * step


# ---------------------------------------------------------------------------
# Pair rules (from GetSpotConfig raw rows)
# ---------------------------------------------------------------------------
@dataclass
class PairRules:
    tick: Decimal
    step: Decimal
    min_qty: Decimal
    min_notional: Decimal
    status: str
    base_scale: int = 8
    quote_scale: int = 6

    @property
    def quotable(self) -> bool:
        return self.status in ("PAIR_STATUS_ENABLED", "PAIR_STATUS_POST_ONLY")

    @classmethod
    def from_raw(cls, row: dict) -> "PairRules":
        def _dec(key: str, default: str) -> Decimal:
            value = row.get(key)
            return Decimal(str(value)) if value is not None else Decimal(default)

        return cls(
            tick=_dec("tickSize", "0.01"),
            step=_dec("stepSize", "0.00000001"),
            min_qty=_dec("minQtyBase", "0.00001"),
            min_notional=_dec("minNotionalQuote", "5"),
            status=str(row.get("status", "")),
            base_scale=int(row.get("baseQuantityScale", 8)),
            quote_scale=int(row.get("quoteQuantityScale", 6)),
        )


# ---------------------------------------------------------------------------
# Pure quoting engine (no SDK, no network) — shared by live and dry-run.
# ---------------------------------------------------------------------------
@dataclass
class Quote:
    side: str  # "buy" | "sell"
    price: Decimal
    qty: Decimal
    client_order_id: str
    version: int = 0


@dataclass
class Engine:
    cfg: Config
    rules: PairRules
    net_base: Decimal = Decimal("0")      # +long / -short, in base units
    volume_usd: Decimal = Decimal("0")    # filled notional, both sides
    fills: int = 0
    start_equity: Decimal | None = None
    equity_stop_hit: bool = False
    # dry-run / test observables
    placed_both_sides: bool = False
    requoted: bool = False
    blocked_a_side: bool = False

    def targets(self, mid: Decimal) -> dict[str, tuple[Decimal, Decimal]]:
        """Target (price, qty) per side, or an empty dict entry if the side
        cannot meet the pair minimums at this mid."""
        out: dict[str, tuple[Decimal, Decimal]] = {}
        bid_price = floor_to_tick(mid - self.rules.tick, self.rules.tick)
        ask_price = ceil_to_tick(mid + self.rules.tick, self.rules.tick)
        for side, price in (("buy", bid_price), ("sell", ask_price)):
            qty = floor_to_step(self.cfg.notional_per_side / price,
                                self.rules.step)
            if qty < self.rules.min_qty:
                continue
            if qty * price < self.rules.min_notional:
                continue
            out[side] = (price, qty)
        return out

    def max_base(self, mid: Decimal) -> Decimal:
        return self.cfg.max_inventory_notional / mid

    def side_blocked(self, side: str, mid: Decimal) -> bool:
        cap = self.max_base(mid)
        if side == "buy":
            return self.net_base >= cap
        return self.net_base <= -cap

    def drift_ticks(self, current: Decimal, target: Decimal) -> Decimal:
        return abs(current - target) / self.rules.tick

    def apply_fill(self, side: str, qty: Decimal, price: Decimal) -> dict:
        if side == "buy":
            self.net_base += qty
        else:
            self.net_base -= qty
        self.volume_usd += qty * price
        self.fills += 1
        return {
            "side": side, "qty": qty, "price": price,
            "notional": qty * price, "net_base": self.net_base,
            "volume_usd": self.volume_usd, "fills": self.fills,
        }

    def check_equity_stop(self, equity: Decimal) -> bool:
        if self.start_equity is None:
            self.start_equity = equity
            return False
        floor = self.start_equity * (Decimal("1")
                                     - self.cfg.equity_stop_pct / Decimal("100"))
        if equity < floor:
            self.equity_stop_hit = True
            return True
        return False


# ---------------------------------------------------------------------------
# Live gateway (official polyester-sdk, sync client)
# ---------------------------------------------------------------------------
class LiveGateway:
    """Thin wrapper over the SDK. All network lives here; the Bot decides."""

    def __init__(self, cfg: Config, log: logging.Logger):
        self.cfg = cfg
        self.log = log
        from polyester import Polyester, POLYESTER_TESTNET_ENVIRONMENT

        key_id = os.environ.get("POLYESTER_API_KEY_ID", "").strip()
        priv = os.environ.get("POLYESTER_API_KEY_PRIVATE", "").strip()
        if not key_id or not priv:
            raise SystemExit(
                "live mode needs POLYESTER_API_KEY_ID and "
                "POLYESTER_API_KEY_PRIVATE in the environment")
        self.client = Polyester(
            environment=POLYESTER_TESTNET_ENVIRONMENT,
            api_key_id=key_id,
            api_private_key=priv,
            default_sub_account_id=cfg.subaccount_id,
            hydrate_catalogs=True,
        )
        self.rules = self._load_rules()
        self.asset_ids = self._load_asset_ids()
        self._coid_seq = 0

    # -- setup ------------------------------------------------------------
    def _load_rules(self) -> PairRules:
        cfg = self.client.market_data.get_spot_config()
        rows = cfg.raw.get("pairs") or cfg.raw.get("symbols") or []
        for row in rows:
            if row.get("symbol") == self.cfg.pair:
                rules = PairRules.from_raw(row)
                self.log.info(
                    "pair rules %s: status=%s tick=%s step=%s min_qty=%s "
                    "min_notional=%s", self.cfg.pair, rules.status, rules.tick,
                    rules.step, rules.min_qty, rules.min_notional)
                return rules
        raise SystemExit(f"pair {self.cfg.pair} not found in spot config")

    def _load_asset_ids(self) -> dict[str, int]:
        cfg = self.client.market_data.get_spot_config()
        out: dict[str, int] = {}
        for row in cfg.raw.get("assets") or []:
            symbol = row.get("asset")
            ledger_id = row.get("ledgerId")
            if symbol and ledger_id is not None:
                out[str(symbol)] = int(ledger_id)
        return out

    def new_coid(self, side: str) -> str:
        self._coid_seq += 1
        return f"qb1-{side[0]}-{self._coid_seq:06d}"

    # -- reads ------------------------------------------------------------
    def top(self) -> tuple[Decimal, Decimal] | None:
        book = self.client.orderbook.get(symbol=self.cfg.pair, depth=5)
        if not book.available or not book.bids or not book.asks:
            return None
        bid = book.bids[0].price.as_decimal()
        ask = book.asks[0].price.as_decimal()
        return bid, ask

    def equity(self, mid: Decimal) -> Decimal:
        balances = self.client.balances.list()
        by_id = {b.asset_id: b for b in balances.balances}
        btc_id = self.asset_ids.get("BTC", 3)
        usdt_id = self.asset_ids.get("USDT", 1)
        btc = Decimal(by_id[btc_id].trading) if btc_id in by_id else Decimal("0")
        usdt = Decimal(by_id[usdt_id].trading) if usdt_id in by_id else Decimal("0")
        return btc * mid + usdt

    def list_open(self) -> list:
        result = self.client.orders.list_open(symbol=self.cfg.pair)
        return list(getattr(result, "orders", []) or [])

    # -- writes -----------------------------------------------------------
    def place_both(self, quotes: dict[str, Quote]) -> dict[str, str | None]:
        """One batch create for both sides. Returns side -> order_id (or None
        when that side was not admitted)."""
        from polyester.models import CreateOrderRequest

        items = [
            CreateOrderRequest(
                symbol=self.cfg.pair, side=q.side, order_type="limit",
                tif="gtc", qty=str(q.qty), price=str(q.price),
                client_order_id=q.client_order_id, post_only=True)
            for q in quotes.values()
        ]
        request_id = f"qb1-batch-{uuid.uuid4().hex[:12]}"
        result = self._send(
            lambda: self.client.orders.batch_create(
                items=items, request_id=request_id),
            request_id, "batch_create")
        admitted: dict[str, str | None] = {s: None for s in quotes}
        if result is None:
            return admitted  # unknown outcome: caller reconciles via list_open
        for item, quote in zip(getattr(result, "items", []) or [],
                               quotes.values()):
            order_id = getattr(item, "order_id", "") or None
            status = getattr(item, "status", "")
            if order_id:
                admitted[quote.side] = order_id
            else:
                self.log.warning("batch_create %s not admitted: status=%s",
                                 quote.side, status)
        return admitted

    def replace(self, old: Quote, new_price: Decimal, new_qty: Decimal,
                new_coid: str) -> bool:
        from polyester.models import BatchReplaceItem, ClientOrderId

        request_id = f"qb1-replace-{uuid.uuid4().hex[:12]}"
        result = self._send(
            lambda: self.client.orders.batch_replace(
                items=[BatchReplaceItem(
                    key=ClientOrderId(value=old.client_order_id),
                    new_price=str(new_price), new_qty=str(new_qty),
                    new_client_order_id=new_coid)],
                symbol=self.cfg.pair, request_id=request_id),
            request_id, "batch_replace")
        if result is None:
            return False  # unknown outcome: caller reconciles via list_open
        return True

    def cancel(self, coid: str) -> None:
        from polyester.models import ClientOrderId

        self._send(
            lambda: self.client.orders.cancel(key=ClientOrderId(value=coid)),
            f"qb1-cancel-{uuid.uuid4().hex[:12]}", f"cancel {coid}")

    def cancel_all(self) -> None:
        self._send(
            lambda: self.client.orders.cancel_all(symbol=self.cfg.pair),
            f"qb1-cancelall-{uuid.uuid4().hex[:12]}", "cancel_all")

    def arm_deadman(self, timeout_sec: int) -> None:
        """Arm (10-120) or disarm (0) the dead-man switch for this pair."""
        self._send(
            lambda: self.client.orders.cancel_all_after(
                timeout_sec=timeout_sec, symbol=self.cfg.pair,
                request_id=f"qb1-dms-{uuid.uuid4().hex[:12]}"),
            None, f"deadman {timeout_sec}s")

    # -- send wrapper: rate limits, unknown outcomes ----------------------
    def _send(self, fn, request_id: str | None, describe: str):
        """Run one write. Returns its result, or None when the outcome is
        unknown (caller must read back before sending again). Raises
        _RateLimitPause when the venue says to back off."""
        from polyester import PolyesterRateLimitError, mutation_outcome_unknown

        try:
            return fn()
        except PolyesterRateLimitError as exc:
            wait = getattr(exc, "retry_after", None) or 5
            self.log.warning("rate limit on %s; backing off %ss",
                             describe, wait)
            raise _RateLimitPause(float(wait))
        except Exception as exc:  # noqa: BLE001 - classify, then re-raise
            if mutation_outcome_unknown(exc):
                self.log.warning(
                    "unknown outcome on %s (%s); will read back before "
                    "resending", describe, type(exc).__name__)
                return None
            text = str(exc)
            if ("RATE_LIMIT" in text or "SYSTEM_RATE_LIMITED" in text
                    or "OVERLOADED" in text):
                self.log.warning("rate limit on %s; backing off 5s", describe)
                raise _RateLimitPause(5.0)
            raise

    def subscribe(self, on_event, on_error):
        return self.client.orders.subscribe_sync(
            on_event=on_event, on_error=on_error)

    def close(self) -> None:
        self.client.close()


class _RateLimitPause(Exception):
    def __init__(self, seconds: float):
        super().__init__(f"rate limit pause {seconds}s")
        self.seconds = seconds


# ---------------------------------------------------------------------------
# Live bot
# ---------------------------------------------------------------------------
class LiveBot:
    def __init__(self, cfg: Config, log: logging.Logger):
        self.cfg = cfg
        self.log = log
        self.gw = LiveGateway(cfg, log)
        self.engine = Engine(cfg, self.gw.rules)
        self.lock = threading.RLock()  # reentrant: reconcile nests under cycle
        self.live: dict[str, Quote] = {}   # side -> live quote
        self.by_coid: dict[str, Quote] = {}
        self.by_order_id: dict[str, Quote] = {}
        self.stop_requested = False
        self.stop_reason = ""
        self._rules_loaded_at = time.monotonic()
        self._last_equity_check = 0.0
        self._last_summary = time.monotonic()
        self._stream_ready = False
        self._stream_stale = False

    # -- stream events ------------------------------------------------------
    def _on_stream_event(self, event) -> None:
        from polyester.models.private_orders import (
            PrivateOrdersReady, PrivateOrdersSnapshot, PrivateOrdersStale,
            PrivateOrdersUnit)

        try:
            if isinstance(event, PrivateOrdersSnapshot):
                with self.lock:
                    self.live = {}
                    for order in event.orders or []:
                        self._track_order(order)
                self._stream_ready = True
            elif isinstance(event, PrivateOrdersUnit):
                with self.lock:
                    for order in event.orders or []:
                        self._track_order(order)
                    for order_id in event.removed_order_ids or []:
                        self.by_order_id.pop(order_id, None)
                    for sub in event.events or []:
                        self._handle_order_event(sub, event.orders or [])
                self._stream_stale = False
            elif isinstance(event, PrivateOrdersReady):
                self._stream_ready = True
                self.log.info("order stream ready")
            elif isinstance(event, PrivateOrdersStale):
                self._stream_stale = bool(getattr(event, "stale", True))
                self.log.warning("order stream stale=%s", self._stream_stale)
            else:
                name = type(event).__name__
                if "Error" in name or "Disconnect" in name:
                    self.log.warning("order stream event: %s", name)
        except Exception as exc:  # noqa: BLE001 - never kill the stream thread
            self.log.exception("stream handler error: %s", exc)

    def _track_order(self, order) -> None:
        coid = getattr(order, "client_order_id", "") or ""
        if not coid.startswith("qb1-"):
            return
        side = getattr(order, "side", "")
        status = getattr(order, "status", "")
        if status in ("filled", "canceled", "rejected", "expired"):
            self.live.pop(side, None)
            self.by_coid.pop(coid, None)
            return
        price = order.price.as_decimal() if order.price else None
        qty = None
        if order.leaves_qty is not None:
            qty = order.leaves_qty.as_decimal()
        if price is None or qty is None:
            return
        quote = Quote(side=side, price=price, qty=qty, client_order_id=coid,
                      version=getattr(order, "version", 0))
        prev = self.live.get(side)
        if prev is None or order.version >= prev.version:
            self.live[side] = quote
            self.by_coid[coid] = quote
            self.by_order_id[getattr(order, "order_id", "")] = quote

    def _handle_order_event(self, sub, unit_orders) -> None:
        kind = getattr(sub, "kind", "")
        if kind != "filled":
            if kind == "rejected":
                self.log.warning("order rejected: order_id=%s reason=%s",
                                 getattr(sub, "order_id", "?"),
                                 getattr(sub, "reject_reason", "?"))
            return
        fill = getattr(sub, "fill", None)
        if fill is None:
            return
        order = next((o for o in unit_orders
                      if getattr(o, "order_id", "") == sub.order_id), None)
        if order is None:
            order = self.by_order_id.get(sub.order_id)
        if order is None:
            self.log.warning("fill for unknown order %s", sub.order_id)
            return
        side = getattr(order, "side", "")
        qty = fill.qty.as_decimal()
        price = (fill.price.as_decimal() if fill.price is not None
                 else order.price.as_decimal())
        info = self.engine.apply_fill(side, qty, price)
        self.log.info(
            "FILL %s qty=%s price=%s notional=%s net_base=%s volume_usd=%s "
            "fills=%d", side, qty, price, info["notional"], info["net_base"],
            info["volume_usd"], info["fills"])

    def _on_stream_error(self, exc: Exception) -> None:
        self.log.warning("order stream error: %s", exc)
        self._stream_stale = True

    # -- heartbeat ----------------------------------------------------------
    def _heartbeat_loop(self) -> None:
        while not self.stop_requested:
            try:
                self.gw.arm_deadman(self.cfg.dms_timeout_sec)
            except _RateLimitPause as exc:
                self.log.warning("dead-man heartbeat rate-limited; "
                                 "retry in %ss", exc.seconds)
                time.sleep(exc.seconds)
                continue
            except Exception as exc:  # noqa: BLE001 - DMS failure is fatal-ish
                self.log.error("dead-man heartbeat failed: %s; open orders "
                               "will be canceled by the switch in %ss",
                               exc, self.cfg.dms_timeout_sec)
            for _ in range(int(self.cfg.heartbeat_sec * 10)):
                if self.stop_requested:
                    return
                time.sleep(0.1)

    # -- main loop ----------------------------------------------------------
    def run(self) -> int:
        if self.cfg.subaccount_id is None:
            self.log.warning(
                "POLYESTER_SUBACCOUNT_ID is not set; the key's own scope "
                "decides which account this trades. Use a Subaccount-bound "
                "key with TRADE_SPOT on %s.", self.cfg.pair)
        sub = self.gw.subscribe(self._on_stream_event, self._on_stream_error)
        self.log.info("order stream subscribed; waiting up to 20s for ready")
        deadline = time.monotonic() + 20
        while not self._stream_ready and time.monotonic() < deadline:
            time.sleep(0.2)
        if not self._stream_ready:
            self.log.warning("stream not ready after 20s; quoting paused "
                             "until it is")
        try:
            self.gw.arm_deadman(self.cfg.dms_timeout_sec)
        except Exception as exc:  # noqa: BLE001 - log and continue; DMS is a guard
            self.log.error("could not arm dead-man switch: %s", exc)
        heartbeat = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat.start()

        self._reconcile("startup")
        try:
            while not self.stop_requested:
                try:
                    self._cycle()
                except _RateLimitPause as exc:
                    time.sleep(exc.seconds)
                except Exception as exc:  # noqa: BLE001 - one bad cycle skips
                    self.log.exception("cycle error (skipping one cycle): %s",
                                       exc)
                for _ in range(int(self.cfg.cycle_sec * 10)):
                    if self.stop_requested:
                        break
                    time.sleep(0.1)
                self._maybe_summary()
        finally:
            self.shutdown(self.stop_reason or "interrupted")

        sub.close()
        return 0

    def _cycle(self) -> None:
        if time.monotonic() - self._rules_loaded_at > self.cfg.rules_refresh_sec:
            self.gw.rules = self.gw._load_rules()
            self.engine.rules = self.gw.rules
            self._rules_loaded_at = time.monotonic()
        if not self.gw.rules.quotable:
            self.log.warning("pair %s status=%s; not quoting",
                             self.cfg.pair, self.gw.rules.status)
            return
        if self._stream_stale or not self._stream_ready:
            return  # wait for a caught-up stream before quoting
        top = self.gw.top()
        if top is None:
            self.log.info("book unavailable or one-sided; waiting")
            return
        bid, ask = top
        mid = (bid + ask) / Decimal("2")

        if time.monotonic() - self._last_equity_check > self.cfg.equity_check_sec:
            self._last_equity_check = time.monotonic()
            equity = self.gw.equity(mid)
            if self.engine.start_equity is None:
                self.engine.start_equity = equity
                self.log.info("session start equity: %s USDT", equity)
            elif self.engine.check_equity_stop(equity):
                self.log.error(
                    "EQUITY STOP: equity %s USDT fell more than %s%% below "
                    "session start %s USDT; stopping and canceling all",
                    equity, self.cfg.equity_stop_pct,
                    self.engine.start_equity)
                self.stop_reason = "equity stop"
                self.stop_requested = True
                return

        targets = self.engine.targets(mid)
        # Snapshot live quotes under the lock; network calls below run
        # WITHOUT the lock so the stream thread can keep processing fills.
        with self.lock:
            live = dict(self.live)
        for side in ("buy", "sell"):
            if side not in targets:
                continue
            if self.engine.side_blocked(side, mid):
                self.engine.blocked_a_side = True
                if side in live:
                    self.log.info(
                        "inventory cap hit (%s net_base=%s); canceling %s",
                        side, self.engine.net_base, side)
                    self._cancel_side(side)
                continue
            price, qty = targets[side]
            current = live.get(side)
            if current is None:
                self._place(side, price, qty)
            elif self.engine.drift_ticks(current.price, price) \
                    >= self.cfg.requote_ticks:
                self._replace(current, price, qty)

    def _place(self, side: str, price: Decimal, qty: Decimal) -> None:
        coid = self.gw.new_coid(side)
        quote = Quote(side=side, price=price, qty=qty, client_order_id=coid)
        admitted = self.gw.place_both({side: quote})
        if admitted.get(side):
            with self.lock:
                self.live[side] = quote
                self.by_coid[coid] = quote
                if len(self.live) == 2:
                    self.engine.placed_both_sides = True
            self.log.info("placed %s %s @ %s qty=%s coid=%s", side,
                          self.cfg.pair, price, qty, coid)
        else:
            self._reconcile(f"place {side} outcome unknown")

    def _replace(self, live: Quote, price: Decimal, qty: Decimal) -> None:
        new_coid = self.gw.new_coid(live.side)
        ok = self.gw.replace(live, price, qty, new_coid)
        if ok:
            with self.lock:
                self.live[live.side] = Quote(
                    side=live.side, price=price, qty=qty,
                    client_order_id=new_coid)
                self.by_coid[new_coid] = self.live[live.side]
            self.engine.requoted = True
            self.log.info("replaced %s: %s @ %s -> %s @ %s coid=%s",
                          live.side, live.price, live.qty, price, qty,
                          new_coid)
        else:
            self._reconcile(f"replace {live.side} outcome unknown")

    def _cancel_side(self, side: str) -> None:
        with self.lock:
            live = self.live.get(side)
        if live is None:
            return
        self.gw.cancel(live.client_order_id)
        with self.lock:
            self.live.pop(side, None)

    def _reconcile(self, why: str) -> None:
        try:
            orders = self.gw.list_open()
        except Exception as exc:  # noqa: BLE001 - reconcile is best-effort
            self.log.warning("reconcile (%s) failed: %s", why, exc)
            return
        with self.lock:
            self.live = {}
            for order in orders:
                self._track_order(order)
        self.log.info("reconciled open orders after %s: %d live quote(s)",
                      why, len(self.live))

    def _maybe_summary(self) -> None:
        if time.monotonic() - self._last_summary < self.cfg.summary_sec:
            return
        self._last_summary = time.monotonic()
        self.log.info(
            "HOURLY SUMMARY pair=%s fills=%d volume_usd=%s net_base=%s "
            "open_quotes=%d start_equity=%s",
            self.cfg.pair, self.engine.fills, self.engine.volume_usd,
            self.engine.net_base, len(self.live), self.engine.start_equity)

    def shutdown(self, reason: str) -> None:
        self.log.info("shutdown (%s): canceling all open orders", reason)
        try:
            self.gw.cancel_all()
        except Exception as exc:  # noqa: BLE001 - DMS is the backstop
            self.log.error("cancel_all failed during shutdown: %s", exc)
        deadline = time.monotonic() + 15
        remaining = True
        while time.monotonic() < deadline:
            try:
                remaining = bool(self.gw.list_open())
            except Exception:  # noqa: BLE001 - assume orders may remain
                break
            if not remaining:
                break
            time.sleep(1)
        if not remaining:
            try:
                self.gw.arm_deadman(0)
                self.log.info("dead-man switch disarmed")
            except Exception as exc:  # noqa: BLE001 - log only
                self.log.warning("could not disarm dead-man switch: %s", exc)
        else:
            self.log.warning(
                "open orders may remain; leaving dead-man switch armed so "
                "they cancel when it expires")
        try:
            self.gw.close()
        except Exception as exc:  # noqa: BLE001 - shutdown must not raise
            self.log.warning("client close failed during shutdown: %s", exc)
        self.log.info("shutdown complete (%s)", reason)


# ---------------------------------------------------------------------------
# Fake gateway (dry-run): scripted book + matching, no network, no key.
# ---------------------------------------------------------------------------
@dataclass
class FakeOrder:
    side: str
    price: Decimal
    qty: Decimal
    client_order_id: str


class FakeGateway:
    """In-memory book and order book for --dry-run. Quotes fill when the
    fake book crosses them (maker fills at our quote price)."""

    SPREAD = Decimal("1.00")  # fake best bid = mid - 0.50, ask = mid + 0.50

    def __init__(self, cfg: Config, log: logging.Logger,
                 mids: list[Decimal], usdt: Decimal = Decimal("200"),
                 btc: Decimal = Decimal("0")):
        self.cfg = cfg
        self.log = log
        self.mids = mids
        self.index = 0
        self.usdt = usdt
        self.btc = btc
        self.orders: dict[str, FakeOrder] = {}
        self.rules = PairRules(
            tick=Decimal("0.01"), step=Decimal("0.00000001"),
            min_qty=Decimal("0.00001"), min_notional=Decimal("5"),
            status="PAIR_STATUS_ENABLED")
        self._coid_seq = 0

    @property
    def mid(self) -> Decimal:
        return self.mids[min(self.index, len(self.mids) - 1)]

    def top(self) -> tuple[Decimal, Decimal] | None:
        half = self.SPREAD / Decimal("2")
        return self.mid - half, self.mid + half

    def equity(self, mid: Decimal) -> Decimal:
        return self.usdt + self.btc * mid

    def new_coid(self, side: str) -> str:
        self._coid_seq += 1
        return f"qb1-{side[0]}-{self._coid_seq:06d}"

    def place_both(self, quotes: dict[str, Quote]) -> dict[str, str | None]:
        out: dict[str, str | None] = {}
        for side, q in quotes.items():
            self.orders[q.client_order_id] = FakeOrder(
                side=side, price=q.price, qty=q.qty,
                client_order_id=q.client_order_id)
            out[side] = f"fake-{q.client_order_id}"
        return out

    def replace(self, old: Quote, new_price: Decimal, new_qty: Decimal,
                new_coid: str) -> bool:
        self.orders.pop(old.client_order_id, None)
        self.orders[new_coid] = FakeOrder(
            side=old.side, price=new_price, qty=new_qty,
            client_order_id=new_coid)
        return True

    def cancel(self, coid: str) -> None:
        self.orders.pop(coid, None)

    def cancel_all(self) -> None:
        self.orders.clear()

    def arm_deadman(self, timeout_sec: int) -> None:
        self.log.info("[dry-run] dead-man switch set to %ss (no-op)",
                     timeout_sec)

    def advance(self) -> None:
        if self.index < len(self.mids) - 1:
            self.index += 1

    def match(self) -> list[dict]:
        """Cross live quotes against the current fake book. Returns fills."""
        bid, ask = self.top()
        fills: list[dict] = []
        for coid, order in list(self.orders.items()):
            if order.side == "buy" and ask <= order.price:
                fills.append({"side": "buy", "qty": order.qty,
                              "price": order.price, "coid": coid})
                self.orders.pop(coid, None)
                self.btc += order.qty
                self.usdt -= order.qty * order.price
            elif order.side == "sell" and bid >= order.price:
                fills.append({"side": "sell", "qty": order.qty,
                              "price": order.price, "coid": coid})
                self.orders.pop(coid, None)
                self.btc -= order.qty
                self.usdt += order.qty * order.price
        return fills


def dry_run(cfg: Config, log: logging.Logger) -> int:
    """Deterministic scenario. Asserts the four behaviours the task asks for:
    quotes placed, requote on drift, inventory cap blocks a side, equity stop
    fires. Returns 0 only when all four are demonstrated."""
    mids = ([Decimal("60000.00")] * 3          # A: stable -> place quotes
            + [Decimal("60000.05")] * 3         # B: +5 ticks -> requote
            + [Decimal("59995"), Decimal("59990"), Decimal("59985"),
               Decimal("59980"), Decimal("59975"), Decimal("59970"),
               Decimal("59965")]                # C: dips -> bid fills to cap
            + [Decimal("59965")]                # C': hold, bid must be blocked
            + [Decimal("55000"), Decimal("50000"),
               Decimal("45000")])               # D: crash -> equity stop
    gw = FakeGateway(cfg, log, mids)
    engine = Engine(cfg, gw.rules)
    live: dict[str, Quote] = {}
    log.info("[dry-run] starting scenario: %d cycles, mid path %s..%s",
             len(mids), mids[0], mids[-1])

    for cycle in range(len(mids)):
        mid = gw.mid
        # 1. match existing quotes against the new book
        for fill in gw.match():
            info = engine.apply_fill(fill["side"], fill["qty"], fill["price"])
            live.pop(fill["side"], None)
            log.info("[dry-run] FILL %s qty=%s price=%s notional=%s "
                     "net_base=%s volume_usd=%s fills=%d",
                     fill["side"], fill["qty"], fill["price"],
                     info["notional"], info["net_base"], info["volume_usd"],
                     info["fills"])
        # 2. equity check (session start on cycle 0)
        equity = gw.equity(mid)
        if engine.start_equity is None:
            engine.start_equity = equity
            log.info("[dry-run] session start equity: %s USDT", equity)
        elif engine.check_equity_stop(equity):
            log.error("[dry-run] EQUITY STOP: equity %s USDT fell more than "
                      "%s%% below session start %s USDT",
                      equity, cfg.equity_stop_pct, engine.start_equity)
            gw.cancel_all()
            live.clear()
            break
        # 3. quote decisions (same rules as live)
        targets = engine.targets(mid)
        for side in ("buy", "sell"):
            if side not in targets:
                continue
            if engine.side_blocked(side, mid):
                engine.blocked_a_side = True
                if side in live:
                    log.info("[dry-run] inventory cap hit (%s net_base=%s); "
                             "canceling %s", side, engine.net_base, side)
                    gw.cancel(live[side].client_order_id)
                    live.pop(side, None)
                continue
            price, qty = targets[side]
            current = live.get(side)
            if current is None:
                coid = gw.new_coid(side)
                quote = Quote(side=side, price=price, qty=qty,
                              client_order_id=coid)
                gw.place_both({side: quote})
                live[side] = quote
                log.info("[dry-run] placed %s @ %s qty=%s coid=%s", side,
                         price, qty, coid)
                if len(live) == 2:
                    engine.placed_both_sides = True
            elif engine.drift_ticks(current.price, price) >= cfg.requote_ticks:
                new_coid = gw.new_coid(side)
                gw.replace(current, price, qty, new_coid)
                live[side] = Quote(side=side, price=price, qty=qty,
                                   client_order_id=new_coid)
                engine.requoted = True
                log.info("[dry-run] replaced %s: %s -> %s coid=%s", side,
                         current.price, price, new_coid)
        gw.advance()

    checks = {
        "quotes placed on both sides": engine.placed_both_sides,
        "requote on drift (>=2 ticks)": engine.requoted,
        "inventory cap blocked a side": engine.blocked_a_side,
        "equity stop fired": engine.equity_stop_hit,
    }
    log.info("[dry-run] SUMMARY fills=%d volume_usd=%s net_base=%s "
             "final_equity=%s start_equity=%s",
             engine.fills, engine.volume_usd, engine.net_base,
             gw.equity(gw.mid), engine.start_equity)
    ok = True
    for name, passed in checks.items():
        log.info("[dry-run] CHECK %-34s %s", name, "PASS" if passed else "FAIL")
        ok = ok and passed
    return 0 if ok else 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main(argv: list[str]) -> int:
    dry = "--dry-run" in argv
    cfg = Config.from_env()
    log = setup_logging(cfg.log_file)
    log.info("quote_bot starting (pair=%s, notional/side=%s USD, "
             "requote>=%d ticks, max inventory=%s USD, equity stop=%s%%, "
             "dry_run=%s)", cfg.pair, cfg.notional_per_side,
             cfg.requote_ticks, cfg.max_inventory_notional,
             cfg.equity_stop_pct, dry)
    if dry:
        return dry_run(cfg, log)

    bot = LiveBot(cfg, log)

    def _request_stop(signum, _frame) -> None:
        log.info("signal %s received; stopping after this cycle", signum)
        bot.stop_requested = True
        bot.stop_reason = f"signal {signum}"

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)
    return bot.run()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
