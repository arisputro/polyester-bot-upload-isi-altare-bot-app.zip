#!/usr/bin/env python3
"""Polyester testnet quoting bot (multi-pair) — TESTNET ONLY.

Keeps one post-only bid and one post-only ask around the mid of EACH
configured pair, following the official Polyester "Build a trading bot"
pattern. This is an independent adaptation of that pattern (own structure,
own risk rules), not a copy of the guide's script.

Default pairs (6): BTC-USDT, ETH-USDT, SOL-USDT, BNB-USDT, XRP-USDT,
LTC-USDT. Change them with POLYESTER_PAIRS (comma-separated).

What it does, per cycle, per pair:
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
  - Arms the dead-man switch (30 s) per pair and refreshes it every 10 s
    with a fresh request_id; clean shutdown cancels all open orders on every
    pair, then disarms it.

Per-pair state: each pair has its own rules, quotes, net inventory and
requote tracking. Each pair's buy side is limited by the USDT actually
available (minus the notional of live buy quotes on the other pairs), and
each pair's sell side by the base balance actually held. A pair with zero
base simply skips its sell side quietly until buys have built inventory;
a pair whose USDT cannot cover its buy notional skips its buy side
quietly. The bot keeps quoting every side it can.

Own safety rails (on top of the guide):
  - One dedicated Subaccount only (POLYESTER_SUBACCOUNT_ID). Never trade from
    the Main account with this script.
  - Max net inventory +/-$100 notional PER PAIR: the side that would add to
    a full book stops quoting until inventory falls back.
  - Equity stop: total equity (all base holdings marked at their own pair
    mid + USDT) is compared with the equity at session start; if it falls
    more than 10% below, the bot stops, cancels everything, and logs
    EQUITY STOP.
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
  POLYESTER_PAIRS           comma-separated, default
                            BTC-USDT,ETH-USDT,SOL-USDT,BNB-USDT,XRP-USDT,LTC-USDT
                            (POLYESTER_PAIR still works as a single-pair
                            override for backwards compatibility)
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

PAIRS_DEFAULT = ("BTC-USDT", "ETH-USDT", "SOL-USDT",
                 "BNB-USDT", "XRP-USDT", "LTC-USDT")
WORKDIR = Path(__file__).resolve().parent
LOG_FILE_DEFAULT = WORKDIR / "quote_bot.log"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
@dataclass
class Config:
    pairs: tuple[str, ...] = PAIRS_DEFAULT
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

        raw_pairs = os.environ.get("POLYESTER_PAIRS", "").strip()
        if raw_pairs:
            pairs = tuple(p.strip() for p in raw_pairs.split(",") if p.strip())
        elif os.environ.get("POLYESTER_PAIR", "").strip():
            # Legacy single-pair override.
            pairs = (os.environ["POLYESTER_PAIR"].strip(),)
        else:
            pairs = PAIRS_DEFAULT
        if not pairs:
            pairs = PAIRS_DEFAULT

        return cls(
            pairs=pairs,
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
    base_asset: str = ""
    quote_asset: str = "USDT"
    symbol_id: int | None = None
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

        symbol_id = row.get("symbolId")
        return cls(
            tick=_dec("tickSize", "0.01"),
            step=_dec("stepSize", "0.00000001"),
            min_qty=_dec("minQtyBase", "0.00001"),
            min_notional=_dec("minNotionalQuote", "5"),
            status=str(row.get("status", "")),
            base_asset=str(row.get("baseAsset", "")),
            quote_asset=str(row.get("quoteAsset", "USDT")),
            symbol_id=int(symbol_id) if symbol_id is not None else None,
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
    """Per-pair quoting state. One Engine per pair; the equity guard is
    shared across all pairs (see SharedRisk)."""
    cfg: Config
    rules: PairRules
    net_base: Decimal = Decimal("0")      # +long / -short, in base units
    volume_usd: Decimal = Decimal("0")    # filled notional, both sides
    fills: int = 0
    # dry-run / test observables
    placed_both_sides: bool = False
    requoted: bool = False
    blocked_a_side: bool = False
    sell_skipped_zero_base: bool = False
    buy_skipped_no_usdt: bool = False

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


@dataclass
class SharedRisk:
    """Account-wide equity guard, shared by every pair's Engine. The first
    equity reading becomes the session start; afterwards a drop of more than
    cfg.equity_stop_pct percent trips the stop exactly once."""
    cfg: Config
    start_equity: Decimal | None = None
    equity_stop_hit: bool = False

    def check(self, equity: Decimal) -> bool:
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
        self.rules: dict[str, PairRules] = self._load_rules()
        self.asset_ids = self._load_asset_ids()
        # Stream orders carry symbol_id, not a symbol string.
        self.symbol_by_id: dict[int, str] = {
            r.symbol_id: pair for pair, r in self.rules.items()
            if r.symbol_id is not None
        }
        self._coid_seq = 0

    # -- setup ------------------------------------------------------------
    def _load_rules(self) -> dict[str, PairRules]:
        """Pair rules for every configured pair (one GetSpotConfig call)."""
        cfg = self.client.market_data.get_spot_config()
        rows = cfg.raw.get("pairs") or cfg.raw.get("symbols") or []
        by_symbol = {row.get("symbol"): row for row in rows}
        out: dict[str, PairRules] = {}
        for pair in self.cfg.pairs:
            row = by_symbol.get(pair)
            if row is None:
                raise SystemExit(f"pair {pair} not found in spot config")
            rules = PairRules.from_raw(row)
            self.log.info(
                "pair rules %s: status=%s tick=%s step=%s min_qty=%s "
                "min_notional=%s base=%s quote=%s", pair, rules.status,
                rules.tick, rules.step, rules.min_qty, rules.min_notional,
                rules.base_asset, rules.quote_asset)
            out[pair] = rules
        return out

    def refresh_rules(self) -> None:
        self.rules = self._load_rules()
        self.symbol_by_id = {
            r.symbol_id: pair for pair, r in self.rules.items()
            if r.symbol_id is not None
        }

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
    def top(self, symbol: str) -> tuple[Decimal, Decimal] | None:
        book = self.client.orderbook.get(symbol=symbol, depth=5)
        if not book.available or not book.bids or not book.asks:
            return None
        bid = book.bids[0].price.as_decimal()
        ask = book.asks[0].price.as_decimal()
        return bid, ask

    def balances(self) -> dict[str, Decimal]:
        """Trading balances by asset name, e.g. {"USDT": Decimal("150")}."""
        result = self.client.balances.list()
        by_id = {b.asset_id: b for b in result.balances}
        out: dict[str, Decimal] = {}
        for asset, ledger_id in self.asset_ids.items():
            entry = by_id.get(ledger_id)
            if entry is not None:
                out[asset] = Decimal(entry.trading)
        return out

    def equity(self, mids: dict[str, Decimal],
               balances: dict[str, Decimal] | None = None) -> Decimal:
        """Total equity in the quote asset: every quote balance, plus each
        held base asset marked at its own pair mid. A base whose pair has no
        mid right now is excluded (warned, never crashes the cycle)."""
        if balances is None:
            balances = self.balances()
        total = Decimal("0")
        for asset in {r.quote_asset for r in self.rules.values()
                      if r.quote_asset}:
            total += balances.get(asset, Decimal("0"))
        marked: set[str] = set()
        for pair, mid in mids.items():
            rules = self.rules.get(pair)
            if rules is None or not rules.base_asset:
                continue
            if rules.base_asset in marked:
                continue  # two pairs sharing one base: count it once
            marked.add(rules.base_asset)
            total += balances.get(rules.base_asset, Decimal("0")) * mid
        for pair, rules in self.rules.items():
            if pair in mids or not rules.base_asset:
                continue
            held = balances.get(rules.base_asset, Decimal("0"))
            if held:
                self.log.warning("equity: no mid for %s, excluding %s %s "
                                 "from equity", pair, held, rules.base_asset)
        return total

    def list_open(self, symbol: str) -> list:
        result = self.client.orders.list_open(symbol=symbol)
        return list(getattr(result, "orders", []) or [])

    # -- writes -----------------------------------------------------------
    def place_both(self, quotes: dict[str, Quote],
                    symbol: str) -> dict[str, str | None]:
        """One batch create for both sides. Returns side -> order_id (or None
        when that side was not admitted)."""
        from polyester.models import CreateOrderRequest

        items = [
            CreateOrderRequest(
                symbol=symbol, side=q.side, order_type="limit",
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
                new_coid: str, symbol: str) -> bool:
        from polyester.models import BatchReplaceItem, ClientOrderId

        request_id = f"qb1-replace-{uuid.uuid4().hex[:12]}"
        result = self._send(
            lambda: self.client.orders.batch_replace(
                items=[BatchReplaceItem(
                    key=ClientOrderId(value=old.client_order_id),
                    new_price=str(new_price), new_qty=str(new_qty),
                    new_client_order_id=new_coid)],
                symbol=symbol, request_id=request_id),
            request_id, "batch_replace")
        if result is None:
            return False  # unknown outcome: caller reconciles via list_open
        return True

    def cancel(self, coid: str) -> None:
        from polyester.models import ClientOrderId

        self._send(
            lambda: self.client.orders.cancel(key=ClientOrderId(value=coid)),
            f"qb1-cancel-{uuid.uuid4().hex[:12]}", f"cancel {coid}")

    def cancel_all(self, symbol: str) -> None:
        self._send(
            lambda: self.client.orders.cancel_all(symbol=symbol),
            f"qb1-cancelall-{uuid.uuid4().hex[:12]}", "cancel_all")

    def arm_deadman(self, timeout_sec: int, symbol: str) -> None:
        """Arm (10-120) or disarm (0) the dead-man switch for this pair."""
        self._send(
            lambda: self.client.orders.cancel_all_after(
                timeout_sec=timeout_sec, symbol=symbol,
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
        self.engines: dict[str, Engine] = {
            pair: Engine(cfg, self.gw.rules[pair]) for pair in cfg.pairs
        }
        self.risk = SharedRisk(cfg)
        self.lock = threading.RLock()  # reentrant: reconcile nests under cycle
        self.live: dict[tuple[str, str], Quote] = {}  # (pair, side) -> quote
        self.by_coid: dict[str, tuple[str, Quote]] = {}
        self.by_order_id: dict[str, tuple[str, Quote]] = {}
        self.stop_requested = False
        self.stop_reason = ""
        self._rules_loaded_at = time.monotonic()
        self._last_equity_check = 0.0
        self._last_summary = time.monotonic()
        self._stream_ready = False
        self._stream_stale = False
        # (pair, side) currently skipped for lack of funds; only transitions
        # are logged, so a skipped side does not spam the log every cycle.
        self._funds_skipped: set[tuple[str, str]] = set()

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

    def _order_pair(self, order) -> str | None:
        """Which configured pair an order belongs to. Our own client order
        IDs are the primary key (issued per pair); the stream's symbol_id
        is the fallback for orders found in a snapshot."""
        coid = getattr(order, "client_order_id", "") or ""
        known = self.by_coid.get(coid)
        if known is not None:
            return known[0]
        symbol_id = getattr(order, "symbol_id", None)
        if symbol_id is not None:
            return self.gw.symbol_by_id.get(symbol_id)
        return None

    def _track_order(self, order) -> None:
        coid = getattr(order, "client_order_id", "") or ""
        if not coid.startswith("qb1-"):
            return
        pair = self._order_pair(order)
        if pair is None or pair not in self.engines:
            return
        side = getattr(order, "side", "")
        status = getattr(order, "status", "")
        if status in ("filled", "canceled", "rejected", "expired"):
            self.live.pop((pair, side), None)
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
        prev = self.live.get((pair, side))
        if prev is None or order.version >= prev.version:
            self.live[(pair, side)] = quote
            self.by_coid[coid] = (pair, quote)
            self.by_order_id[getattr(order, "order_id", "")] = (pair, quote)

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
        pair: str | None = None
        side = ""
        ref_price = None
        if order is not None:
            pair = self._order_pair(order)
            side = getattr(order, "side", "")
            ref_price = getattr(order, "price", None)
        if pair is None:
            known = self.by_order_id.get(sub.order_id)
            if known is None:
                self.log.warning("fill for unknown order %s", sub.order_id)
                return
            pair, quote = known
            side = quote.side
            ref_price = quote.price
        qty = fill.qty.as_decimal()
        if fill.price is not None:
            price = fill.price.as_decimal()
        elif ref_price is not None:
            price = (ref_price.as_decimal()
                     if hasattr(ref_price, "as_decimal") else ref_price)
        else:
            self.log.warning("fill without price for order %s", sub.order_id)
            return
        info = self.engines[pair].apply_fill(side, qty, price)
        self.log.info(
            "FILL %s %s qty=%s price=%s notional=%s net_base=%s volume_usd=%s "
            "fills=%d", pair, side, qty, price, info["notional"],
            info["net_base"], info["volume_usd"], info["fills"])

    def _on_stream_error(self, exc: Exception) -> None:
        self.log.warning("order stream error: %s", exc)
        self._stream_stale = True

    # -- heartbeat ----------------------------------------------------------
    def _heartbeat_loop(self) -> None:
        while not self.stop_requested:
            try:
                for pair in self.cfg.pairs:
                    self.gw.arm_deadman(self.cfg.dms_timeout_sec, pair)
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
                "key with TRADE_SPOT on %s.", ", ".join(self.cfg.pairs))
        sub = self.gw.subscribe(self._on_stream_event, self._on_stream_error)
        self.log.info("order stream subscribed; waiting up to 20s for ready")
        deadline = time.monotonic() + 20
        while not self._stream_ready and time.monotonic() < deadline:
            time.sleep(0.2)
        if not self._stream_ready:
            self.log.warning("stream not ready after 20s; quoting paused "
                             "until it is")
        try:
            for pair in self.cfg.pairs:
                self.gw.arm_deadman(self.cfg.dms_timeout_sec, pair)
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
            self.gw.refresh_rules()
            for pair, engine in self.engines.items():
                engine.rules = self.gw.rules[pair]
            self._rules_loaded_at = time.monotonic()
        if self._stream_stale or not self._stream_ready:
            return  # wait for a caught-up stream before quoting

        # 1. Mids, one book read per pair. A pair with no book is skipped
        #    this cycle; the others keep quoting.
        mids: dict[str, Decimal] = {}
        for pair in self.cfg.pairs:
            if not self.gw.rules[pair].quotable:
                self.log.warning("pair %s status=%s; not quoting",
                                 pair, self.gw.rules[pair].status)
                continue
            top = self.gw.top(pair)
            if top is None:
                self.log.info("book unavailable or one-sided for %s; waiting",
                              pair)
                continue
            bid, ask = top
            mids[pair] = (bid + ask) / Decimal("2")
        if not mids:
            return

        # 2. Balances once per cycle, then the account-wide equity check.
        balances = self.gw.balances()
        if time.monotonic() - self._last_equity_check > self.cfg.equity_check_sec:
            self._last_equity_check = time.monotonic()
            equity = self.gw.equity(mids, balances)
            if self.risk.start_equity is None:
                self.risk.start_equity = equity
                self.log.info("session start equity: %s USDT", equity)
            elif self.risk.check(equity):
                self.log.error(
                    "EQUITY STOP: equity %s USDT fell more than %s%% below "
                    "session start %s USDT; stopping and canceling all",
                    equity, self.cfg.equity_stop_pct,
                    self.risk.start_equity)
                self.stop_reason = "equity stop"
                self.stop_requested = True
                return

        # 3. Quote each pair that has a mid.
        for pair, mid in mids.items():
            try:
                self._quote_pair(pair, mid, balances)
            except _RateLimitPause:
                raise
            except Exception as exc:  # noqa: BLE001 - one bad pair skips
                self.log.exception("quote %s error (skipping pair): %s",
                                   pair, exc)

    def _note_funds_skip(self, pair: str, side: str, skipping: bool) -> None:
        key = (pair, side)
        if skipping and key not in self._funds_skipped:
            self._funds_skipped.add(key)
            reason = ("no base balance" if side == "sell"
                      else "not enough USDT")
            self.log.info("%s %s skipped: %s; quoting resumes when funds "
                          "allow", pair, side, reason)
        elif not skipping and key in self._funds_skipped:
            self._funds_skipped.discard(key)
            self.log.info("%s %s funds available again; quoting resumes",
                          pair, side)

    def _quote_pair(self, pair: str, mid: Decimal,
                    balances: dict[str, Decimal]) -> None:
        engine = self.engines[pair]
        rules = self.gw.rules[pair]
        targets = engine.targets(mid)
        # Snapshot this pair's live quotes under the lock; network calls
        # below run WITHOUT the lock so the stream thread can keep
        # processing fills.
        with self.lock:
            live = {side: q for (p, side), q in self.live.items()
                    if p == pair}
        # USDT actually free for a new buy: the balance minus the notional
        # of live buy quotes on the OTHER pairs (this pair's own live buy
        # is being replaced, not added to).
        quote_asset = rules.quote_asset or "USDT"
        usdt = balances.get(quote_asset, Decimal("0"))
        reserved = Decimal("0")
        with self.lock:
            for (p, side), q in self.live.items():
                if side == "buy" and p != pair:
                    reserved += q.price * q.qty
        base = (balances.get(rules.base_asset, Decimal("0"))
                if rules.base_asset else Decimal("0"))

        for side in ("buy", "sell"):
            if side not in targets:
                continue
            price, qty = targets[side]
            # -- funds: skip quietly, log only on transition ------------
            if side == "buy":
                if usdt - reserved < qty * price:
                    engine.buy_skipped_no_usdt = True
                    self._note_funds_skip(pair, side, True)
                    continue
                self._note_funds_skip(pair, side, False)
            else:
                # Sell only what is actually held. If the balance covers
                # less than the target, shrink the quote to the balance
                # (when that still meets the pair minimums).
                if base <= 0:
                    engine.sell_skipped_zero_base = True
                    self._note_funds_skip(pair, side, True)
                    continue
                if base < qty:
                    qty = floor_to_step(base, rules.step)
                    if (qty < rules.min_qty
                            or qty * price < rules.min_notional):
                        engine.sell_skipped_zero_base = True
                        self._note_funds_skip(pair, side, True)
                        continue
                self._note_funds_skip(pair, side, False)
            if engine.side_blocked(side, mid):
                engine.blocked_a_side = True
                if side in live:
                    self.log.info(
                        "inventory cap hit (%s %s net_base=%s); canceling %s",
                        pair, side, engine.net_base, side)
                    self._cancel_side(pair, side)
                continue
            current = live.get(side)
            if current is None:
                self._place(pair, side, price, qty)
            elif engine.drift_ticks(current.price, price) \
                    >= self.cfg.requote_ticks:
                self._replace(pair, current, price, qty)

    def _place(self, pair: str, side: str, price: Decimal,
               qty: Decimal) -> None:
        coid = self.gw.new_coid(side)
        quote = Quote(side=side, price=price, qty=qty, client_order_id=coid)
        admitted = self.gw.place_both({side: quote}, pair)
        if admitted.get(side):
            with self.lock:
                self.live[(pair, side)] = quote
                self.by_coid[coid] = (pair, quote)
                if len([1 for (p, _s) in self.live if p == pair]) == 2:
                    self.engines[pair].placed_both_sides = True
            self.log.info("placed %s %s @ %s qty=%s coid=%s", side,
                          pair, price, qty, coid)
        else:
            self._reconcile(f"place {pair} {side} outcome unknown")

    def _replace(self, pair: str, live: Quote, price: Decimal,
                 qty: Decimal) -> None:
        new_coid = self.gw.new_coid(live.side)
        ok = self.gw.replace(live, price, qty, new_coid, pair)
        if ok:
            with self.lock:
                self.live[(pair, live.side)] = Quote(
                    side=live.side, price=price, qty=qty,
                    client_order_id=new_coid)
                self.by_coid[new_coid] = (pair, self.live[(pair, live.side)])
            self.engines[pair].requoted = True
            self.log.info("replaced %s %s: %s @ %s -> %s @ %s coid=%s",
                          pair, live.side, live.price, live.qty, price, qty,
                          new_coid)
        else:
            self._reconcile(f"replace {pair} {live.side} outcome unknown")

    def _cancel_side(self, pair: str, side: str) -> None:
        with self.lock:
            live = self.live.get((pair, side))
        if live is None:
            return
        self.gw.cancel(live.client_order_id)
        with self.lock:
            self.live.pop((pair, side), None)

    def _reconcile(self, why: str) -> None:
        try:
            orders: list = []
            for pair in self.cfg.pairs:
                orders.extend(self.gw.list_open(pair))
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
        for pair, engine in self.engines.items():
            self.log.info(
                "HOURLY SUMMARY pair=%s fills=%d volume_usd=%s net_base=%s "
                "open_quotes=%d", pair, engine.fills, engine.volume_usd,
                engine.net_base,
                len([1 for (p, _s) in self.live if p == pair]))
        total_volume = sum(e.volume_usd for e in self.engines.values())
        total_fills = sum(e.fills for e in self.engines.values())
        self.log.info("HOURLY SUMMARY total fills=%d volume_usd=%s "
                      "open_quotes=%d start_equity=%s",
                      total_fills, total_volume, len(self.live),
                      self.risk.start_equity)

    def shutdown(self, reason: str) -> None:
        self.log.info("shutdown (%s): canceling all open orders", reason)
        for pair in self.cfg.pairs:
            try:
                self.gw.cancel_all(pair)
            except Exception as exc:  # noqa: BLE001 - DMS is the backstop
                self.log.error("cancel_all %s failed during shutdown: %s",
                               pair, exc)
        deadline = time.monotonic() + 15
        remaining = True
        while time.monotonic() < deadline:
            try:
                remaining = any(self.gw.list_open(pair)
                                for pair in self.cfg.pairs)
            except Exception:  # noqa: BLE001 - assume orders may remain
                break
            if not remaining:
                break
            time.sleep(1)
        if not remaining:
            try:
                for pair in self.cfg.pairs:
                    self.gw.arm_deadman(0, pair)
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
    pair: str
    side: str
    price: Decimal
    qty: Decimal
    client_order_id: str


# Realistic per-pair starting prices / ticks for the dry-run book. Unknown
# pairs fall back to a $10 market with a 0.001 tick (1 bp) so --dry-run
# still works with any POLYESTER_PAIRS value.
FAKE_PAIR_SPECS: dict[str, tuple[Decimal, Decimal, Decimal, Decimal]] = {
    # pair: (start mid, tick, step, min_qty); min notional is 5 everywhere.
    "BTC-USDT": (Decimal("60000"), Decimal("0.01"),
                 Decimal("0.00000001"), Decimal("0.00001")),
    "ETH-USDT": (Decimal("2000"), Decimal("0.01"),
                 Decimal("0.000001"), Decimal("0.0001")),
    "SOL-USDT": (Decimal("70"), Decimal("0.01"),
                 Decimal("0.001"), Decimal("0.01")),
    "BNB-USDT": (Decimal("550"), Decimal("0.01"),
                 Decimal("0.0001"), Decimal("0.001")),
    "XRP-USDT": (Decimal("0.92"), Decimal("0.0001"),
                 Decimal("0.01"), Decimal("1")),
    "LTC-USDT": (Decimal("65"), Decimal("0.01"),
                 Decimal("0.0001"), Decimal("0.001")),
}
FAKE_SPREAD_TICKS = 100  # fake book is 100 ticks wide (half-spread 50)


def _fake_spec(pair: str) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    spec = FAKE_PAIR_SPECS.get(pair)
    if spec is not None:
        return spec
    # Fallback for pairs without a spec: a $10 market whose tick is 1 bp of
    # the price, so the 1.5% scripted dips are ~150 ticks — comfortably
    # above the 51-tick fill threshold of the 100-tick fake spread.
    return (Decimal("10"), Decimal("0.001"),
            Decimal("0.0001"), Decimal("0.01"))


def _fake_mid_path(p0: Decimal, tick: Decimal) -> list[Decimal]:
    """Scripted mids: stable, +5 ticks (requote), 1.5% dips (fills up to
    the inventory cap), hold, then a crash to 67% of the hold level
    (equity stop). Dip steps are >> the 51-tick fill threshold for every
    pair, so the mechanics are identical across high- and low-price pairs.
    """
    dip = p0 * Decimal("0.015")
    hold = p0 - dip * 7
    return ([p0] * 3
            + [p0 + 5 * tick] * 3
            + [p0 - dip * k for k in range(1, 8)]
            + [hold]
            + [hold * f for f in (Decimal("0.9167"), Decimal("0.8333"),
                                  Decimal("0.75"))])


class FakeGateway:
    """In-memory book and order book for --dry-run. Quotes fill when the
    fake book crosses them (maker fills at our quote price)."""

    def __init__(self, cfg: Config, log: logging.Logger,
                 mids_by_pair: dict[str, list[Decimal]],
                 balances: dict[str, Decimal]):
        self.cfg = cfg
        self.log = log
        self.mids_by_pair = mids_by_pair
        self.index = 0
        self.balance: dict[str, Decimal] = dict(balances)
        self.orders: dict[str, FakeOrder] = {}
        self.rules: dict[str, PairRules] = {}
        for pair in cfg.pairs:
            p0, tick, step, min_qty = _fake_spec(pair)
            base, _, quote = pair.partition("-")
            self.rules[pair] = PairRules(
                tick=tick, step=step, min_qty=min_qty,
                min_notional=Decimal("5"), status="PAIR_STATUS_ENABLED",
                base_asset=base, quote_asset=quote or "USDT")
        self._coid_seq = 0

    @property
    def n_cycles(self) -> int:
        return max(len(p) for p in self.mids_by_pair.values())

    def mid(self, pair: str) -> Decimal:
        path = self.mids_by_pair[pair]
        return path[min(self.index, len(path) - 1)]

    def mids(self) -> dict[str, Decimal]:
        return {pair: self.mid(pair) for pair in self.cfg.pairs}

    def top(self, symbol: str) -> tuple[Decimal, Decimal] | None:
        tick = self.rules[symbol].tick
        half = (FAKE_SPREAD_TICKS // 2) * tick
        mid = self.mid(symbol)
        return mid - half, mid + half

    def balances(self) -> dict[str, Decimal]:
        return dict(self.balance)

    def equity(self, mids: dict[str, Decimal],
               balances: dict[str, Decimal] | None = None) -> Decimal:
        if balances is None:
            balances = self.balance
        total = Decimal("0")
        for asset in {r.quote_asset for r in self.rules.values()}:
            total += balances.get(asset, Decimal("0"))
        for pair, mid in mids.items():
            total += (balances.get(self.rules[pair].base_asset,
                                   Decimal("0")) * mid)
        return total

    def new_coid(self, side: str) -> str:
        self._coid_seq += 1
        return f"qb1-{side[0]}-{self._coid_seq:06d}"

    def place_both(self, quotes: dict[str, Quote],
                   symbol: str) -> dict[str, str | None]:
        out: dict[str, str | None] = {}
        for side, q in quotes.items():
            self.orders[q.client_order_id] = FakeOrder(
                pair=symbol, side=side, price=q.price, qty=q.qty,
                client_order_id=q.client_order_id)
            out[side] = f"fake-{q.client_order_id}"
        return out

    def replace(self, old: Quote, new_price: Decimal, new_qty: Decimal,
                new_coid: str, symbol: str) -> bool:
        self.orders.pop(old.client_order_id, None)
        self.orders[new_coid] = FakeOrder(
            pair=symbol, side=old.side, price=new_price, qty=new_qty,
            client_order_id=new_coid)
        return True

    def cancel(self, coid: str) -> None:
        self.orders.pop(coid, None)

    def cancel_all(self, symbol: str | None = None) -> None:
        if symbol is None:
            self.orders.clear()
        else:
            for coid in [c for c, o in self.orders.items()
                         if o.pair == symbol]:
                del self.orders[coid]

    def arm_deadman(self, timeout_sec: int, symbol: str | None = None) -> None:
        self.log.info("[dry-run] dead-man switch set to %ss (no-op)",
                     timeout_sec)

    def advance(self) -> None:
        if self.index < self.n_cycles - 1:
            self.index += 1

    def match(self, mids: dict[str, Decimal]) -> list[dict]:
        """Cross live quotes against the current fake book. Returns fills."""
        fills: list[dict] = []
        for coid, order in list(self.orders.items()):
            bid, ask = self.top(order.pair)
            rules = self.rules[order.pair]
            if order.side == "buy" and ask <= order.price:
                fills.append({"pair": order.pair, "side": "buy",
                              "qty": order.qty, "price": order.price,
                              "coid": coid})
                self.orders.pop(coid, None)
                self.balance[rules.base_asset] = (
                    self.balance.get(rules.base_asset, Decimal("0"))
                    + order.qty)
                self.balance[rules.quote_asset] = (
                    self.balance.get(rules.quote_asset, Decimal("0"))
                    - order.qty * order.price)
            elif order.side == "sell" and bid >= order.price:
                fills.append({"pair": order.pair, "side": "sell",
                              "qty": order.qty, "price": order.price,
                              "coid": coid})
                self.orders.pop(coid, None)
                self.balance[rules.base_asset] = (
                    self.balance.get(rules.base_asset, Decimal("0"))
                    - order.qty)
                self.balance[rules.quote_asset] = (
                    self.balance.get(rules.quote_asset, Decimal("0"))
                    + order.qty * order.price)
        return fills


def simulate(cfg: Config, log: logging.Logger,
               gw: FakeGateway) -> tuple[dict[str, Engine], SharedRisk,
                                         dict[tuple[str, str], Quote]]:
    """Run the scripted cycles against a FakeGateway. Mirrors LiveBot's
    per-cycle logic (match fills -> equity check -> quote decisions,
    including the funds-aware side skips and the reserved-USDT accounting).
    Returns (engines, risk, live)."""
    engines = {pair: Engine(cfg, gw.rules[pair]) for pair in cfg.pairs}
    risk = SharedRisk(cfg)
    live: dict[tuple[str, str], Quote] = {}
    skip_buy_logged: set[str] = set()
    skip_sell_logged: set[str] = set()
    for _cycle in range(gw.n_cycles):
        mids = gw.mids()
        # 1. match existing quotes against the new book
        for fill in gw.match(mids):
            pair = fill["pair"]
            info = engines[pair].apply_fill(fill["side"], fill["qty"],
                                            fill["price"])
            live.pop((pair, fill["side"]), None)
            log.info("[dry-run] FILL %s %s qty=%s price=%s notional=%s "
                     "net_base=%s volume_usd=%s fills=%d",
                     pair, fill["side"], fill["qty"], fill["price"],
                     info["notional"], info["net_base"], info["volume_usd"],
                     info["fills"])
        # 2. equity check (session start on cycle 0)
        equity = gw.equity(mids)
        if risk.start_equity is None:
            risk.start_equity = equity
            log.info("[dry-run] session start equity: %s USDT", equity)
        elif risk.check(equity):
            log.error("[dry-run] EQUITY STOP: equity %s USDT fell more than "
                      "%s%% below session start %s USDT",
                      equity, cfg.equity_stop_pct, risk.start_equity)
            gw.cancel_all()
            live.clear()
            break
        # 3. quote decisions, per pair (same rules as live)
        balances = gw.balances()
        for pair, mid in mids.items():
            engine = engines[pair]
            rules = gw.rules[pair]
            targets = engine.targets(mid)
            live_pair = {side: q for (p, side), q in live.items()
                         if p == pair}
            usdt = balances.get(rules.quote_asset, Decimal("0"))
            reserved = sum(q.price * q.qty
                           for (p, side), q in live.items()
                           if side == "buy" and p != pair)
            base = balances.get(rules.base_asset, Decimal("0"))
            for side in ("buy", "sell"):
                if side not in targets:
                    continue
                price, qty = targets[side]
                if side == "buy":
                    if usdt - reserved < qty * price:
                        engine.buy_skipped_no_usdt = True
                        if pair not in skip_buy_logged:
                            skip_buy_logged.add(pair)
                            log.info("[dry-run] %s buy skipped: not enough "
                                     "USDT", pair)
                        continue
                else:
                    if base <= 0:
                        engine.sell_skipped_zero_base = True
                        if pair not in skip_sell_logged:
                            skip_sell_logged.add(pair)
                            log.info("[dry-run] %s sell skipped: no base "
                                     "balance", pair)
                        continue
                    if base < qty:
                        qty = floor_to_step(base, rules.step)
                        if (qty < rules.min_qty
                                or qty * price < rules.min_notional):
                            engine.sell_skipped_zero_base = True
                            if pair not in skip_sell_logged:
                                skip_sell_logged.add(pair)
                                log.info("[dry-run] %s sell skipped: base "
                                         "balance below minimums", pair)
                            continue
                if engine.side_blocked(side, mid):
                    engine.blocked_a_side = True
                    if side in live_pair:
                        log.info("[dry-run] inventory cap hit (%s %s "
                                 "net_base=%s); canceling %s",
                                 pair, side, engine.net_base, side)
                        gw.cancel(live_pair[side].client_order_id)
                        live.pop((pair, side), None)
                    continue
                current = live_pair.get(side)
                if current is None:
                    coid = gw.new_coid(side)
                    quote = Quote(side=side, price=price, qty=qty,
                                  client_order_id=coid)
                    gw.place_both({side: quote}, pair)
                    live[(pair, side)] = quote
                    log.info("[dry-run] placed %s %s @ %s qty=%s coid=%s",
                             side, pair, price, qty, coid)
                    if len([1 for (p, _s) in live if p == pair]) == 2:
                        engine.placed_both_sides = True
                elif engine.drift_ticks(current.price, price) \
                        >= cfg.requote_ticks:
                    new_coid = gw.new_coid(side)
                    gw.replace(current, price, qty, new_coid, pair)
                    live[(pair, side)] = Quote(
                        side=side, price=price, qty=qty,
                        client_order_id=new_coid)
                    engine.requoted = True
                    log.info("[dry-run] replaced %s %s: %s -> %s coid=%s",
                             pair, side, current.price, price, new_coid)
        gw.advance()
    return engines, risk, live


def dry_run(cfg: Config, log: logging.Logger) -> int:
    """Two deterministic scenarios, no network, no key.
    A: the full script for every configured pair (stable -> +5-tick drift
       -> 1.5% dips -> hold -> crash). B: a zero-funds probe (USDT 0, no
       base) proving a side with no funds is skipped quietly instead of
       erroring. Returns 0 only when every check passes."""
    n = len(cfg.pairs)
    # --- scenario A: full script, $200 USDT per pair, no base at start ---
    mids_by_pair = {}
    for pair in cfg.pairs:
        p0, tick, _step, _min_qty = _fake_spec(pair)
        mids_by_pair[pair] = _fake_mid_path(p0, tick)
    gw = FakeGateway(cfg, log, mids_by_pair,
                     {"USDT": Decimal("200") * n})
    log.info("[dry-run] scenario A: %d pairs, %d cycles, start USDT %s",
             n, gw.n_cycles, Decimal("200") * n)
    engines, risk, _live = simulate(cfg, log, gw)
    for pair, engine in engines.items():
        log.info("[dry-run] A %s fills=%d volume_usd=%s net_base=%s",
                 pair, engine.fills, engine.volume_usd, engine.net_base)
    log.info("[dry-run] A final equity=%s start_equity=%s",
             gw.equity(gw.mids()), risk.start_equity)
    checks = {
        f"quotes placed on both sides ({n} pairs)":
            all(e.placed_both_sides for e in engines.values()),
        f"requote on drift (>=2 ticks) ({n} pairs)":
            all(e.requoted for e in engines.values()),
        f"inventory cap blocked a side ({n} pairs)":
            all(e.blocked_a_side for e in engines.values()),
        "equity stop fired": risk.equity_stop_hit,
        f"zero-base sell skipped quietly ({n} pairs)":
            all(e.sell_skipped_zero_base for e in engines.values()),
    }
    # --- scenario B: zero funds, flat book, 3 cycles ---
    gw_b = FakeGateway(cfg, log,
                       {pair: [_fake_spec(pair)[0]] * 3
                        for pair in cfg.pairs},
                       {"USDT": Decimal("0")})
    log.info("[dry-run] scenario B: zero-funds probe (USDT 0, no base)")
    engines_b, _risk_b, _live_b = simulate(cfg, log, gw_b)
    checks["zero-USDT buy skipped quietly (probe)"] = (
        all(e.buy_skipped_no_usdt for e in engines_b.values())
        and not gw_b.orders
        and all(e.fills == 0 for e in engines_b.values()))
    checks["zero-base sell skipped quietly (probe)"] = all(
        e.sell_skipped_zero_base for e in engines_b.values())

    ok = True
    for name, passed in checks.items():
        log.info("[dry-run] CHECK %-46s %s", name, "PASS" if passed else "FAIL")
        ok = ok and passed
    return 0 if ok else 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main(argv: list[str]) -> int:
    dry = "--dry-run" in argv
    cfg = Config.from_env()
    log = setup_logging(cfg.log_file)
    log.info("quote_bot starting (pairs=%s, notional/side=%s USD, "
             "requote>=%d ticks, max inventory=%s USD/pair, equity stop=%s%%, "
             "dry_run=%s)", ", ".join(cfg.pairs), cfg.notional_per_side,
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
