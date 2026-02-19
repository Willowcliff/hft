from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import socket
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Set

import requests
import uvicorn
import websockets
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams, OrderArgs, OrderType

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

try:
    from web3 import Web3
except ImportError:
    Web3 = None


LOGGER = logging.getLogger("polymarket_copy_trader")

if load_dotenv is not None:
    load_dotenv()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def env_csv(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default)
    if raw is None:
        return []
    return [part.strip().lower() for part in raw.split(",") if part.strip()]


def safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def chunks(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def round_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return round(price, 6)
    steps = round(price / tick_size)
    rounded = steps * tick_size
    rounded = clamp(rounded, tick_size, 1 - tick_size)
    return round(rounded, 6)


def round_size(size: float) -> float:
    return round(size, 4)


def find_available_port(host: str, requested_port: int, max_tries: int = 20) -> int:
    for offset in range(max_tries):
        candidate = requested_port + offset
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, candidate))
                return candidate
            except OSError:
                continue
    raise RuntimeError(f"No open port found in range {requested_port}-{requested_port + max_tries - 1}")


def pnl_for_position(side: str, entry_price: float, mark_price: float, size: float) -> float:
    if side == "BUY":
        return (mark_price - entry_price) * size
    return (entry_price - mark_price) * size


def pnl_pct_for_position(side: str, entry_price: float, mark_price: float) -> float:
    if entry_price <= 0:
        return 0.0
    if side == "BUY":
        return (mark_price - entry_price) / entry_price
    return (entry_price - mark_price) / entry_price


def opposite_side(side: str) -> str:
    return "SELL" if side == "BUY" else "BUY"


def extract_balance_number(payload: Any) -> float:
    if payload is None:
        return 0.0
    if isinstance(payload, (int, float, str)):
        return safe_float(payload, 0.0)
    if isinstance(payload, list):
        for item in payload:
            value = extract_balance_number(item)
            if value > 0:
                return value
        return 0.0
    if isinstance(payload, dict):
        # Prefer explicit balance-like keys and do not scan every dict value:
        # allowance payloads can contain huge integers that are not spendable balance.
        keys = ["balance", "availableBalance", "available_balance", "available", "amount", "value", "usdc"]
        for key in keys:
            if key in payload:
                return extract_balance_number(payload.get(key))

        container_keys = ["data", "result", "wallet", "account", "collateral", "balances", "response"]
        for key in container_keys:
            if key in payload:
                parsed = extract_balance_number(payload.get(key))
                if parsed > 0:
                    return parsed
    return 0.0


def parse_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def parse_orderbook_levels(levels: Any, descending: bool) -> List[Dict[str, float]]:
    parsed: List[Dict[str, float]] = []
    if not isinstance(levels, list):
        return parsed

    for level in levels:
        price = 0.0
        size = 0.0
        if isinstance(level, dict):
            price = safe_float(level.get("price"), 0.0)
            size = safe_float(level.get("size"), 0.0)
            if size <= 0:
                size = safe_float(level.get("amount"), 0.0)
            if size <= 0:
                size = safe_float(level.get("quantity"), 0.0)
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price = safe_float(level[0], 0.0)
            size = safe_float(level[1], 0.0)

        if price > 0 and size > 0:
            parsed.append({"price": price, "size": size})

    parsed.sort(key=lambda x: x["price"], reverse=descending)
    return parsed


@dataclass
class BotConfig:
    source_wallet: str
    clob_host: str = "https://clob.polymarket.com"
    gamma_host: str = "https://gamma-api.polymarket.com"
    data_api_host: str = "https://data-api.polymarket.com"
    market_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    chain_id: int = 137
    private_key: str = ""
    funder: str = ""
    signature_type: int = 2
    dry_run: bool = True
    paper_start_balance: float = 500.0

    copy_balance_fraction: float = 0.0125
    max_trade_usdc: float = 20.0
    min_trade_usdc: float = 2.0
    max_open_positions: int = 12
    copy_sell_trades: bool = False

    min_market_volume_24h: float = 100.0
    min_market_liquidity: float = 5000.0
    max_market_spread: float = 0.025
    max_slippage_from_source: float = 0.025
    min_copy_price: float = 0.24
    book_stale_seconds: float = 8.0
    source_signal_cooldown_seconds: float = 2.0
    market_tag_allowlist: List[str] = field(default_factory=list)
    event_tag_cache_seconds: int = 300
    event_tag_page_size: int = 200
    event_tag_max_pages: int = 20
    signal_only_book_subscriptions: bool = False
    pull_book_on_signal: bool = False
    source_poll_seconds: float = 0.75
    source_poll_limit: int = 200
    source_poll_bootstrap_skip_history: bool = True

    enable_soft_stop: bool = True
    soft_stop_arm_profit_pct: float = 0.05
    soft_stop_trigger_loss_pct: float = 0.035
    close_on_source_sell: bool = True
    auto_claim_winnings: bool = True
    resolution_check_seconds: int = 20
    polygon_rpc_url: str = ""
    ctf_contract_address: str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    usdc_contract_address: str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

    market_refresh_seconds: int = 20
    subscribe_all_active_assets_for_signals: bool = True
    balance_refresh_seconds: int = 8
    risk_loop_seconds: float = 1.0
    equity_snapshot_seconds: int = 5
    websocket_subscribe_chunk_size: int = 200

    dry_run_realistic_fills: bool = True
    dry_run_latency_ms_min: int = 25
    dry_run_latency_ms_max: int = 220
    dry_run_latency_spike_chance: float = 0.08
    dry_run_latency_spike_ms_min: int = 300
    dry_run_latency_spike_ms_max: int = 1500
    dry_run_depth_decay_per_100ms: float = 0.025
    dry_run_random_fail_prob: float = 0.015

    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8080
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "BotConfig":
        source_wallet = os.getenv("SOURCE_WALLET", "").strip().lower()
        return cls(
            source_wallet=source_wallet,
            clob_host=os.getenv("CLOB_HOST", "https://clob.polymarket.com").rstrip("/"),
            gamma_host=os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com").rstrip("/"),
            data_api_host=os.getenv("DATA_API_HOST", "https://data-api.polymarket.com").rstrip("/"),
            market_ws_url=os.getenv("MARKET_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
            chain_id=env_int("CHAIN_ID", 137),
            private_key=os.getenv("POLY_PRIVATE_KEY", "").strip(),
            funder=os.getenv("POLY_FUNDER", "").strip(),
            signature_type=env_int("POLY_SIGNATURE_TYPE", 2),
            dry_run=env_bool("DRY_RUN", True),
            paper_start_balance=env_float("PAPER_START_BALANCE", 500.0),
            copy_balance_fraction=env_float("COPY_BALANCE_FRACTION", 0.0125),
            max_trade_usdc=env_float("MAX_TRADE_USDC", 20.0),
            min_trade_usdc=env_float("MIN_TRADE_USDC", 2.0),
            max_open_positions=env_int("MAX_OPEN_POSITIONS", 12),
            copy_sell_trades=env_bool("COPY_SELL_TRADES", False),
            min_market_volume_24h=env_float("MIN_MARKET_VOLUME_24H", 100.0),
            min_market_liquidity=env_float("MIN_MARKET_LIQUIDITY", 5000.0),
            max_market_spread=env_float("MAX_MARKET_SPREAD", 0.025),
            max_slippage_from_source=env_float("MAX_SLIPPAGE_FROM_SOURCE", 0.025),
            min_copy_price=env_float("MIN_COPY_PRICE", 0.24),
            book_stale_seconds=env_float("BOOK_STALE_SECONDS", 8.0),
            source_signal_cooldown_seconds=env_float("SOURCE_SIGNAL_COOLDOWN_SECONDS", 2.0),
            market_tag_allowlist=env_csv("MARKET_TAG_ALLOWLIST", ""),
            event_tag_cache_seconds=env_int("EVENT_TAG_CACHE_SECONDS", 300),
            event_tag_page_size=env_int("EVENT_TAG_PAGE_SIZE", 200),
            event_tag_max_pages=env_int("EVENT_TAG_MAX_PAGES", 20),
            signal_only_book_subscriptions=env_bool("SIGNAL_ONLY_BOOK_SUBSCRIPTIONS", False),
            pull_book_on_signal=env_bool("PULL_BOOK_ON_SIGNAL", False),
            source_poll_seconds=env_float("SOURCE_POLL_SECONDS", 0.75),
            source_poll_limit=env_int("SOURCE_POLL_LIMIT", 200),
            source_poll_bootstrap_skip_history=env_bool("SOURCE_POLL_BOOTSTRAP_SKIP_HISTORY", True),
            enable_soft_stop=env_bool("ENABLE_SOFT_STOP", True),
            soft_stop_arm_profit_pct=env_float("SOFT_STOP_ARM_PROFIT_PCT", 0.05),
            soft_stop_trigger_loss_pct=env_float("SOFT_STOP_TRIGGER_LOSS_PCT", 0.035),
            close_on_source_sell=env_bool("CLOSE_ON_SOURCE_SELL", True),
            auto_claim_winnings=env_bool("AUTO_CLAIM_WINNINGS", True),
            resolution_check_seconds=env_int("RESOLUTION_CHECK_SECONDS", 20),
            polygon_rpc_url=os.getenv("POLYGON_RPC_URL", "").strip(),
            ctf_contract_address=os.getenv("CTF_CONTRACT_ADDRESS", "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045").strip(),
            usdc_contract_address=os.getenv("USDC_CONTRACT_ADDRESS", "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174").strip(),
            market_refresh_seconds=env_int("MARKET_REFRESH_SECONDS", 20),
            subscribe_all_active_assets_for_signals=env_bool("SUBSCRIBE_ALL_ACTIVE_ASSETS_FOR_SIGNALS", True),
            balance_refresh_seconds=env_int("BALANCE_REFRESH_SECONDS", 8),
            risk_loop_seconds=env_float("RISK_LOOP_SECONDS", 1.0),
            equity_snapshot_seconds=env_int("EQUITY_SNAPSHOT_SECONDS", 5),
            websocket_subscribe_chunk_size=env_int("WEBSOCKET_SUBSCRIBE_CHUNK_SIZE", 200),
            dry_run_realistic_fills=env_bool("DRY_RUN_REALISTIC_FILLS", True),
            dry_run_latency_ms_min=env_int("DRY_RUN_LATENCY_MS_MIN", 25),
            dry_run_latency_ms_max=env_int("DRY_RUN_LATENCY_MS_MAX", 220),
            dry_run_latency_spike_chance=env_float("DRY_RUN_LATENCY_SPIKE_CHANCE", 0.08),
            dry_run_latency_spike_ms_min=env_int("DRY_RUN_LATENCY_SPIKE_MS_MIN", 300),
            dry_run_latency_spike_ms_max=env_int("DRY_RUN_LATENCY_SPIKE_MS_MAX", 1500),
            dry_run_depth_decay_per_100ms=env_float("DRY_RUN_DEPTH_DECAY_PER_100MS", 0.025),
            dry_run_random_fail_prob=env_float("DRY_RUN_RANDOM_FAIL_PROB", 0.015),
            dashboard_host=os.getenv("DASHBOARD_HOST", "127.0.0.1"),
            dashboard_port=env_int("DASHBOARD_PORT", 8080),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )

    def validate(self) -> None:
        if not self.source_wallet:
            raise ValueError("SOURCE_WALLET is required.")
        if not self.source_wallet.startswith("0x"):
            raise ValueError("SOURCE_WALLET must be a hex wallet address.")
        if not self.dry_run and not self.private_key:
            raise ValueError("POLY_PRIVATE_KEY is required when DRY_RUN=false.")
        if self.copy_balance_fraction <= 0:
            raise ValueError("COPY_BALANCE_FRACTION must be > 0.")
        if self.source_poll_seconds <= 0:
            raise ValueError("SOURCE_POLL_SECONDS must be > 0.")
        if self.source_poll_limit <= 0:
            raise ValueError("SOURCE_POLL_LIMIT must be > 0.")
        if not (0.0 <= self.min_copy_price < 1.0):
            raise ValueError("MIN_COPY_PRICE must be between 0 and 1.")
        if self.event_tag_cache_seconds <= 0:
            raise ValueError("EVENT_TAG_CACHE_SECONDS must be > 0.")
        if self.event_tag_page_size <= 0:
            raise ValueError("EVENT_TAG_PAGE_SIZE must be > 0.")
        if self.event_tag_max_pages <= 0:
            raise ValueError("EVENT_TAG_MAX_PAGES must be > 0.")
        if self.enable_soft_stop:
            if self.soft_stop_arm_profit_pct <= 0:
                raise ValueError("SOFT_STOP_ARM_PROFIT_PCT must be > 0.")
            if self.soft_stop_trigger_loss_pct <= 0:
                raise ValueError("SOFT_STOP_TRIGGER_LOSS_PCT must be > 0.")
        if self.auto_claim_winnings and not self.dry_run and not self.polygon_rpc_url:
            raise ValueError("POLYGON_RPC_URL is required when AUTO_CLAIM_WINNINGS=true and DRY_RUN=false.")
        if self.dry_run_realistic_fills:
            if self.dry_run_latency_ms_min < 0 or self.dry_run_latency_ms_max < self.dry_run_latency_ms_min:
                raise ValueError("DRY_RUN_LATENCY_MS_MIN/MAX are invalid.")
            if not (0.0 <= self.dry_run_latency_spike_chance <= 1.0):
                raise ValueError("DRY_RUN_LATENCY_SPIKE_CHANCE must be between 0 and 1.")
            if self.dry_run_latency_spike_ms_min < 0 or self.dry_run_latency_spike_ms_max < self.dry_run_latency_spike_ms_min:
                raise ValueError("DRY_RUN_LATENCY_SPIKE_MS_MIN/MAX are invalid.")
            if not (0.0 <= self.dry_run_depth_decay_per_100ms <= 1.0):
                raise ValueError("DRY_RUN_DEPTH_DECAY_PER_100MS must be between 0 and 1.")
            if not (0.0 <= self.dry_run_random_fail_prob <= 1.0):
                raise ValueError("DRY_RUN_RANDOM_FAIL_PROB must be between 0 and 1.")


@dataclass
class MarketMeta:
    asset_id: str
    market_id: str
    condition_id: str
    outcome_index: int
    question: str
    outcome: str
    volume_24h: float
    liquidity: float
    spread: float
    order_min_size: float
    tick_size: float


@dataclass
class BookTop:
    bid: float
    ask: float
    spread: float
    ts: float
    bid_levels: List[Dict[str, float]]
    ask_levels: List[Dict[str, float]]
    last_trade_price: float = 0.0


@dataclass
class Position:
    position_id: int
    asset_id: str
    market_id: str
    condition_id: str
    outcome_index: int
    question: str
    outcome: str
    side: str
    size: float
    entry_price: float
    entry_notional: float
    source_trade_id: str
    source_wallet: str
    opened_at: str
    peak_price: float
    trough_price: float
    soft_stop_armed: bool = False
    stop_loss_price: Optional[float] = None
    status: str = "OPEN"
    close_reason: Optional[str] = None
    exit_price: Optional[float] = None
    realized_pnl: float = 0.0
    closed_at: Optional[str] = None


@dataclass
class ExecutionResult:
    ok: bool
    order_id: str = ""
    message: str = ""
    raw: Optional[Dict[str, Any]] = None


class PortfolioState:
    def __init__(self, start_balance: float) -> None:
        self._lock = threading.Lock()
        self._position_id_counter = 0
        self.start_equity = start_balance
        self.live_usdc_balance = start_balance
        self.realized_pnl = 0.0
        self.open_positions: Dict[int, Position] = {}
        self.resolved_positions: Deque[Position] = deque(maxlen=5000)
        self.equity_curve: Deque[Dict[str, Any]] = deque(maxlen=10000)

    def set_live_balance(self, balance: float) -> None:
        with self._lock:
            if balance >= 0:
                self.live_usdc_balance = balance
                if self.start_equity <= 0:
                    self.start_equity = balance

    def next_position_id(self) -> int:
        with self._lock:
            self._position_id_counter += 1
            return self._position_id_counter

    def add_open_position(self, position: Position) -> None:
        with self._lock:
            self.open_positions[position.position_id] = position

    def list_open_positions(self) -> List[Position]:
        with self._lock:
            return [Position(**asdict(pos)) for pos in self.open_positions.values()]

    def update_tracking(
        self,
        position_id: int,
        peak_price: float,
        trough_price: float,
        soft_stop_armed: bool,
        stop_loss_price: Optional[float],
    ) -> None:
        with self._lock:
            position = self.open_positions.get(position_id)
            if not position:
                return
            position.peak_price = peak_price
            position.trough_price = trough_price
            position.soft_stop_armed = soft_stop_armed
            position.stop_loss_price = stop_loss_price

    def resolve_position(self, position_id: int, exit_price: float, reason: str, closed_at: str) -> Optional[Position]:
        with self._lock:
            position = self.open_positions.pop(position_id, None)
            if not position:
                return None
            position.exit_price = exit_price
            position.close_reason = reason
            position.closed_at = closed_at
            position.status = "RESOLVED"
            position.realized_pnl = pnl_for_position(position.side, position.entry_price, exit_price, position.size)
            self.realized_pnl += position.realized_pnl
            self.resolved_positions.appendleft(position)
            return Position(**asdict(position))

    def record_equity(self, snapshot: Dict[str, Any]) -> None:
        with self._lock:
            self.equity_curve.append(snapshot)

    def raw_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "start_equity": self.start_equity,
                "live_usdc_balance": self.live_usdc_balance,
                "realized_pnl": self.realized_pnl,
                "open_positions_count": len(self.open_positions),
                "resolved_positions_count": len(self.resolved_positions),
            }

    def resolved_positions_rows(self, limit: int = 200) -> List[Dict[str, Any]]:
        with self._lock:
            rows: List[Dict[str, Any]] = []
            for pos in list(self.resolved_positions)[:limit]:
                result_tag = "WIN" if pos.realized_pnl > 0 else "LOSS" if pos.realized_pnl < 0 else "FLAT"
                rows.append(
                    {
                        "position_id": pos.position_id,
                        "question": pos.question,
                        "outcome": pos.outcome,
                        "side": pos.side,
                        "size": pos.size,
                        "entry_price": pos.entry_price,
                        "exit_price": pos.exit_price,
                        "realized_pnl": pos.realized_pnl,
                        "result_tag": result_tag,
                        "close_reason": pos.close_reason,
                        "opened_at": pos.opened_at,
                        "closed_at": pos.closed_at,
                    }
                )
            return rows

    def equity_curve_rows(self, limit: int = 5000) -> List[Dict[str, Any]]:
        with self._lock:
            if limit <= 0:
                return list(self.equity_curve)
            return list(self.equity_curve)[-limit:]


class ExecutionClient:
    def __init__(self, config: BotConfig, book_provider: Optional[Callable[[str], Optional[BookTop]]] = None) -> None:
        self.config = config
        self.book_provider = book_provider
        self.client: Optional[ClobClient] = None
        self._paper_balance = config.paper_start_balance
        self._web3 = None
        self._wallet_address = ""
        self._claim_abi = [
            {
                "inputs": [
                    {"internalType": "contract IERC20", "name": "collateralToken", "type": "address"},
                    {"internalType": "bytes32", "name": "parentCollectionId", "type": "bytes32"},
                    {"internalType": "bytes32", "name": "conditionId", "type": "bytes32"},
                    {"internalType": "uint256[]", "name": "indexSets", "type": "uint256[]"},
                ],
                "name": "redeemPositions",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function",
            }
        ]

    def _sample_dry_run_latency_ms(self) -> tuple[int, bool]:
        latency = random.randint(self.config.dry_run_latency_ms_min, self.config.dry_run_latency_ms_max)
        spike = random.random() < self.config.dry_run_latency_spike_chance
        if spike:
            latency += random.randint(
                self.config.dry_run_latency_spike_ms_min,
                self.config.dry_run_latency_spike_ms_max,
            )
        return latency, spike

    def _simulate_dry_run_fok(
        self,
        asset_id: str,
        side: str,
        price: float,
        size: float,
        tick_size: float,
    ) -> ExecutionResult:
        order_id = f"dry-{int(time.time() * 1000)}"
        if not self.config.dry_run_realistic_fills or self.book_provider is None:
            return ExecutionResult(
                ok=True,
                order_id=order_id,
                message="Dry run order accepted (simple mode).",
                raw={
                    "dry_run": True,
                    "model": "simple",
                    "asset_id": asset_id,
                    "side": side,
                    "limit_price": price,
                    "size": size,
                    "simulated_fill_price": price,
                },
            )

        book = self.book_provider(asset_id)
        if not book:
            return ExecutionResult(
                ok=False,
                message="Dry run reject: missing order book snapshot.",
                raw={"dry_run": True, "reject_reason": "missing_book"},
            )

        levels = book.ask_levels if side == "BUY" else book.bid_levels
        if side == "BUY":
            eligible = [lvl for lvl in levels if lvl["price"] <= price]
        else:
            eligible = [lvl for lvl in levels if lvl["price"] >= price]

        if not eligible:
            return ExecutionResult(
                ok=False,
                message="Dry run reject: no eligible liquidity at limit price.",
                raw={"dry_run": True, "reject_reason": "no_eligible_depth"},
            )

        latency_ms, spike = self._sample_dry_run_latency_ms()
        base_depth = sum(lvl["size"] for lvl in eligible)
        decay = max(0.0, 1.0 - (latency_ms / 100.0) * self.config.dry_run_depth_decay_per_100ms)
        if spike:
            decay *= random.uniform(0.55, 0.82)
        else:
            decay *= random.uniform(0.9, 1.0)

        adjusted_levels = [
            {"price": lvl["price"], "size": max(0.0, lvl["size"] * decay)} for lvl in eligible
        ]
        adjusted_depth = sum(lvl["size"] for lvl in adjusted_levels)

        if adjusted_depth < size:
            return ExecutionResult(
                ok=False,
                message="Dry run reject: FOK not fully fillable after latency/depth decay.",
                raw={
                    "dry_run": True,
                    "model": "realistic",
                    "reject_reason": "insufficient_depth_after_latency",
                    "asset_id": asset_id,
                    "side": side,
                    "limit_price": price,
                    "requested_size": size,
                    "base_depth": base_depth,
                    "adjusted_depth": adjusted_depth,
                    "simulated_latency_ms": latency_ms,
                    "latency_spike": spike,
                },
            )

        if random.random() < self.config.dry_run_random_fail_prob:
            return ExecutionResult(
                ok=False,
                message="Dry run reject: queue race loss.",
                raw={
                    "dry_run": True,
                    "model": "realistic",
                    "reject_reason": "queue_race",
                    "asset_id": asset_id,
                    "side": side,
                    "simulated_latency_ms": latency_ms,
                    "latency_spike": spike,
                },
            )

        remaining = size
        notional = 0.0
        for lvl in adjusted_levels:
            if remaining <= 0:
                break
            take = min(remaining, lvl["size"])
            notional += take * lvl["price"]
            remaining -= take

        fill_price = notional / size if size > 0 else price
        tick = tick_size if tick_size > 0 else 0.001
        extra_ticks = 0
        if latency_ms > 150:
            extra_ticks += random.choice([0, 1])
        if latency_ms > 450:
            extra_ticks += random.choice([0, 1, 2])
        if spike:
            extra_ticks += random.choice([1, 2])

        if side == "BUY":
            fill_price += extra_ticks * tick
            if fill_price > price:
                return ExecutionResult(
                    ok=False,
                    message="Dry run reject: simulated drift breached buy limit.",
                    raw={
                        "dry_run": True,
                        "model": "realistic",
                        "reject_reason": "limit_breached",
                        "asset_id": asset_id,
                        "side": side,
                        "limit_price": price,
                        "simulated_fill_price": fill_price,
                        "simulated_latency_ms": latency_ms,
                        "latency_spike": spike,
                    },
                )
        else:
            fill_price -= extra_ticks * tick
            if fill_price < price:
                return ExecutionResult(
                    ok=False,
                    message="Dry run reject: simulated drift breached sell limit.",
                    raw={
                        "dry_run": True,
                        "model": "realistic",
                        "reject_reason": "limit_breached",
                        "asset_id": asset_id,
                        "side": side,
                        "limit_price": price,
                        "simulated_fill_price": fill_price,
                        "simulated_latency_ms": latency_ms,
                        "latency_spike": spike,
                    },
                )

        return ExecutionResult(
            ok=True,
            order_id=order_id,
            message="Dry run order filled (realistic model).",
            raw={
                "dry_run": True,
                "model": "realistic",
                "asset_id": asset_id,
                "side": side,
                "limit_price": price,
                "size": size,
                "base_depth": base_depth,
                "adjusted_depth": adjusted_depth,
                "simulated_fill_price": fill_price,
                "simulated_latency_ms": latency_ms,
                "latency_spike": spike,
                "depth_decay_factor": decay,
            },
        )

    def initialize(self) -> None:
        if self.config.dry_run and not self.config.private_key:
            LOGGER.warning("DRY_RUN enabled without POLY_PRIVATE_KEY. Using paper-only mode.")
            return

        funder_arg = self.config.funder or None
        self.client = ClobClient(
            host=self.config.clob_host,
            chain_id=self.config.chain_id,
            key=self.config.private_key,
            signature_type=self.config.signature_type,
            funder=funder_arg,
        )
        try:
            # Most accounts already have API creds; derive first to avoid noisy create->400 fallback logs.
            creds = self.client.derive_api_key()
        except Exception:  # noqa: BLE001
            creds = self.client.create_api_key()
        if creds is None:
            raise RuntimeError("Failed to obtain CLOB API credentials.")
        self.client.set_api_creds(creds)
        LOGGER.info("Authenticated CLOB client initialized.")

        if self.config.auto_claim_winnings and Web3 is not None and self.config.polygon_rpc_url:
            try:
                self._web3 = Web3(Web3.HTTPProvider(self.config.polygon_rpc_url))
                self._wallet_address = self._web3.eth.account.from_key(self.config.private_key).address
                LOGGER.info("Auto-claim web3 client initialized.")
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Failed to initialize auto-claim web3 client: %s", exc)

    def get_live_balance_usdc(self) -> float:
        if self.client is None:
            return self._paper_balance
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=self.config.signature_type)
        response = self.client.get_balance_allowance(params)
        balance = extract_balance_number(response)
        if balance >= 0:
            self._paper_balance = balance
        return self._paper_balance

    def place_fok_order(self, asset_id: str, side: str, price: float, size: float, tick_size: float = 0.001) -> ExecutionResult:
        if self.config.dry_run:
            return self._simulate_dry_run_fok(asset_id=asset_id, side=side, price=price, size=size, tick_size=tick_size)

        if self.client is None:
            return ExecutionResult(ok=False, message="Execution client not initialized.")

        try:
            order_args = OrderArgs(token_id=asset_id, price=price, size=size, side=side)
            order = self.client.create_order(order_args)
            response = self.client.post_order(order, OrderType.FOK)
            order_id = (
                str(response.get("orderID") or response.get("order_id") or response.get("id") or "")
                if isinstance(response, dict)
                else ""
            )
            return ExecutionResult(ok=True, order_id=order_id, raw=response)
        except Exception as exc:  # noqa: BLE001
            return ExecutionResult(ok=False, message=str(exc))

    def claim_winnings(self, condition_id: str) -> ExecutionResult:
        if self.config.dry_run:
            return ExecutionResult(ok=True, message=f"Dry run claim simulated for {condition_id}")

        if not self.config.auto_claim_winnings:
            return ExecutionResult(ok=False, message="AUTO_CLAIM_WINNINGS is disabled.")
        if Web3 is None:
            return ExecutionResult(ok=False, message="web3 is not installed.")
        if self._web3 is None:
            return ExecutionResult(ok=False, message="Web3 client is not initialized.")
        if not self.config.private_key:
            return ExecutionResult(ok=False, message="POLY_PRIVATE_KEY is required for claiming.")

        try:
            if not condition_id.startswith("0x") or len(condition_id) != 66:
                return ExecutionResult(ok=False, message=f"Invalid condition id: {condition_id}")

            w3 = self._web3
            wallet = self._wallet_address
            ctf_addr = Web3.to_checksum_address(self.config.ctf_contract_address)
            usdc_addr = Web3.to_checksum_address(self.config.usdc_contract_address)
            contract = w3.eth.contract(address=ctf_addr, abi=self._claim_abi)

            tx = contract.functions.redeemPositions(
                usdc_addr,
                bytes(32),
                bytes.fromhex(condition_id[2:]),
                [1, 2],
            ).build_transaction(
                {
                    "from": wallet,
                    "nonce": w3.eth.get_transaction_count(wallet, "pending"),
                    "gas": 450000,
                    "gasPrice": w3.eth.gas_price,
                    "chainId": self.config.chain_id,
                }
            )
            signed = w3.eth.account.sign_transaction(tx, self.config.private_key)
            tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
            if receipt.status == 1:
                return ExecutionResult(ok=True, order_id=tx_hash.hex(), message="Claim transaction confirmed.")
            return ExecutionResult(ok=False, order_id=tx_hash.hex(), message="Claim transaction reverted.")
        except Exception as exc:  # noqa: BLE001
            return ExecutionResult(ok=False, message=str(exc))


class PolymarketCopyTrader:
    def __init__(self, config: BotConfig) -> None:
        self.config = config
        # In live mode, anchor equity to the first fetched wallet balance instead of paper settings.
        start_balance = config.paper_start_balance if config.dry_run else 0.0
        self.state = PortfolioState(start_balance=start_balance)
        self.books: Dict[str, BookTop] = {}
        self.execution = ExecutionClient(config, book_provider=self.get_book_snapshot)
        self.http = requests.Session()
        self.market_meta_by_asset: Dict[str, MarketMeta] = {}
        self.subscription_asset_ids: List[str] = []
        self.market_universe_version: int = 0
        self.seen_signal_timestamps: Dict[str, float] = {}
        self.last_copied_timestamp_by_asset: Dict[str, float] = {}
        self.decision_counts: Dict[str, int] = {}
        self.last_decision_summary_ts: float = time.time()
        self.source_poll_bootstrapped: bool = False
        self.source_poll_high_watermark: int = 0
        self.source_poll_start_ts: int = 0
        self.source_poll_seen_keys: Deque[str] = deque()
        self.source_poll_seen_key_set: Set[str] = set()
        self.signal_asset_ids: Set[str] = set()
        self.allowed_market_ids_by_tags: Optional[Set[str]] = None
        self.allowed_market_ids_by_tags_ts: float = 0.0
        self.claimed_conditions: Set[str] = set()
        self._shutdown = asyncio.Event()
        self._dashboard_server: Optional[uvicorn.Server] = None

    def get_book_snapshot(self, asset_id: str) -> Optional[BookTop]:
        return self.books.get(asset_id)

    def dashboard_app(self) -> FastAPI:
        app = FastAPI(title="Polymarket HFT Copy Trader")
        html_path = Path(__file__).resolve().parents[1] / "dashboard" / "index.html"

        @app.get("/", response_class=HTMLResponse)
        async def dashboard_home() -> HTMLResponse:
            return HTMLResponse(html_path.read_text(encoding="utf-8"))

        @app.get("/api/overview", response_class=JSONResponse)
        async def api_overview() -> JSONResponse:
            return JSONResponse(self.build_overview())

        @app.get("/api/positions/open", response_class=JSONResponse)
        async def api_positions_open() -> JSONResponse:
            return JSONResponse(self.open_positions_rows())

        @app.get("/api/positions/resolved", response_class=JSONResponse)
        async def api_positions_resolved() -> JSONResponse:
            return JSONResponse(self.state.resolved_positions_rows())

        @app.get("/api/equity", response_class=JSONResponse)
        async def api_equity() -> JSONResponse:
            return JSONResponse(self.state.equity_curve_rows())

        return app

    def mark_price_for_position(self, position: Position) -> Optional[float]:
        book = self.books.get(position.asset_id)
        if not book:
            return None
        if book.bid > 0 and book.ask > 0:
            # Use mid-price for PnL marking so deployed notional is not treated as immediate loss.
            mid = (book.bid + book.ask) / 2.0
            very_wide = book.spread > 0.08
            if very_wide and book.last_trade_price > 0:
                return clamp(book.last_trade_price, 0.001, 0.999)
            return clamp(mid, 0.001, 0.999)
        if book.last_trade_price > 0:
            return clamp(book.last_trade_price, 0.001, 0.999)
        if book.bid > 0:
            return clamp(book.bid, 0.001, 0.999)
        if book.ask > 0:
            return clamp(book.ask, 0.001, 0.999)
        return None

    def open_positions_rows(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for pos in self.state.list_open_positions():
            mark = self.mark_price_for_position(pos)
            unrealized = pnl_for_position(pos.side, pos.entry_price, mark, pos.size) if mark else 0.0
            pnl_pct = pnl_pct_for_position(pos.side, pos.entry_price, mark) * 100 if mark else 0.0
            rows.append(
                {
                    "position_id": pos.position_id,
                    "question": pos.question,
                    "outcome": pos.outcome,
                    "side": pos.side,
                    "size": pos.size,
                    "entry_price": pos.entry_price,
                    "mark_price": mark,
                    "entry_notional": pos.entry_notional,
                    "unrealized_pnl": unrealized,
                    "unrealized_pnl_pct": pnl_pct,
                    "soft_stop_armed": pos.soft_stop_armed,
                    "stop_loss_price": pos.stop_loss_price,
                    "opened_at": pos.opened_at,
                    "status_tag": "OPEN",
                }
            )
        rows.sort(key=lambda x: x["position_id"], reverse=True)
        return rows

    def _open_unrealized(self) -> float:
        total = 0.0
        for row in self.open_positions_rows():
            total += safe_float(row.get("unrealized_pnl"), 0.0)
        return total

    def max_drawdown_pct(self) -> float:
        curve = self.state.equity_curve_rows(limit=5000)
        if not curve:
            return 0.0
        peak = -math.inf
        max_dd = 0.0
        for point in curve:
            equity = safe_float(point.get("equity"), 0.0)
            if equity > peak:
                peak = equity
            if peak > 0:
                drawdown = (peak - equity) / peak
                max_dd = max(max_dd, drawdown)
        return max_dd * 100

    def build_overview(self) -> Dict[str, Any]:
        base = self.state.raw_snapshot()
        realized_rows = self.state.resolved_positions_rows(limit=5000)
        wins = sum(1 for row in realized_rows if safe_float(row["realized_pnl"]) > 0)
        resolved_count = len(realized_rows)
        win_rate = (wins / resolved_count * 100) if resolved_count else 0.0
        unrealized = self._open_unrealized()
        equity = base["start_equity"] + base["realized_pnl"] + unrealized
        total_return_pct = ((equity - base["start_equity"]) / base["start_equity"] * 100) if base["start_equity"] else 0.0
        return {
            "timestamp": utc_now_iso(),
            "source_wallet": self.config.source_wallet,
            "dry_run": self.config.dry_run,
            "tracked_assets": len(self.market_meta_by_asset),
            "open_positions": base["open_positions_count"],
            "resolved_positions": base["resolved_positions_count"],
            "live_usdc_balance": round(base["live_usdc_balance"], 4),
            "start_equity": round(base["start_equity"], 4),
            "equity": round(equity, 4),
            "realized_pnl": round(base["realized_pnl"], 4),
            "unrealized_pnl": round(unrealized, 4),
            "total_return_pct": round(total_return_pct, 3),
            "win_rate_pct": round(win_rate, 3),
            "max_drawdown_pct": round(self.max_drawdown_pct(), 3),
        }

    async def run(self) -> None:
        self.execution.initialize()
        self.state.set_live_balance(self.execution.get_live_balance_usdc())
        try:
            await self.refresh_market_quality()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Initial market refresh failed: %s", exc)
        await self.start_dashboard()

        tasks = [
            asyncio.create_task(self.market_quality_loop(), name="market_quality_loop"),
            asyncio.create_task(self.balance_loop(), name="balance_loop"),
            asyncio.create_task(self.risk_loop(), name="risk_loop"),
            asyncio.create_task(self.resolution_loop(), name="resolution_loop"),
            asyncio.create_task(self.equity_loop(), name="equity_loop"),
            asyncio.create_task(self.source_signal_loop(), name="source_signal_loop"),
            asyncio.create_task(self.market_ws_loop(), name="market_ws_loop"),
        ]
        LOGGER.info("Bot running. Dashboard: http://%s:%s", self.config.dashboard_host, self.config.dashboard_port)

        try:
            await asyncio.gather(*tasks)
        finally:
            self._shutdown.set()
            for task in tasks:
                task.cancel()
            await self.stop_dashboard()

    async def start_dashboard(self) -> None:
        chosen_port = find_available_port(self.config.dashboard_host, self.config.dashboard_port)
        if chosen_port != self.config.dashboard_port:
            LOGGER.warning("Dashboard port %s unavailable. Falling back to %s.", self.config.dashboard_port, chosen_port)
            self.config.dashboard_port = chosen_port
        app = self.dashboard_app()
        config = uvicorn.Config(
            app=app,
            host=self.config.dashboard_host,
            port=self.config.dashboard_port,
            log_level=self.config.log_level.lower(),
            access_log=False,
        )
        self._dashboard_server = uvicorn.Server(config)
        asyncio.create_task(self._dashboard_server.serve(), name="dashboard_server")
        await asyncio.sleep(0.1)

    async def stop_dashboard(self) -> None:
        if self._dashboard_server:
            self._dashboard_server.should_exit = True

    async def market_quality_loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                await self.refresh_market_quality()
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Market refresh failed: %s", exc)
            await asyncio.sleep(self.config.market_refresh_seconds)

    async def balance_loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                self.state.set_live_balance(self.execution.get_live_balance_usdc())
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Balance refresh failed: %s", exc)
            await asyncio.sleep(self.config.balance_refresh_seconds)

    async def equity_loop(self) -> None:
        while not self._shutdown.is_set():
            overview = self.build_overview()
            self.state.record_equity(
                {
                    "timestamp": overview["timestamp"],
                    "equity": overview["equity"],
                    "live_usdc_balance": overview["live_usdc_balance"],
                    "realized_pnl": overview["realized_pnl"],
                    "unrealized_pnl": overview["unrealized_pnl"],
                }
            )
            await asyncio.sleep(self.config.equity_snapshot_seconds)

    async def risk_loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                await self.evaluate_open_positions()
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Risk loop error: %s", exc)
            await asyncio.sleep(self.config.risk_loop_seconds)

    async def resolution_loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                await self.reconcile_resolved_markets()
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Resolution loop error: %s", exc)
            await asyncio.sleep(self.config.resolution_check_seconds)

    async def evaluate_open_positions(self) -> None:
        open_positions = self.state.list_open_positions()
        for pos in open_positions:
            mark = self.mark_price_for_position(pos)
            if not mark:
                continue
            pnl_pct = pnl_pct_for_position(pos.side, pos.entry_price, mark)
            peak = pos.peak_price
            trough = pos.trough_price
            soft_stop_armed = pos.soft_stop_armed
            stop_loss_price = pos.stop_loss_price

            if pos.side == "BUY":
                peak = max(peak, mark)
            else:
                trough = min(trough, mark)

            if self.config.enable_soft_stop and not soft_stop_armed and pnl_pct >= self.config.soft_stop_arm_profit_pct:
                soft_stop_armed = True
                stop_loss_price = (
                    pos.entry_price * (1 - self.config.soft_stop_trigger_loss_pct)
                    if pos.side == "BUY"
                    else pos.entry_price * (1 + self.config.soft_stop_trigger_loss_pct)
                )

            self.state.update_tracking(pos.position_id, peak, trough, soft_stop_armed, stop_loss_price)

            if self.config.enable_soft_stop and soft_stop_armed and pnl_pct <= -self.config.soft_stop_trigger_loss_pct:
                await self.close_position(pos, "soft_stop_after_profit", mark)

    async def close_position(self, position: Position, reason: str, mark_price: float) -> None:
        exit_side = opposite_side(position.side)
        book = self.books.get(position.asset_id)
        if not book:
            return
        limit_price = book.bid if exit_side == "SELL" else book.ask
        if limit_price <= 0:
            return

        meta = self.market_meta_by_asset.get(position.asset_id)
        tick_size = meta.tick_size if meta else 0.001
        result = self.execution.place_fok_order(
            asset_id=position.asset_id,
            side=exit_side,
            price=round_to_tick(limit_price, tick_size),
            size=round_size(position.size),
            tick_size=tick_size,
        )
        if not result.ok:
            return

        filled_exit_price = mark_price
        if isinstance(result.raw, dict):
            filled_exit_price = safe_float(result.raw.get("simulated_fill_price"), mark_price)

        resolved = self.state.resolve_position(
            position_id=position.position_id,
            exit_price=filled_exit_price,
            reason=reason,
            closed_at=utc_now_iso(),
        )
        if resolved:
            LOGGER.info(
                "Closed pos=%s side=%s pnl=%.4f reason=%s",
                resolved.position_id,
                resolved.side,
                resolved.realized_pnl,
                reason,
            )

    async def settle_position_at_resolution(self, position: Position, exit_price: float, reason: str) -> None:
        resolved = self.state.resolve_position(
            position_id=position.position_id,
            exit_price=exit_price,
            reason=reason,
            closed_at=utc_now_iso(),
        )
        if resolved:
            LOGGER.info(
                "Settled pos=%s by resolution side=%s pnl=%.4f reason=%s",
                resolved.position_id,
                resolved.side,
                resolved.realized_pnl,
                reason,
            )

    async def close_positions_on_source_sell(self, asset_id: str, source_trade_id: str) -> None:
        matching = [p for p in self.state.list_open_positions() if p.asset_id == asset_id and p.side == "BUY"]
        for pos in matching:
            mark = self.mark_price_for_position(pos)
            if mark is None:
                mark = pos.entry_price
            await self.close_position(pos, f"source_wallet_sell:{source_trade_id}", mark)

    async def reconcile_resolved_markets(self) -> None:
        open_positions = self.state.list_open_positions()
        if not open_positions:
            return

        grouped: Dict[str, List[Position]] = {}
        for pos in open_positions:
            grouped.setdefault(pos.market_id, []).append(pos)

        for market_id, positions in grouped.items():
            try:
                response = self.http.get(f"{self.config.gamma_host}/markets/{market_id}", timeout=12)
                response.raise_for_status()
                market = response.json()
            except Exception:
                continue

            if not isinstance(market, dict):
                continue
            if not self.market_is_resolved(market):
                continue

            outcome_prices = parse_json_list(market.get("outcomePrices"))
            token_ids = parse_json_list(market.get("clobTokenIds"))
            asset_price_map: Dict[str, float] = {}
            for i, token_id in enumerate(token_ids):
                price = safe_float(outcome_prices[i], -1.0) if i < len(outcome_prices) else -1.0
                if price >= 0:
                    asset_price_map[str(token_id)] = price

            for pos in positions:
                exit_price = asset_price_map.get(pos.asset_id)
                if exit_price is None:
                    mark = self.mark_price_for_position(pos)
                    exit_price = mark if mark is not None else pos.entry_price
                await self.settle_position_at_resolution(pos, exit_price, "market_resolved")

            condition_id = str(market.get("conditionId") or positions[0].condition_id or "")
            if condition_id and condition_id not in self.claimed_conditions and self.config.auto_claim_winnings:
                if any(asset_price_map.get(pos.asset_id, 0.0) >= 0.999 for pos in positions):
                    claim_result = self.execution.claim_winnings(condition_id)
                    if claim_result.ok:
                        self.claimed_conditions.add(condition_id)
                        LOGGER.info("Auto-claim succeeded for condition=%s tx=%s", condition_id, claim_result.order_id)
                    else:
                        LOGGER.warning("Auto-claim failed for condition=%s: %s", condition_id, claim_result.message)

    def market_is_resolved(self, market: Dict[str, Any]) -> bool:
        if market.get("closed") is True:
            return True
        status = str(market.get("umaResolutionStatus") or "").lower()
        if status == "resolved":
            return True
        if market.get("automaticallyResolved") is True:
            return True
        if market.get("acceptingOrders") is False:
            prices = [safe_float(x, -1.0) for x in parse_json_list(market.get("outcomePrices"))]
            if prices and any(p >= 0.999 for p in prices):
                return True
        return False

    def allowed_market_ids_from_event_tags(self) -> Optional[Set[str]]:
        allow_tags = {tag.strip().lower() for tag in self.config.market_tag_allowlist if tag.strip()}
        if not allow_tags:
            return None

        now = time.time()
        if self.allowed_market_ids_by_tags is not None and now - self.allowed_market_ids_by_tags_ts < self.config.event_tag_cache_seconds:
            return self.allowed_market_ids_by_tags

        allowed_market_ids: Set[str] = set()
        offset = 0
        page_size = max(1, self.config.event_tag_page_size)
        max_pages = max(1, self.config.event_tag_max_pages)
        for _ in range(max_pages):
            response = self.http.get(
                f"{self.config.gamma_host}/events",
                params={"limit": page_size, "offset": offset, "active": "true", "closed": "false"},
                timeout=20,
            )
            response.raise_for_status()
            events = response.json()
            if not isinstance(events, list) or not events:
                break

            for event in events:
                if not isinstance(event, dict):
                    continue
                tags = event.get("tags") or []
                tag_slugs: Set[str] = set()
                if isinstance(tags, list):
                    for tag in tags:
                        if isinstance(tag, dict):
                            slug = str(tag.get("slug") or "").strip().lower()
                            if slug:
                                tag_slugs.add(slug)
                if not tag_slugs.intersection(allow_tags):
                    continue

                markets = event.get("markets") or []
                if not isinstance(markets, list):
                    continue
                for event_market in markets:
                    market_id = ""
                    if isinstance(event_market, dict):
                        market_id = str(event_market.get("id") or "")
                    elif isinstance(event_market, (str, int)):
                        market_id = str(event_market)
                    if market_id:
                        allowed_market_ids.add(market_id)

            if len(events) < page_size:
                break
            offset += page_size

        self.allowed_market_ids_by_tags = allowed_market_ids
        self.allowed_market_ids_by_tags_ts = now
        LOGGER.info(
            "Event tag filter active tags=%s allowed_markets=%s",
            ",".join(sorted(allow_tags)),
            len(allowed_market_ids),
        )
        return allowed_market_ids

    def fetch_book_snapshot_rest(self, asset_id: str) -> Optional[Dict[str, Any]]:
        try:
            response = self.http.get(
                f"{self.config.clob_host}/book",
                params={"token_id": asset_id},
                timeout=8,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Book snapshot fetch failed asset=%s err=%s", asset_id, exc)
            return None

        if not isinstance(payload, dict):
            return None
        bid_levels = parse_orderbook_levels(payload.get("bids") or [], descending=True)
        ask_levels = parse_orderbook_levels(payload.get("asks") or [], descending=False)
        best_bid = bid_levels[0]["price"] if bid_levels else 0.0
        best_ask = ask_levels[0]["price"] if ask_levels else 0.0
        if best_bid <= 0 and best_ask <= 0:
            return None

        spread = (best_ask - best_bid) if best_bid > 0 and best_ask > 0 else 1.0
        book = BookTop(
            bid=best_bid,
            ask=best_ask,
            spread=spread,
            ts=time.time(),
            bid_levels=bid_levels,
            ask_levels=ask_levels,
            last_trade_price=safe_float(payload.get("last_trade_price"), 0.0),
        )
        self.books[asset_id] = book
        return {
            "book": book,
            "tick_size": safe_float(payload.get("tick_size"), 0.001),
            "min_order_size": safe_float(payload.get("min_order_size"), 5.0),
            "market_id": str(payload.get("market") or ""),
        }

    def ensure_asset_context_from_trade(self, asset_id: str, trade: Dict[str, Any]) -> None:
        if not asset_id:
            return

        existing_book = self.books.get(asset_id)
        need_fresh_book = existing_book is None or time.time() - existing_book.ts > self.config.book_stale_seconds
        need_meta = asset_id not in self.market_meta_by_asset
        fetched = self.fetch_book_snapshot_rest(asset_id) if (need_fresh_book or need_meta) else None
        tick_size = safe_float(fetched.get("tick_size"), 0.001) if isinstance(fetched, dict) else 0.001
        min_order_size = safe_float(fetched.get("min_order_size"), 5.0) if isinstance(fetched, dict) else 5.0
        market_id = str(fetched.get("market_id") or "") if isinstance(fetched, dict) else ""

        if asset_id not in self.market_meta_by_asset:
            question = str(trade.get("title") or trade.get("question") or trade.get("slug") or f"Asset {asset_id}")
            outcome = str(trade.get("outcome") or "Unknown")
            condition_id = str(trade.get("conditionId") or "")
            outcome_index = int(safe_float(trade.get("outcomeIndex"), 0.0))
            self.market_meta_by_asset[asset_id] = MarketMeta(
                asset_id=asset_id,
                market_id=market_id,
                condition_id=condition_id,
                outcome_index=outcome_index,
                question=question,
                outcome=outcome,
                volume_24h=0.0,
                liquidity=0.0,
                spread=0.0,
                order_min_size=min_order_size if min_order_size > 0 else 5.0,
                tick_size=tick_size if tick_size > 0 else 0.001,
            )
            self.record_decision("hydrated_meta_from_signal")

        if asset_id not in self.subscription_asset_ids:
            self.subscription_asset_ids.append(asset_id)
            self.subscription_asset_ids = sorted(set(self.subscription_asset_ids))
            self.market_universe_version += 1
            LOGGER.info(
                "Added source signal asset=%s to websocket universe. universe_version=%s",
                asset_id,
                self.market_universe_version,
            )
        self.signal_asset_ids.add(asset_id)

    async def refresh_market_quality(self) -> None:
        url = f"{self.config.gamma_host}/markets?limit=1000&closed=false&active=true"
        response = self.http.get(url, timeout=15)
        response.raise_for_status()
        markets = response.json()
        if not isinstance(markets, list):
            raise ValueError("Gamma markets response is not a list.")

        allowed_market_ids = self.allowed_market_ids_from_event_tags()
        next_meta: Dict[str, MarketMeta] = {}
        next_subscription_assets: Set[str] = set()
        for market in markets:
            if not market.get("acceptingOrders"):
                continue
            market_id = str(market.get("id") or "")
            if allowed_market_ids is not None:
                if not market_id or market_id not in allowed_market_ids:
                    continue

            raw_assets = market.get("clobTokenIds", "[]")
            raw_outcomes = market.get("outcomes", "[]")
            try:
                asset_ids = json.loads(raw_assets) if isinstance(raw_assets, str) else list(raw_assets)
            except Exception:  # noqa: BLE001
                continue
            try:
                outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else list(raw_outcomes)
            except Exception:  # noqa: BLE001
                outcomes = []

            if self.config.subscribe_all_active_assets_for_signals and not self.config.signal_only_book_subscriptions:
                for asset_id in asset_ids:
                    if asset_id:
                        next_subscription_assets.add(str(asset_id))

            volume_24h = safe_float(market.get("volume24hr"), 0.0)
            liquidity = safe_float(market.get("liquidityNum"), 0.0)
            spread = safe_float(market.get("spread"), 1.0)
            if volume_24h < self.config.min_market_volume_24h:
                continue
            if liquidity < self.config.min_market_liquidity:
                continue
            if spread > self.config.max_market_spread:
                continue

            question = str(market.get("question") or "")
            condition_id = str(market.get("conditionId") or "")
            order_min_size = safe_float(market.get("orderMinSize"), 5.0)
            tick_size = safe_float(market.get("orderPriceMinTickSize"), 0.001)

            for idx, asset_id in enumerate(asset_ids):
                if not asset_id:
                    continue
                outcome = outcomes[idx] if idx < len(outcomes) else f"Outcome {idx + 1}"
                next_meta[str(asset_id)] = MarketMeta(
                    asset_id=str(asset_id),
                    market_id=market_id,
                    condition_id=condition_id,
                    outcome_index=idx,
                    question=question,
                    outcome=str(outcome),
                    volume_24h=volume_24h,
                    liquidity=liquidity,
                    spread=spread,
                    order_min_size=order_min_size,
                    tick_size=tick_size,
                )
                if not self.config.subscribe_all_active_assets_for_signals and not self.config.signal_only_book_subscriptions:
                    next_subscription_assets.add(str(asset_id))

        for asset_id in self.signal_asset_ids:
            if asset_id:
                next_subscription_assets.add(asset_id)
            if asset_id not in next_meta and asset_id in self.market_meta_by_asset:
                next_meta[asset_id] = self.market_meta_by_asset[asset_id]

        for pos in self.state.list_open_positions():
            if pos.asset_id:
                next_subscription_assets.add(pos.asset_id)
            if pos.asset_id not in next_meta and pos.asset_id in self.market_meta_by_asset:
                next_meta[pos.asset_id] = self.market_meta_by_asset[pos.asset_id]

        old_subscription_assets = set(self.subscription_asset_ids)
        if old_subscription_assets != next_subscription_assets:
            self.market_universe_version += 1

        self.subscription_asset_ids = sorted(next_subscription_assets)
        self.market_meta_by_asset = next_meta
        LOGGER.info(
            "Market refresh complete. Tradable assets=%s subscribed assets=%s universe_version=%s",
            len(self.market_meta_by_asset),
            len(self.subscription_asset_ids),
            self.market_universe_version,
        )

    async def market_ws_loop(self) -> None:
        backoff = 1.0
        while not self._shutdown.is_set():
            tracked_assets = list(self.subscription_asset_ids)
            if not tracked_assets and not self.config.signal_only_book_subscriptions:
                tracked_assets = list(self.market_meta_by_asset.keys())
            if not tracked_assets:
                await asyncio.sleep(1)
                continue
            try:
                async with websockets.connect(
                    self.config.market_ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=2048,
                    open_timeout=20,
                ) as ws:
                    subscribed_version = self.market_universe_version
                    await self.subscribe_assets(ws, tracked_assets)
                    backoff = 1.0
                    async for raw in ws:
                        await self.handle_ws_message(raw)
                        if self.market_universe_version != subscribed_version:
                            LOGGER.info(
                                "Market universe changed (%s -> %s); reconnecting websocket for resubscribe.",
                                subscribed_version,
                                self.market_universe_version,
                            )
                            await ws.close()
                            break
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Websocket disconnected: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def subscribe_assets(self, ws: websockets.WebSocketClientProtocol, asset_ids: List[str]) -> None:
        chunk_size = max(1, self.config.websocket_subscribe_chunk_size)
        subscribed = 0
        for bucket in chunks(asset_ids, chunk_size):
            await ws.send(json.dumps({"assets_ids": bucket, "type": "market"}))
            subscribed += len(bucket)
        LOGGER.info("Subscribed to %s assets on market websocket.", subscribed)

    async def handle_ws_message(self, raw: str) -> None:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return
        events = parsed if isinstance(parsed, list) else [parsed]
        for event in events:
            if not isinstance(event, dict):
                continue
            event_type = str(event.get("event_type", "")).lower()
            if event_type == "book":
                self.update_book(event)
            elif event_type == "trade":
                await self.handle_trade_event(event)

    def update_book(self, event: Dict[str, Any]) -> None:
        asset_id = str(event.get("asset_id") or "")
        if not asset_id:
            return
        bid_levels = parse_orderbook_levels(event.get("bids") or [], descending=True)
        ask_levels = parse_orderbook_levels(event.get("asks") or [], descending=False)
        best_bid = bid_levels[0]["price"] if bid_levels else 0.0
        best_ask = ask_levels[0]["price"] if ask_levels else 0.0
        if best_bid <= 0 and best_ask <= 0:
            return
        spread = (best_ask - best_bid) if best_bid > 0 and best_ask > 0 else 1.0
        self.books[asset_id] = BookTop(
            bid=best_bid,
            ask=best_ask,
            spread=spread,
            ts=time.time(),
            bid_levels=bid_levels,
            ask_levels=ask_levels,
            last_trade_price=safe_float(event.get("last_trade_price"), 0.0),
        )

    def record_decision(self, reason: str) -> None:
        self.decision_counts[reason] = self.decision_counts.get(reason, 0) + 1
        now = time.time()
        if now - self.last_decision_summary_ts < 60:
            return
        summary = ", ".join(
            f"{key}={value}" for key, value in sorted(self.decision_counts.items(), key=lambda kv: (-kv[1], kv[0]))
        )
        if summary:
            LOGGER.info("Signal/copy stats (last ~60s): %s", summary)
        self.decision_counts.clear()
        self.last_decision_summary_ts = now

    def _make_poll_trade_key(self, trade: Dict[str, Any]) -> str:
        trade_id = str(trade.get("transactionHash") or trade.get("id") or trade.get("timestamp") or "")
        timestamp = int(safe_float(trade.get("timestamp"), 0.0))
        asset_id = str(trade.get("asset") or trade.get("asset_id") or "")
        side = str(trade.get("side") or "").upper()
        price = round(safe_float(trade.get("price"), 0.0), 6)
        size = round(safe_float(trade.get("size"), safe_float(trade.get("amount"), 0.0)), 4)
        return f"{trade_id}:{timestamp}:{asset_id}:{side}:{price}:{size}"

    def _remember_poll_trade_key(self, key: str) -> None:
        if key in self.source_poll_seen_key_set:
            return
        self.source_poll_seen_keys.append(key)
        self.source_poll_seen_key_set.add(key)
        while len(self.source_poll_seen_keys) > 8000:
            evicted = self.source_poll_seen_keys.popleft()
            self.source_poll_seen_key_set.discard(evicted)

    def _fetch_source_wallet_trades(self) -> List[Dict[str, Any]]:
        url = f"{self.config.data_api_host}/trades"
        params = {
            "user": self.config.source_wallet,
            "limit": self.config.source_poll_limit,
            "takerOnly": "false",
        }
        response = self.http.get(url, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            for key in ("data", "trades", "items"):
                rows = payload.get(key)
                if isinstance(rows, list):
                    return [row for row in rows if isinstance(row, dict)]
        return []

    async def source_signal_loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                await self.poll_source_wallet_once()
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Source signal poll failed: %s", exc)
            await asyncio.sleep(self.config.source_poll_seconds)

    async def poll_source_wallet_once(self) -> None:
        trades = self._fetch_source_wallet_trades()
        if not trades:
            if not self.source_poll_bootstrapped:
                self.source_poll_bootstrapped = True
                self.source_poll_high_watermark = int(time.time())
                self.source_poll_start_ts = self.source_poll_high_watermark
                LOGGER.info(
                    "Source signal poll bootstrapped with empty history. New-only boundary ts=%s.",
                    self.source_poll_start_ts,
                )
            return

        parsed: List[tuple[int, str, Dict[str, Any]]] = []
        for trade in trades:
            timestamp = int(safe_float(trade.get("timestamp"), 0.0))
            key = self._make_poll_trade_key(trade)
            parsed.append((timestamp, key, trade))
        parsed.sort(key=lambda item: (item[0], item[1]))

        if not self.source_poll_bootstrapped:
            self.source_poll_bootstrapped = True
            max_ts = max((item[0] for item in parsed), default=int(time.time()))
            if self.config.source_poll_bootstrap_skip_history:
                self.source_poll_high_watermark = max_ts
                self.source_poll_start_ts = max(max_ts, int(time.time()))
                for _, key, _ in parsed:
                    self._remember_poll_trade_key(key)
                LOGGER.info(
                    "Source signal poll bootstrapped at ts=%s; skipped %s historical trades. New-only boundary ts=%s.",
                    max_ts,
                    len(parsed),
                    self.source_poll_start_ts,
                )
                return
            self.source_poll_high_watermark = max(0, max_ts - 1)
            self.source_poll_start_ts = 0
            LOGGER.info("Source signal poll bootstrapped at ts=%s; processing current batch.", self.source_poll_high_watermark)

        for timestamp, key, trade in parsed:
            if self.source_poll_start_ts and timestamp <= self.source_poll_start_ts:
                self.record_decision("skip_polled_prestart")
                continue
            if timestamp < self.source_poll_high_watermark:
                continue
            if key in self.source_poll_seen_key_set:
                self.record_decision("skip_polled_duplicate")
                continue
            self._remember_poll_trade_key(key)
            if timestamp > self.source_poll_high_watermark:
                self.source_poll_high_watermark = timestamp
            await self.handle_polled_trade(trade)

    async def handle_polled_trade(self, trade: Dict[str, Any]) -> None:
        owner = str(trade.get("proxyWallet") or trade.get("user") or "").lower()
        if owner and owner != self.config.source_wallet:
            self.record_decision("skip_owner_mismatch")
            return
        asset_id = str(trade.get("asset") or trade.get("asset_id") or "")
        side = str(trade.get("side") or "").upper()
        source_price = safe_float(trade.get("price"), 0.0)
        source_size = safe_float(trade.get("size"), safe_float(trade.get("amount"), 0.0))
        source_trade_id = str(trade.get("transactionHash") or trade.get("id") or trade.get("timestamp") or "")
        if not asset_id or side not in {"BUY", "SELL"} or source_price <= 0 or source_size <= 0:
            self.record_decision("skip_invalid_polled_trade")
            return
        self.ensure_asset_context_from_trade(asset_id, trade)
        await self.process_source_signal(
            asset_id=asset_id,
            side=side,
            source_price=source_price,
            source_size=source_size,
            source_trade_id=source_trade_id,
            source_owner=owner or self.config.source_wallet,
            signal_origin="data_api_poll",
        )

    def should_accept_signal(self, signal_key: str, asset_id: str) -> tuple[bool, str]:
        now = time.time()
        last_seen = self.seen_signal_timestamps.get(signal_key)
        if last_seen and now - last_seen < 60:
            return False, "duplicate_signal"
        cooldown_last = self.last_copied_timestamp_by_asset.get(asset_id)
        if cooldown_last and now - cooldown_last < self.config.source_signal_cooldown_seconds:
            return False, "asset_cooldown"
        self.seen_signal_timestamps[signal_key] = now
        stale_cutoff = now - 600
        old_keys = [key for key, ts in self.seen_signal_timestamps.items() if ts < stale_cutoff]
        for key in old_keys:
            self.seen_signal_timestamps.pop(key, None)
        return True, "ok"

    async def process_source_signal(
        self,
        asset_id: str,
        side: str,
        source_price: float,
        source_size: float,
        source_trade_id: str,
        source_owner: str,
        signal_origin: str,
    ) -> None:
        signal_key = f"{source_trade_id}:{source_owner}:{asset_id}:{side}:{round(source_price, 6)}:{round(source_size, 4)}"
        allowed, reason = self.should_accept_signal(signal_key, asset_id)
        if not allowed:
            self.record_decision(f"skip_signal_{reason}")
            return

        self.record_decision(f"signal_{signal_origin}")
        if side == "SELL" and self.config.close_on_source_sell:
            await self.close_positions_on_source_sell(asset_id, source_trade_id)

        if side == "SELL" and not self.config.copy_sell_trades:
            self.record_decision("skip_sell_copy_disabled")
            return
        await self.execute_copy_trade(asset_id, side, source_price, source_size, source_trade_id)

    async def handle_trade_event(self, event: Dict[str, Any]) -> None:
        taker_side = str(event.get("side") or "").upper()
        maker_side = "SELL" if taker_side == "BUY" else "BUY" if taker_side == "SELL" else ""
        trade_id = str(event.get("id") or event.get("trade_id") or event.get("timestamp") or "")
        maker_orders = event.get("maker_orders") or []
        if not maker_orders:
            self.record_decision("skip_ws_trade_without_maker_orders")
            return
        for maker in maker_orders:
            owner = str(maker.get("owner") or maker.get("maker_address") or "").lower()
            if owner != self.config.source_wallet:
                continue
            asset_id = str(maker.get("asset_id") or event.get("asset_id") or "")
            side = maker_side or str(maker.get("side") or "").upper()
            if not asset_id or side not in {"BUY", "SELL"}:
                self.record_decision("skip_invalid_ws_signal")
                continue
            source_price = safe_float(maker.get("price"), safe_float(event.get("price"), 0.0))
            source_size = safe_float(maker.get("matched_amount"), safe_float(event.get("size"), 0.0))
            if source_price <= 0 or source_size <= 0:
                self.record_decision("skip_invalid_ws_signal")
                continue
            await self.process_source_signal(
                asset_id=asset_id,
                side=side,
                source_price=source_price,
                source_size=source_size,
                source_trade_id=trade_id,
                source_owner=owner,
                signal_origin="market_ws_trade",
            )

    async def execute_copy_trade(self, asset_id: str, side: str, source_price: float, source_size: float, source_trade_id: str) -> bool:
        if len(self.state.list_open_positions()) >= self.config.max_open_positions:
            self.record_decision("skip_max_open_positions")
            return False
        if source_price < self.config.min_copy_price:
            self.record_decision("skip_source_price_below_min_copy_price")
            return False
        meta = self.market_meta_by_asset.get(asset_id)
        if not meta:
            self.record_decision("skip_asset_not_in_tradable_universe")
            return False
        book = self.books.get(asset_id)
        now = time.time()

        if self.config.pull_book_on_signal:
            fetched = self.fetch_book_snapshot_rest(asset_id)
            if isinstance(fetched, dict):
                book = fetched.get("book") if isinstance(fetched.get("book"), BookTop) else self.books.get(asset_id)
                self.record_decision("rest_book_on_signal")
        elif not book or now - book.ts > self.config.book_stale_seconds:
            fetched = self.fetch_book_snapshot_rest(asset_id)
            if isinstance(fetched, dict):
                book = fetched.get("book") if isinstance(fetched.get("book"), BookTop) else self.books.get(asset_id)
                self.record_decision("rest_book_refresh")
        if not book:
            self.record_decision("skip_missing_book")
            return False
        if time.time() - book.ts > self.config.book_stale_seconds:
            self.record_decision("skip_stale_book")
            return False
        if book.spread <= 0 or book.spread > self.config.max_market_spread:
            self.record_decision("skip_spread_filter")
            return False
        expected_price = book.ask if side == "BUY" else book.bid
        if expected_price <= 0:
            self.record_decision("skip_invalid_expected_price")
            return False
        if expected_price < self.config.min_copy_price:
            self.record_decision("skip_expected_price_below_min_copy_price")
            return False
        if abs(expected_price - source_price) > self.config.max_slippage_from_source:
            self.record_decision("skip_slippage_from_source")
            return False

        balance = self.state.raw_snapshot()["live_usdc_balance"]
        target_notional = min(self.config.max_trade_usdc, balance * self.config.copy_balance_fraction)
        source_notional = source_price * source_size
        if source_notional > 0:
            target_notional = min(target_notional, source_notional)
        if target_notional < self.config.min_trade_usdc:
            self.record_decision("skip_below_min_notional")
            return False
        size = round_size(target_notional / expected_price)
        if size <= 0:
            self.record_decision("skip_non_positive_size")
            return False
        if size < meta.order_min_size:
            required_notional = meta.order_min_size * expected_price
            max_affordable_notional = min(self.config.max_trade_usdc, balance)
            if required_notional <= max_affordable_notional and required_notional >= self.config.min_trade_usdc:
                size = round_size(meta.order_min_size)
                self.record_decision("upsize_to_market_min")
            else:
                self.record_decision("skip_below_market_min_size")
                return False

        if size <= 0:
            self.record_decision("skip_below_market_min_size")
            return False

        result = self.execution.place_fok_order(
            asset_id=asset_id,
            side=side,
            price=round_to_tick(expected_price, meta.tick_size),
            size=size,
            tick_size=meta.tick_size,
        )
        if not result.ok:
            reject_reason = ""
            if isinstance(result.raw, dict):
                reject_reason = str(result.raw.get("reject_reason") or "").strip().lower().replace(" ", "_")
            self.record_decision(f"reject_{reject_reason}" if reject_reason else "reject_execution")
            return False

        filled_entry_price = expected_price
        if isinstance(result.raw, dict):
            filled_entry_price = safe_float(result.raw.get("simulated_fill_price"), expected_price)

        position = Position(
            position_id=self.state.next_position_id(),
            asset_id=asset_id,
            market_id=meta.market_id,
            condition_id=meta.condition_id,
            outcome_index=meta.outcome_index,
            question=meta.question,
            outcome=meta.outcome,
            side=side,
            size=size,
            entry_price=filled_entry_price,
            entry_notional=size * filled_entry_price,
            source_trade_id=source_trade_id,
            source_wallet=self.config.source_wallet,
            opened_at=utc_now_iso(),
            peak_price=filled_entry_price,
            trough_price=filled_entry_price,
            soft_stop_armed=False,
            stop_loss_price=None,
        )
        self.state.add_open_position(position)
        self.last_copied_timestamp_by_asset[asset_id] = time.time()
        self.record_decision("copied")
        LOGGER.info(
            "Copied trade asset=%s side=%s size=%.4f expected=%.4f filled=%.4f latency_ms=%s",
            asset_id,
            side,
            size,
            expected_price,
            filled_entry_price,
            result.raw.get("simulated_latency_ms") if isinstance(result.raw, dict) else None,
        )
        return True


async def async_main() -> None:
    config = BotConfig.from_env()
    config.validate()

    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    LOGGER.info("Starting Polymarket copy trader. dry_run=%s source=%s", config.dry_run, config.source_wallet)
    bot = PolymarketCopyTrader(config)
    await bot.run()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
