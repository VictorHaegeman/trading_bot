"""
=============================================================
  AI TRADING BOT — Binance x Claude/Groq x TradingView x X
  Version 5.0 — Multi-Crypto — Claude AI — ROI-Optimized
=============================================================
  pip install python-binance groq anthropic requests python-dotenv flask

  .env requis (dans le meme dossier que bot.py) :
    BINANCE_API_KEY=...
    BINANCE_SECRET=...
    ANTHROPIC_API_KEY=...    <- Claude AI (prioritaire)
    GROQ_API_KEY=...         <- fallback si pas de cle Anthropic
    AI_MODEL=claude-haiku-4-5-20251001  <- optionnel (defaut: haiku)
    TESTNET=true             <- false pour le live
    TELEGRAM_TOKEN=...       <- optionnel
    TELEGRAM_CHAT_ID=...     <- optionnel
    TWITTER_BEARER_TOKEN=... <- optionnel
=============================================================
"""

import os
import sys
import json
import re
import time
import logging
import requests
import traceback
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from binance.client import Client
from binance.exceptions import BinanceAPIException
from groq import Groq
import anthropic
from flask import Flask, request, jsonify, send_from_directory
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed


# ─── HTTP SESSION AVEC RETRY/POOLING ─────────────────────────
def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=0.4, status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return s

http = _make_session()

# ─── FORCE UTF-8 ─────────────────────────────────────────────
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ─── CONFIG ──────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
env_path   = SCRIPT_DIR / ".env"
load_dotenv(dotenv_path=env_path)

TESTNET              = os.getenv("TESTNET", "true").lower() != "false"
BINANCE_API_KEY      = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_SECRET       = os.getenv("BINANCE_SECRET", "").strip()
BINANCE_FUTURES_KEY  = os.getenv("BINANCE_FUTURES_KEY", BINANCE_API_KEY).strip()
BINANCE_FUTURES_SEC  = os.getenv("BINANCE_FUTURES_SECRET", BINANCE_SECRET).strip()
GROQ_API_KEY         = os.getenv("GROQ_API_KEY", "").strip()
ANTHROPIC_API_KEY    = os.getenv("ANTHROPIC_API_KEY", "").strip()
AI_MODEL             = os.getenv("AI_MODEL", "claude-haiku-4-5-20251001").strip()
TELEGRAM_TOKEN       = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TWITTER_BEARER       = os.getenv("TWITTER_BEARER_TOKEN", "").strip()

# Ignore placeholder values
if TELEGRAM_TOKEN and ("COLLE" in TELEGRAM_TOKEN or len(TELEGRAM_TOKEN) < 20):
    TELEGRAM_TOKEN = ""
if TELEGRAM_CHAT_ID and "COLLE" in TELEGRAM_CHAT_ID:
    TELEGRAM_CHAT_ID = ""
if ANTHROPIC_API_KEY and ("COLLE" in ANTHROPIC_API_KEY or len(ANTHROPIC_API_KEY) < 20):
    ANTHROPIC_API_KEY = ""

# Parametres de risque
MAX_TRADE_PCT         = 0.05    # 5% = ~$50 par trade sur $1000
DAILY_LOSS_CAP        = 0.05    # Pause si -5% dans la journee
CAPITAL_LIMIT_USDT    = float(os.getenv("CAPITAL_LIMIT_USDT", "1000"))
MAX_DAILY_TRADES      = int(os.getenv("MAX_DAILY_TRADES", "20"))
MAX_LOSS_STREAK       = int(os.getenv("MAX_LOSS_STREAK", "4"))
MIN_CONFIDENCE        = float(os.getenv("MIN_CONFIDENCE", "0.60"))
MAX_OPEN_POSITIONS    = int(os.getenv("MAX_OPEN_POSITIONS", "8"))
ALL_SYMBOLS_MIN_VOL   = float(os.getenv("ALL_SYMBOLS_MIN_VOL", "50000"))
TOP_CANDIDATES        = int(os.getenv("TOP_CANDIDATES", "6"))
FUTURES_LEVERAGE      = int(os.getenv("FUTURES_LEVERAGE", "2"))     # levier futures (2x par defaut)
SCAN_INTERVAL         = 120   # 2 min — réduit l'usage tokens Groq (1.1M/jour gratuit)
TV_CACHE_SECS         = 300
CG_CACHE_SECS         = 600

# ─── LOGGING ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(str(SCRIPT_DIR / "bot.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# Verification des cles
for name, val in [("BINANCE_API_KEY", BINANCE_API_KEY), ("BINANCE_SECRET", BINANCE_SECRET)]:
    print(f"[CHECK] {name}: {'OK' if val else 'MANQUANTE'}")
    if not val:
        log.error(f"{name} manquante — verifier le .env")
        sys.exit(1)

if ANTHROPIC_API_KEY:
    print(f"[CHECK] AI: Claude ({AI_MODEL})")
elif GROQ_API_KEY:
    print(f"[CHECK] AI: Groq (llama-3.3-70b-versatile) — ajouter ANTHROPIC_API_KEY pour Claude")
else:
    log.error("ANTHROPIC_API_KEY ou GROQ_API_KEY requis — verifier le .env")
    sys.exit(1)

if TWITTER_BEARER:
    log.info("X/Twitter: cle detectee — sentiment active")
else:
    log.info("X/Twitter: pas de cle — sentiment desactive")

# ─── CLIENTS ─────────────────────────────────────────────────
if TESTNET:
    binance = Client(BINANCE_API_KEY, BINANCE_SECRET, testnet=True)
    log.info("MODE TESTNET spot — aucun vrai argent en jeu")
else:
    binance = Client(BINANCE_API_KEY, BINANCE_SECRET)
    log.info("MODE LIVE — attention argent reel")

# Client futures (testnet.binancefuture.com ou fapi.binance.com sur live)
# Sur testnet: cert SSL auto-signe → verify=False obligatoire
_ssl_params = {"verify": False} if TESTNET else {}
futures_binance = Client(BINANCE_FUTURES_KEY, BINANCE_FUTURES_SEC,
                         testnet=TESTNET, requests_params=_ssl_params)

# Test si les futures sont accessibles
_futures_enabled = False
try:
    futures_binance.futures_exchange_info()
    _futures_enabled = True
    log.info("Futures Binance: ACTIF (longs + shorts)")
except Exception as _fe:
    log.warning(f"Futures non disponibles ({_fe}) — LONG spot uniquement")
    log.warning("Pour activer les shorts: ajouter BINANCE_FUTURES_KEY et BINANCE_FUTURES_SECRET dans .env")
    log.warning("Cles disponibles sur testnet.binancefuture.com (testnet) ou Binance.com (live)")

# Sync horloge avec Binance (evite -1021 timestamp error)
try:
    server_ms = binance.get_server_time()["serverTime"]
    local_ms  = int(time.time() * 1000)
    binance.timestamp_offset          = server_ms - local_ms
    futures_binance.timestamp_offset  = binance.timestamp_offset
    log.info(f"Timestamp offset Binance: {binance.timestamp_offset}ms")
except Exception as e:
    log.warning(f"Impossible de syncer l'horloge Binance: {e}")

groq_client   = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None
_use_claude   = claude_client is not None

# Rotation des modèles Groq gratuits — limites séparées par modèle
GROQ_MODELS   = [
    "llama-3.3-70b-versatile",   # 100k tokens/jour (meilleur)
    "llama-3.1-8b-instant",      # 500k tokens/jour (fallback rapide)
    "gemma2-9b-it",              # 500k tokens/jour (fallback backup)
]
_groq_model_idx = 0  # index courant dans GROQ_MODELS

# ─── ETAT GLOBAL ─────────────────────────────────────────────
state = {
    "daily_pnl":           0.0,
    "daily_start_balance": None,
    "paused":              False,
    "paused_until":        0,       # timestamp de fin de pause temporaire
    "trades_today":        [],
    "last_signal":         None,
    "scanning_symbols":    [],
    "tv_signals":          {},
    "last_tv_update":      0,
    "current_scan":        "—",
    "trending":            [],
    "last_cg_update":      0,
    "loss_streak":         0,       # pertes consecutives
    "open_exits":          {},      # symbol -> {sl, tp, qty, entry, oco_id}
}

# ─── TELEGRAM ────────────────────────────────────────────────
def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        http.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=5)
    except Exception as e:
        log.error(f"Telegram error: {e}")

# ─── DONNEES BINANCE ─────────────────────────────────────────
_symbols_cache: dict = {"value": [], "ts": 0.0}

def get_all_symbols() -> list:
    """Toutes les paires USDT de Binance avec volume suffisant, sans tokens levier. Cache 10min."""
    now = time.time()
    if now - _symbols_cache["ts"] < 600 and _symbols_cache["value"]:
        return _symbols_cache["value"]
    try:
        excluded = ["DOWN", "UP", "BEAR", "BULL", "3L", "3S", "2L", "2S", "BUSD", "LP"]
        tickers  = binance.get_ticker()
        pairs    = [
            t for t in tickers
            if t["symbol"].endswith("USDT")
            and not any(x in t["symbol"] for x in excluded)
            and float(t.get("quoteVolume", 0)) > ALL_SYMBOLS_MIN_VOL
        ]
        pairs.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
        result = [t["symbol"] for t in pairs]
        _symbols_cache["value"] = result
        _symbols_cache["ts"]    = now
        log.info(f"Univers: {len(result)} paires USDT (vol>{'%.0fk' % (ALL_SYMBOLS_MIN_VOL/1000)}/24h)")
        return result
    except Exception as e:
        log.warning(f"get_all_symbols error: {e}")
        return _symbols_cache["value"] or ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]

def get_price(symbol: str) -> float:
    return float(binance.get_symbol_ticker(symbol=symbol)["price"])

def get_klines(symbol: str, interval: str = "1h", limit: int = 50) -> list:
    klines = binance.get_klines(symbol=symbol, interval=interval, limit=limit)
    return [
        {"open": float(k[1]), "high": float(k[2]), "low": float(k[3]),
         "close": float(k[4]), "volume": float(k[5])}
        for k in klines
    ]

def get_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, period + 1):
        diff = closes[-i] - closes[-i - 1]
        (gains if diff > 0 else losses).append(abs(diff))
    avg_gain = sum(gains) / period if gains else 0
    avg_loss = sum(losses) / period if losses else 0.001
    return round(100 - (100 / (1 + avg_gain / avg_loss)), 2)

# ─── CACHES ──────────────────────────────────────────────────
_fg_cache  = {"value": None, "ts": 0}
_bal_cache = {"value": 0.0,  "ts": 0}

def get_fear_greed() -> dict:
    now = time.time()
    if now - _fg_cache["ts"] < 300 and _fg_cache["value"]:
        return _fg_cache["value"]
    try:
        r = http.get("https://api.alternative.me/fng/", timeout=5)
        d = r.json()["data"][0]
        result = {"value": int(d["value"]), "label": d["value_classification"]}
        _fg_cache["value"] = result
        _fg_cache["ts"]    = now
        return result
    except:
        return _fg_cache["value"] or {"value": 50, "label": "Neutral"}

def get_balance() -> float:
    now     = time.time()
    max_age = 5 if len(state.get("open_exits", {})) > 0 else 30
    if now - _bal_cache["ts"] < max_age:
        return _bal_cache["value"]
    try:
        account = binance.get_account()
        for asset in account["balances"]:
            if asset["asset"] == "USDT":
                val = float(asset["free"])
                _bal_cache["value"] = val
                _bal_cache["ts"]    = now
                return val
    except Exception as e:
        log.warning(f"get_balance error: {e}")
    return _bal_cache["value"]

def get_open_positions(symbol: str = None) -> list:
    try:
        orders = binance.get_open_orders(symbol=symbol) if symbol else binance.get_open_orders()
        return [{"id": o["orderId"], "side": o["side"], "price": o["price"], "symbol": o["symbol"]} for o in orders]
    except:
        return []

_precision_cache: dict = {}  # {symbol: {step, tick}}

def _fetch_precision(symbol: str) -> dict:
    if symbol in _precision_cache:
        return _precision_cache[symbol]
    step, tick = 0.001, 0.01
    try:
        info = binance.get_symbol_info(symbol)
        for f in info.get("filters", []):
            if f["filterType"] == "LOT_SIZE":
                step = float(f["stepSize"])
            elif f["filterType"] == "PRICE_FILTER":
                tick = float(f["tickSize"])
    except Exception:
        pass
    _precision_cache[symbol] = {"step": step, "tick": tick}
    return _precision_cache[symbol]

def get_step_size(symbol: str) -> float:
    return _fetch_precision(symbol)["step"]

def round_qty(qty: float, step: float) -> float:
    if step <= 0:
        return qty
    precision = max(0, len(f"{step:.10f}".rstrip("0").split(".")[-1]))
    return round(round(qty / step) * step, precision)

def get_tick_size(symbol: str) -> float:
    return _fetch_precision(symbol)["tick"]

def round_price(price: float, tick: float) -> float:
    if tick <= 0:
        return price
    precision = max(0, len(f"{tick:.10f}".rstrip("0").split(".")[-1]))
    return round(round(price / tick) * tick, precision)

def get_atr(klines: list, period: int = 14) -> float:
    """Average True Range — mesure la volatilite pour le sizing."""
    if len(klines) < period + 1:
        return 0.0
    trs = []
    for i in range(1, period + 1):
        h = klines[-i]["high"]
        l = klines[-i]["low"]
        pc = klines[-i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / len(trs)

# ─── TRADINGVIEW SCREENER ─────────────────────────────────────
def _parse_tv_row(row: dict) -> tuple:
    """Extrait (symbol, signal_dict) d'une ligne de reponse TV."""
    sym = row["s"].replace("BINANCE:", "")
    d   = row["d"]
    rec = float(d[8]) if d[8] is not None else 0.0
    return sym, {
        "price":       d[1],
        "rsi":         d[2],
        "rsi_prev":    d[3],
        "ema20":       d[4],
        "ema50":       d[5],
        "macd":        d[6],
        "macd_signal": d[7],
        "rec_all":     rec,
        "rec_ma":      float(d[9]) if d[9] is not None else 0.0,
        "rec_other":   float(d[10]) if d[10] is not None else 0.0,
        "vol_ratio":   d[11],
        "change_pct":  d[12],
        "label": (
            "FORT ACHAT" if rec > 0.5  else
            "ACHAT"      if rec > 0.2  else
            "FORT VENTE" if rec < -0.5 else
            "VENTE"      if rec < -0.2 else
            "NEUTRE"
        ),
    }

_TV_COLUMNS = [
    "name", "close", "RSI", "RSI[1]",
    "EMA20", "EMA50", "MACD.macd", "MACD.signal",
    "Recommend.All", "Recommend.MA", "Recommend.Other",
    "relative_volume_10d_calc", "change",
]
_TV_HEADERS = {
    "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer":      "https://www.tradingview.com",
    "Origin":       "https://www.tradingview.com",
    "Content-Type": "application/json",
}

def get_tv_signals(symbols: list) -> dict:
    """Signaux TradingView pour une grande liste de symboles, par batches de 100."""
    url        = "https://scanner.tradingview.com/crypto/scan"
    BATCH      = 100
    all_res    = {}
    batches    = [symbols[i:i+BATCH] for i in range(0, len(symbols), BATCH)]

    for idx, batch in enumerate(batches):
        try:
            payload = {
                "symbols":  {"tickers": [f"BINANCE:{s}" for s in batch]},
                "columns":  _TV_COLUMNS,
            }
            r = http.post(url, json=payload, headers=_TV_HEADERS, timeout=20)
            for row in r.json().get("data", []):
                sym, sig = _parse_tv_row(row)
                all_res[sym] = sig
        except Exception as e:
            log.error(f"TV batch {idx+1}/{len(batches)} error: {e}")
        if idx < len(batches) - 1:
            time.sleep(0.25)

    log.info(f"TradingView: {len(all_res)}/{len(symbols)} signaux ({len(batches)} batches)")
    return all_res

# ─── X / TWITTER SENTIMENT ───────────────────────────────────
def get_x_sentiment(symbol: str) -> dict:
    if not TWITTER_BEARER:
        return {"available": False}
    base = symbol.replace("USDT", "").upper()
    query = f"${base} OR #{base.lower()} crypto -is:retweet lang:en"
    try:
        r = http.get(
            "https://api.twitter.com/2/tweets/search/recent",
            headers={"Authorization": f"Bearer {TWITTER_BEARER}"},
            params={"query": query, "max_results": 10, "tweet.fields": "public_metrics"},
            timeout=10,
        )
        data = r.json()
        if "errors" in data or r.status_code != 200:
            return {"available": False}
        tweets = data.get("data", [])
        if not tweets:
            return {"available": True, "score": 50, "count": 0, "label": "Neutral"}
        engagement = sum(
            t["public_metrics"]["like_count"] + t["public_metrics"]["retweet_count"]
            for t in tweets
        )
        score = min(100, max(0, 50 + (engagement / len(tweets)) * 0.3))
        return {
            "available": True,
            "score":     round(score, 1),
            "count":     len(tweets),
            "label":     "Bullish" if score > 60 else "Bearish" if score < 40 else "Neutral",
        }
    except Exception as e:
        log.warning(f"X sentiment error: {e}")
        return {"available": False}

# ─── FUTURES SIGNALS (PUBLICS, SANS AUTH) ────────────────────
_futures_cache: dict = {}

def get_futures_context(symbol: str) -> dict:
    """
    Recupere funding rate et open interest via l'API publique Binance Futures.
    Ces donnees sont disponibles meme depuis un compte spot/testnet.
    Funding rate > +0.05% = marche overleveraged long (bearish)
    Funding rate < -0.05% = marche overleveraged short (bullish)
    """
    now = time.time()
    cached = _futures_cache.get(symbol)
    if cached and now - cached["ts"] < 60:
        return cached["data"]

    result = {"funding_rate": None, "open_interest": None, "funding_label": "N/A"}
    try:
        r = http.get(
            "https://fapi.binance.com/fapi/v1/premiumIndex",
            params={"symbol": symbol},
            timeout=5,
        )
        if r.status_code == 200:
            d = r.json()
            fr = float(d.get("lastFundingRate", 0))
            result["funding_rate"] = round(fr * 100, 4)
            result["funding_label"] = (
                "OVERLEV LONG"  if fr >  0.0005 else
                "OVERLEV SHORT" if fr < -0.0005 else
                "NEUTRE"
            )
    except Exception as e:
        log.debug(f"funding rate {symbol}: {e}")

    try:
        r = http.get(
            "https://fapi.binance.com/fapi/v1/openInterest",
            params={"symbol": symbol},
            timeout=5,
        )
        if r.status_code == 200:
            result["open_interest"] = float(r.json().get("openInterest", 0))
    except Exception as e:
        log.debug(f"open interest {symbol}: {e}")

    _futures_cache[symbol] = {"data": result, "ts": now}
    return result

_ls_cache: dict = {}

def get_ls_ratio(symbol: str) -> dict:
    """
    Ratio longs/shorts des comptes via Binance Futures (public, sans auth).
    long_pct > 65% = marche surcharge en longs → signal contrarian BEARISH
    long_pct < 35% = marche surcharge en shorts → signal contrarian BULLISH
    """
    now = time.time()
    cached = _ls_cache.get(symbol)
    if cached and now - cached["ts"] < 300:
        return cached["data"]
    result = {"long_pct": None, "short_pct": None, "ls_label": "N/A"}
    try:
        r = http.get(
            "https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
            params={"symbol": symbol, "period": "1h", "limit": 1},
            timeout=5,
        )
        if r.status_code == 200 and r.json():
            d = r.json()[0]
            lp = float(d["longAccount"]) * 100
            result = {
                "long_pct":  round(lp, 1),
                "short_pct": round(100 - lp, 1),
                "ls_label": (
                    "SURCHARGE LONGS"  if lp > 65 else
                    "SURCHARGE SHORTS" if lp < 35 else
                    "EQUILIBRE"
                ),
            }
    except Exception as e:
        log.debug(f"ls_ratio {symbol}: {e}")
    _ls_cache[symbol] = {"data": result, "ts": now}
    return result

_htf_cache: dict = {}

def get_htf_bias(symbol: str) -> dict:
    """
    Biais directionnel 4h (EMA20/50 + RSI).
    Confirmation multi-timeframe critique pour filtrer les faux signaux 1h.
    """
    now = time.time()
    cached = _htf_cache.get(symbol)
    if cached and now - cached["ts"] < 240:
        return cached["data"]
    result = {"bias_4h": "N/A", "rsi_4h": None, "ema20_4h": None}
    try:
        klines = get_klines(symbol, "4h", 50)
        closes = [k["close"] for k in klines]
        rsi_4h  = get_rsi(closes)
        ema20_4h = sum(closes[-20:]) / 20
        ema50_4h = sum(closes[-50:]) / min(50, len(closes))
        price    = closes[-1]
        if price > ema20_4h > ema50_4h and rsi_4h > 50:
            bias = "BULLISH"
        elif price < ema20_4h < ema50_4h and rsi_4h < 50:
            bias = "BEARISH"
        else:
            bias = "NEUTRE"
        result = {"bias_4h": bias, "rsi_4h": round(rsi_4h, 1), "ema20_4h": round(ema20_4h, 6)}
    except Exception as e:
        log.debug(f"htf_bias {symbol}: {e}")
    _htf_cache[symbol] = {"data": result, "ts": now}
    return result

# ─── COINGECKO TRENDING ───────────────────────────────────────
def get_trending_coins() -> list:
    """Coins trending sur CoinGecko (cache 10min)."""
    now = time.time()
    if now - state["last_cg_update"] < CG_CACHE_SECS:
        return state["trending"]
    try:
        r = http.get("https://api.coingecko.com/api/v3/search/trending", timeout=8)
        coins = [c["item"]["symbol"].upper() + "USDT" for c in r.json().get("coins", [])]
        state["trending"] = coins
        state["last_cg_update"] = now
        return coins
    except:
        return state.get("trending", [])

# ─── ORDERBOOK IMBALANCE ─────────────────────────────────────
def get_orderbook_imbalance(symbol: str, levels: int = 20) -> dict:
    """Ratio bids vs asks (pression achat immédiate)."""
    try:
        ob   = binance.get_order_book(symbol=symbol, limit=levels)
        bids = sum(float(b[1]) for b in ob["bids"][:levels])
        asks = sum(float(a[1]) for a in ob["asks"][:levels])
        total = bids + asks
        imb   = (bids - asks) / total if total > 0 else 0
        label = ("FORT BUY" if imb > 0.20 else "BUY" if imb > 0.07
                 else "FORT SELL" if imb < -0.20 else "SELL" if imb < -0.07
                 else "NEUTRE")
        return {"imbalance": round(imb, 3), "label": label}
    except Exception:
        return {"imbalance": 0.0, "label": "N/A"}


# ─── CONTEXTE MARCHE ─────────────────────────────────────────
def get_market_context(symbol: str) -> dict:
    """Agrege toutes les donnees de marche pour une paire."""
    klines  = get_klines(symbol, interval="1h", limit=50)
    closes  = [k["close"] for k in klines]
    price   = get_price(symbol)
    rsi     = get_rsi(closes)
    atr     = get_atr(klines)
    fg      = get_fear_greed()
    balance = min(get_balance(), CAPITAL_LIMIT_USDT)
    ema20   = sum(closes[-20:]) / 20
    ema50   = sum(closes[-50:]) / min(50, len(closes))

    klines_15m = get_klines(symbol, interval="15m", limit=20)
    vols       = [k["volume"] for k in klines_15m]
    vol_ratio  = round(vols[-1] / (sum(vols) / len(vols)), 2) if vols else 1.0

    futures = get_futures_context(symbol)
    ls      = get_ls_ratio(symbol)
    htf     = get_htf_bias(symbol)
    ob      = get_orderbook_imbalance(symbol)

    risk_amount    = balance * 0.015
    atr_sl_dist    = 1.5 * atr / price if price > 0 else 0.02
    atr_based_size = round(risk_amount / atr_sl_dist, 2) if atr_sl_dist > 0 else balance * MAX_TRADE_PCT
    max_size       = min(balance * MAX_TRADE_PCT, atr_based_size)

    return {
        "symbol":           symbol,
        "price":            price,
        "rsi_14":           rsi,
        "atr_1h":           round(atr, 6),
        "atr_pct":          round(atr / price * 100, 3) if price else 0,
        "ema20":            round(ema20, 6),
        "ema50":            round(ema50, 6),
        "price_vs_ema20":   "above" if price > ema20 else "below",
        "price_vs_ema50":   "above" if price > ema50 else "below",
        "volume_ratio":     vol_ratio,
        "fear_greed_value": fg["value"],
        "fear_greed_label": fg["label"],
        "balance_usdt":     round(balance, 2),
        "max_size_usdt":    round(max_size, 2),
        "daily_pnl_pct":    round(state["daily_pnl"], 4),
        "open_positions":   get_open_positions(symbol),
        "funding_rate":     futures["funding_rate"],
        "funding_label":    futures["funding_label"],
        "open_interest":    futures["open_interest"],
        "long_pct":         ls["long_pct"],
        "ls_label":         ls["ls_label"],
        "bias_4h":          htf["bias_4h"],
        "rsi_4h":           htf["rsi_4h"],
        "ob_imbalance":     ob["imbalance"],
        "ob_label":         ob["label"],
    }

# ─── DECISION IA MULTI-CRYPTO ─────────────────────────────────
def _parse_ai_json(text: str) -> dict:
    """Extrait un objet JSON valide même si le modèle ajoute du texte autour."""
    text = text.strip()
    # Retire les blocs markdown ```json ... ```
    text = re.sub(r"```(?:json)?\s*", "", text).replace("```", "").strip()
    # Tente direct
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Cherche le premier {...} complet avec comptage des accolades
    depth, start = 0, -1
    for i, c in enumerate(text):
        if c == "{":
            if depth == 0:
                start = i
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0 and start != -1:
                try:
                    return json.loads(text[start:i+1])
                except json.JSONDecodeError:
                    pass
                start = -1
    raise json.JSONDecodeError("Aucun JSON trouvé", text, 0)


def ask_ai_multi(contexts: list, perf: dict = None) -> dict:
    """Passe plusieurs paires candidates a Claude (Haiku) ou Groq (fallback) + historique."""
    balance   = contexts[0]["balance_usdt"]
    max_trade = round(balance * MAX_TRADE_PCT, 2)
    fg        = contexts[0]["fear_greed_value"]
    trending  = get_trending_coins()
    perf      = perf or {"n": 0, "summary": "Aucun historique"}

    cands_text = ""
    for ctx in contexts:
        tv      = ctx.get("tv_signals", {})
        rec     = tv.get("rec_all", ctx.get("tv_recommendation", 0)) or 0
        x       = ctx.get("x_sentiment", {})
        x_str   = f"\n  X/Twitter: {x['label']} ({x['score']:.0f}/100)" if x.get("available") else ""
        tr_flag = " [TRENDING]" if ctx["symbol"] in trending else ""
        fr      = ctx.get("funding_rate")
        lp      = ctx.get("long_pct")
        b4h     = ctx.get("bias_4h", "N/A")
        r4h     = ctx.get("rsi_4h")
        atr_p   = ctx.get("atr_pct", 0)
        max_sz  = ctx.get("max_size_usdt", max_trade)
        cands_text += f"""
▸ {ctx['symbol']}{tr_flag}
  Prix: ${ctx['price']:,.6g} | RSI-1h: {ctx['rsi_14']} | RSI-4h: {r4h} | Biais 4h: {b4h}
  EMA20: ${ctx['ema20']:,.6g} ({ctx['price_vs_ema20']}) | EMA50: ${ctx['ema50']:,.6g} ({ctx['price_vs_ema50']})
  Vol ratio: {ctx['volume_ratio']}x | ATR: {atr_p:.2f}% du prix | Taille ATR-optimale: ${max_sz}
  TradingView: {tv.get('label','N/A')} (rec {rec:+.2f}) | MACD: {tv.get('macd',0) or 0:.4g} vs {tv.get('macd_signal',0) or 0:.4g}
  Funding: {f"{fr:+.4f}%" if fr is not None else "N/A"} ({ctx.get('funding_label','N/A')}) | L/S: {f"{lp:.0f}% longs" if lp else "N/A"} ({ctx.get('ls_label','N/A')})
  Orderbook: imbalance={ctx.get('ob_imbalance',0):+.3f} ({ctx.get('ob_label','N/A')})
  Positions ouvertes: {len(ctx['open_positions'])}{x_str}
"""

    short_note = (
        f"SELL (SHORT Futures x{FUTURES_LEVERAGE}) disponible"
        if _futures_enabled else
        "SHORT non disponible — BUY uniquement"
    )

    prompt = f"""Tu es un trader algorithmique expert qui prend des decisions rapides et rentables ({datetime.now().strftime('%H:%M UTC')}).

PORTEFEUILLE:
- Capital: ${balance} | Fear&Greed: {fg}/100 ({contexts[0]['fear_greed_label']})
- Trending: {', '.join(trending[:5]) if trending else 'N/A'}
- Positions ouvertes: {len(state['open_exits'])}/{MAX_OPEN_POSITIONS} | Pertes consecutives: {state['loss_streak']}

HISTORIQUE ({perf['n']} trades): {perf['summary']}

PAIRES CANDIDATES:
{cands_text}

REGLES ESSENTIELLES:
- Confidence min {MIN_CONFIDENCE} | R:R min 1.5 | SL max 3.5%
- BUY: prefere RSI < 75, biais 4h non BEARISH
- SELL/SHORT: prefere RSI > 25, biais 4h non BULLISH | {short_note}
- SHORT: stop_loss SUPERIEUR a entry, take_profit INFERIEUR a entry
- Apprends de l'historique: si beaucoup de pertes recentes, sois plus selectif; si bon win_rate, trade plus librement
- HOLD seulement si vraiment rien de convaincant — prends les opportunites

ACTIONS: BUY (long spot), SELL (short futures), HOLD

JSON uniquement:
{{"symbol":"SOLUSDT","action":"BUY","size_usdt":100.00,"entry_price":185.50,"stop_loss":181.00,"take_profit":195.00,"confidence":0.72,"reasoning":"..."}}
SHORT ex: {{"symbol":"BTCUSDT","action":"SELL","size_usdt":100.00,"entry_price":105000,"stop_loss":107500,"take_profit":100000,"confidence":0.68,"reasoning":"..."}}
HOLD: {{"symbol":"NONE","action":"HOLD","size_usdt":null,"entry_price":null,"stop_loss":null,"take_profit":null,"confidence":0.0,"reasoning":"..."}}"""

    sys_msg = "Expert trading algorithmique. Reponds UNIQUEMENT avec un objet JSON valide, sans texte avant ou apres."
    raw = ""

    def _call_groq(model: str) -> str:
        resp = groq_client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": sys_msg},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.15,
            max_tokens=350,
        )
        return resp.choices[0].message.content.strip()

    try:
        if _use_claude:
            resp = claude_client.messages.create(
                model=AI_MODEL,
                max_tokens=400,
                temperature=0.15,
                system=sys_msg,
                messages=[{"role": "user", "content": prompt}],
            )
            raw      = resp.content[0].text.strip()
            provider = f"Claude({AI_MODEL.split('-')[1]})"
        else:
            # Rotation automatique sur les modèles Groq gratuits
            global _groq_model_idx
            tried = set()
            last_err = None
            while len(tried) < len(GROQ_MODELS):
                model = GROQ_MODELS[_groq_model_idx % len(GROQ_MODELS)]
                if model in tried:
                    _groq_model_idx += 1
                    continue
                tried.add(model)
                try:
                    raw      = _call_groq(model)
                    provider = f"Groq({model.split('-')[0]}{'70b' if '70b' in model else '8b' if '8b' in model else 'gemma'})"
                    last_err = None
                    break
                except Exception as me:
                    if "429" in str(me) or "rate_limit" in str(me).lower():
                        log.warning(f"Rate limit {model} → rotation vers prochain modèle")
                        _groq_model_idx += 1
                        last_err = me
                    else:
                        raise
            if last_err:
                raise last_err

        decision = _parse_ai_json(raw)
        log.info(f"[{provider}] → {decision.get('symbol','?')} {decision['action']} conf={decision.get('confidence','?')} | {decision.get('reasoning','')[:120]}")
        return decision
    except json.JSONDecodeError:
        log.error(f"JSON invalide: {raw[:200]}")
        return {"symbol": "NONE", "action": "HOLD", "confidence": 0, "reasoning": "JSON invalide"}
    except Exception as e:
        log.error(f"AI error: {e}")
        return {"symbol": "NONE", "action": "HOLD", "confidence": 0, "reasoning": str(e)}

# ─── RISK GATE ────────────────────────────────────────────────
def risk_gate(decision: dict, context: dict, min_conf: float = None) -> tuple:
    action = decision.get("action", "HOLD")
    if action == "HOLD":
        return True, "OK"
    if action not in ("BUY", "SELL"):
        return False, f"Action inconnue: {action}"

    # SELL/SHORT requiert futures actives
    if action == "SELL" and not _futures_enabled:
        return False, "SHORT refuse: futures non actives (configurer BINANCE_FUTURES_KEY)"

    # Pause
    if state["paused"] and time.time() < state.get("paused_until", 0):
        remaining = int((state["paused_until"] - time.time()) / 60)
        return False, f"Bot en pause ({remaining} min restantes)"
    elif state["paused"]:
        state["paused"] = False

    # Max trades quotidiens
    if len(state["trades_today"]) >= MAX_DAILY_TRADES:
        return False, f"Max trades atteint ({MAX_DAILY_TRADES}/jour)"

    # Serie de pertes
    if state["loss_streak"] >= MAX_LOSS_STREAK:
        return False, f"Serie de pertes ({state['loss_streak']}x)"

    # Confidence (adaptative si fournie)
    eff_conf = min_conf if min_conf is not None else MIN_CONFIDENCE
    conf     = decision.get("confidence", 0)
    if conf < eff_conf:
        return False, f"Confidence {conf:.2f} < {eff_conf:.2f}"

    # Stop-loss obligatoire
    if not decision.get("stop_loss"):
        return False, "Stop-loss manquant"

    entry = decision.get("entry_price", 0)
    sl    = decision.get("stop_loss", 0)
    tp    = decision.get("take_profit", 0)

    # Taille adaptative — reduit si serie de pertes
    streak   = state.get("loss_streak", 0)
    streak_factor = 0.5 if streak >= 3 else 0.7 if streak >= 2 else 1.0
    size     = decision.get("size_usdt") or 0
    max_size = (context.get("max_size_usdt") or (context["balance_usdt"] * MAX_TRADE_PCT)) * streak_factor
    if size > max_size * 1.05:
        decision["size_usdt"] = round(max_size, 2)
    if decision["size_usdt"] < 10:
        return False, f"Taille trop petite (${decision['size_usdt']:.2f})"
    if decision["size_usdt"] > context["balance_usdt"]:
        return False, f"Balance insuffisante (${context['balance_usdt']:.2f})"
    if streak_factor < 1.0:
        log.info(f"Taille reduite ×{streak_factor} (streak {streak} pertes)")

    # RSI extremes (assoupli : 80 au lieu de 72)
    rsi = context.get("rsi_14", 50)
    if action == "BUY"  and rsi > 80:
        return False, f"RSI trop surachete ({rsi} > 80)"
    if action == "SELL" and rsi < 20:
        return False, f"RSI trop survendu ({rsi} < 20) pour shorter"

    # Volume minimal (0.05x — testnet a peu de volume)
    vol_ratio = context.get("volume_ratio", 1.0)
    if vol_ratio < 0.05:
        return False, f"Volume trop faible ({vol_ratio}x < 0.05x)"

    # R:R minimum 1.5 — auto-corriger le TP plutôt que rejeter
    if entry and sl and tp:
        risk   = abs(entry - sl)
        reward = abs(tp - entry)
        if risk > 0 and (reward / risk) < 1.5:
            # L'AI a bien identifié la direction et le SL — on force le TP à 1.5×risk
            new_tp = (entry + risk * 1.5) if action == "BUY" else (entry - risk * 1.5)
            log.info(f"Risk Gate: TP auto-corrigé {tp:.6g} → {new_tp:.6g} (R:R forcé 1.5)")
            decision["take_profit"] = new_tp

    # SL max 2.5% — auto-corriger si trop loin plutôt que rejeter
    if entry and sl:
        sl_pct = abs(entry - sl) / entry * 100
        if sl_pct > 2.5:
            new_sl = (entry * 0.975) if action == "BUY" else (entry * 1.025)
            log.info(f"Risk Gate: SL auto-corrigé {sl:.6g} → {new_sl:.6g} ({sl_pct:.1f}% → 2.5%)")
            decision["stop_loss"] = new_sl
            # Recalculer TP pour maintenir R:R 1.5
            risk   = abs(entry - new_sl)
            new_tp = (entry + risk * 1.5) if action == "BUY" else (entry - risk * 1.5)
            decision["take_profit"] = new_tp
            log.info(f"Risk Gate: TP recalculé → {new_tp:.6g} (R:R 1.5)")

    return True, "OK"

# ─── OCO EXIT ORDERS ──────────────────────────────────────────
def place_exit_orders(symbol: str, qty: float, stop_loss: float,
                      take_profit: float, entry_price: float = 0.0):
    """Place un ordre OCO (SL + TP simultanes) apres un achat spot."""
    try:
        tick     = get_tick_size(symbol)
        sl       = round_price(stop_loss, tick)
        tp       = round_price(take_profit, tick)
        sl_limit = round_price(sl * 0.9985, tick)  # 0.15% sous le stop

        # Nouveau format OCO Binance (api/v3/orderList/oco)
        oco = binance._post("orderList/oco", True, data={
            "symbol":            symbol,
            "side":              "SELL",
            "quantity":          str(qty),
            "aboveType":         "LIMIT_MAKER",
            "abovePrice":        str(tp),
            "belowType":         "STOP_LOSS_LIMIT",
            "belowStopPrice":    str(sl),
            "belowPrice":        str(sl_limit),
            "belowTimeInForce":  "GTC",
        })
        oco_id = oco.get("orderListId", "?")
        log.info(f"OCO place: {symbol} TP=${tp} SL=${sl} (list #{oco_id})")
        state["open_exits"][symbol] = {
            "oco_id":      oco_id,
            "sl":          sl,
            "tp":          tp,
            "qty":         qty,
            "entry_price": entry_price,
        }
        return oco_id
    except BinanceAPIException as e:
        log.error(f"Erreur OCO {symbol}: {e.message}")
        return None


def _close_position(sym: str, info: dict, fill_px: float, is_short: bool = False):
    """Traite la cloture d'une position — PnL, streak, log, trades.json."""
    entry_px = info.get("entry_price", fill_px)
    sl_px    = info.get("sl", 0)
    # Pour un SHORT, le SL est au-dessus de l'entree
    if is_short:
        pnl = (entry_px - fill_px) / entry_px * 100 if entry_px else 0
        is_sl = fill_px >= sl_px * 0.998
    else:
        pnl = (fill_px - entry_px) / entry_px * 100 if entry_px else 0
        is_sl = fill_px <= sl_px * 1.002

    outcome = "LOSS" if is_sl else "WIN"
    if is_sl:
        state["loss_streak"] = state.get("loss_streak", 0) + 1
        log.warning(f"SL {sym} @ ${fill_px:.4g} | PnL {pnl:+.2f}% | streak {state['loss_streak']}")
        send_telegram(f"{'SHORT' if is_short else 'LONG'} SL *{sym}* `{pnl:+.2f}%` streak:{state['loss_streak']}")
    else:
        state["loss_streak"] = 0
        log.info(f"TP {sym} @ ${fill_px:.4g} | PnL {pnl:+.2f}%")
        send_telegram(f"{'SHORT' if is_short else 'LONG'} TP *{sym}* `{pnl:+.2f}%` WIN")

    update_trade_outcome(sym, info.get("oco_id"), pnl / 100, outcome)


def sync_open_positions():
    """Retire de open_exits les OCO/futures clos et met a jour loss_streak + trades.json."""
    to_remove = []
    for sym, info in list(state["open_exits"].items()):
        oco_id   = info.get("oco_id")
        is_short = info.get("is_short", False)

        # ── Futures SHORT ──
        if is_short and _futures_enabled:
            try:
                pos = futures_binance.futures_position_information(symbol=sym)
                amt = float(pos[0].get("positionAmt", 0)) if pos else 0
                if abs(amt) < 1e-9:  # position fermee
                    # Lire le dernier trade futures pour le PnL
                    try:
                        trades_f = futures_binance.futures_account_trades(symbol=sym, limit=5)
                        if trades_f:
                            last = trades_f[-1]
                            fill_px = float(last.get("price", info.get("entry_price", 0)))
                            _close_position(sym, info, fill_px, is_short=True)
                    except Exception:
                        pass
                    to_remove.append(sym)
            except Exception as e:
                log.warning(f"sync futures {sym}: {e}")
            continue

        # ── Spot LONG (OCO) ──
        if not oco_id:
            continue
        try:
            oco    = binance._get("orderList", True, data={"orderListId": int(oco_id)})
            status = oco.get("listOrderStatus", "")
            if status not in ("ALL_DONE", "RESPONSE"):
                continue
            for ord_ref in oco.get("orders", []):
                try:
                    detail = binance.get_order(symbol=sym, orderId=ord_ref["orderId"])
                    if detail.get("status") != "FILLED":
                        continue
                    fill_px = float(detail.get("price") or detail.get("avgPrice") or 0)
                    if fill_px:
                        _close_position(sym, info, fill_px, is_short=False)
                    break
                except Exception:
                    pass
            to_remove.append(sym)
        except Exception as e:
            log.warning(f"sync OCO {sym}: {e}")

    for sym in to_remove:
        del state["open_exits"][sym]
    if to_remove:
        log.info(f"Positions fermees: {', '.join(to_remove)}")


# ─── SHORT FUTURES ────────────────────────────────────────────
def execute_short(decision: dict, context: dict):
    """Ouvre un SHORT via Binance Futures USD-M avec levier et SL/TP."""
    if not _futures_enabled:
        log.warning("SHORT ignore: futures non actives (ajouter BINANCE_FUTURES_KEY dans .env)")
        return

    symbol = decision.get("symbol") or context["symbol"]
    price  = context["price"]
    size   = decision["size_usdt"]
    sl     = decision.get("stop_loss")   # > entry pour un short
    tp     = decision.get("take_profit") # < entry pour un short

    step = get_step_size(symbol)
    qty  = round_qty((size * FUTURES_LEVERAGE) / price, step)

    try:
        # Levier
        try:
            futures_binance.futures_change_leverage(symbol=symbol, leverage=FUTURES_LEVERAGE)
        except Exception:
            pass

        # Ordre short (SELL sur futures = ouvrir une position short)
        order = futures_binance.futures_create_order(
            symbol=symbol, side="SELL", type="MARKET", quantity=qty
        )
        fill_price = float(order.get("avgPrice") or price)
        log.info(f"SHORT EXECUTE: {qty} {symbol} @ ~${fill_price:,.6g} x{FUTURES_LEVERAGE} (#{order.get('orderId')})")

        # Stop-Loss (ABOVE entry for short)
        if sl:
            try:
                futures_binance.futures_create_order(
                    symbol=symbol, side="BUY", type="STOP_MARKET",
                    stopPrice=str(round_price(sl, get_tick_size(symbol))),
                    closePosition=True, timeInForce="GTE_GTC",
                )
            except Exception as e:
                log.warning(f"Futures SL order error {symbol}: {e}")

        # Take-Profit (BELOW entry for short)
        if tp:
            try:
                futures_binance.futures_create_order(
                    symbol=symbol, side="BUY", type="TAKE_PROFIT_MARKET",
                    stopPrice=str(round_price(tp, get_tick_size(symbol))),
                    closePosition=True, timeInForce="GTE_GTC",
                )
            except Exception as e:
                log.warning(f"Futures TP order error {symbol}: {e}")

        trade_log = {
            "timestamp":   datetime.now().isoformat(),
            "symbol":      symbol,
            "action":      "SELL",
            "direction":   "SHORT",
            "qty":         qty,
            "leverage":    FUTURES_LEVERAGE,
            "entry_price": fill_price,
            "stop_loss":   sl,
            "take_profit": tp,
            "size_usdt":   size,
            "reasoning":   decision.get("reasoning"),
            "confidence":  decision.get("confidence"),
            "order_id":    order.get("orderId"),
        }
        state["trades_today"].append(trade_log)
        save_trade_log(trade_log)
        state["open_exits"][symbol] = {
            "oco_id":      None,
            "sl":          sl,
            "tp":          tp,
            "qty":         qty,
            "entry_price": fill_price,
            "is_short":    True,
        }

        sl_pct = abs(sl - fill_price) / fill_price * 100 if sl else 0
        tp_pct = abs(fill_price - tp) / fill_price * 100 if tp else 0
        send_telegram(
            f"SHORT *{symbol}* x{FUTURES_LEVERAGE}\n"
            f"Entry: `${fill_price:,.6g}` | Taille: `${size}` USDT\n"
            f"SL: `+{sl_pct:.1f}%` (${sl:,.6g}) | TP: `-{tp_pct:.1f}%` (${tp:,.6g})\n"
            f"Conf: `{decision['confidence']}` | _{decision.get('reasoning','-')}_"
        )
    except BinanceAPIException as e:
        log.error(f"Futures error execute_short: {e}")
        send_telegram(f"Erreur SHORT {symbol}: {e.message}")


# ─── EXECUTION ────────────────────────────────────────────────
def execute_trade(decision: dict, context: dict):
    action = decision["action"]
    if action == "SELL":
        execute_short(decision, context)
        return
    if action != "BUY":
        return

    symbol = decision.get("symbol") or context["symbol"]
    price  = context["price"]
    size   = decision["size_usdt"]
    sl     = decision.get("stop_loss")
    tp     = decision.get("take_profit")

    step = get_step_size(symbol)
    qty  = round_qty(size / price, step)

    try:
        order = binance.order_market(symbol=symbol, side="BUY", quantity=qty)
        fill_price = float(order.get("fills", [{}])[0].get("price", price)) if order.get("fills") else price
        log.info(f"ACHAT EXECUTE: {qty} {symbol} @ ~${fill_price:,.6g} (ordre #{order['orderId']})")

        trade_log = {
            "timestamp":   datetime.now().isoformat(),
            "symbol":      symbol,
            "action":      "BUY",
            "qty":         qty,
            "entry_price": fill_price,
            "stop_loss":   sl,
            "take_profit": tp,
            "size_usdt":   size,
            "reasoning":   decision.get("reasoning"),
            "confidence":  decision.get("confidence"),
            "order_id":    order["orderId"],
        }
        state["trades_today"].append(trade_log)
        save_trade_log(trade_log)

        # Marquer la position comme ouverte IMMEDIATEMENT (avant OCO)
        # pour eviter que le prochain cycle rachete le meme symbole
        state["open_exits"][symbol] = {
            "oco_id":      None,
            "sl":          sl,
            "tp":          tp,
            "qty":         qty,
            "entry_price": fill_price,
            "is_short":    False,
        }

        # Placement OCO si SL + TP definis
        if sl and tp:
            oco_id = place_exit_orders(symbol, qty, sl, tp, entry_price=fill_price)
            if oco_id:
                state["open_exits"][symbol]["oco_id"] = oco_id
                trade_log["oco_id"] = oco_id
                save_trade_log(trade_log)

        sl_pct = abs(fill_price - sl) / fill_price * 100 if sl else 0
        tp_pct = abs(tp - fill_price) / fill_price * 100 if tp else 0
        send_telegram(
            f"BUY *{symbol}*\n"
            f"Prix entree: `${fill_price:,.6g}` | Taille: `${size}` USDT\n"
            f"SL: `-{sl_pct:.1f}%` (${sl:,.6g}) | TP: `+{tp_pct:.1f}%` (${tp:,.6g})\n"
            f"Conf: `{decision['confidence']}` | _{decision.get('reasoning','-')}_"
        )
    except BinanceAPIException as e:
        log.error(f"Binance error execute_trade: {e}")
        send_telegram(f"Erreur ordre {symbol}: {e.message}")

def save_trade_log(trade: dict):
    try:
        log_file = SCRIPT_DIR / "trades.json"
        logs = []
        if log_file.exists():
            with open(log_file) as f:
                logs = json.load(f)
        # Update existant si meme order_id, sinon append
        oid = trade.get("order_id")
        updated = False
        if oid:
            for i, t in enumerate(logs):
                if t.get("order_id") == oid:
                    logs[i] = {**t, **trade}
                    updated = True
                    break
        if not updated:
            logs.append(trade)
        with open(log_file, "w") as f:
            json.dump(logs, f, indent=2)
    except Exception as e:
        log.error(f"Erreur save trade log: {e}")


def update_trade_outcome(symbol: str, order_id, pnl_pct: float, outcome: str):
    """Ecrit le resultat (WIN/LOSS + PnL%) dans trades.json pour apprentissage."""
    try:
        log_file = SCRIPT_DIR / "trades.json"
        if not log_file.exists():
            return
        with open(log_file) as f:
            logs = json.load(f)
        for t in reversed(logs):
            if t.get("symbol") == symbol and t.get("oco_id") is not None:
                t["pnl_pct"]  = round(pnl_pct, 4)
                t["outcome"]  = outcome
                t["closed_at"] = datetime.now().isoformat()
                break
        with open(log_file, "w") as f:
            json.dump(logs, f, indent=2)
    except Exception as e:
        log.warning(f"update_trade_outcome error: {e}")


def get_perf_stats(n: int = 20) -> dict:
    """Stats des n derniers trades clotures depuis trades.json (apprentissage)."""
    try:
        log_file = SCRIPT_DIR / "trades.json"
        if not log_file.exists():
            return {"n": 0, "win_rate": 0.5, "avg_pnl": 0.0, "summary": "Aucun historique"}
        with open(log_file) as f:
            trades = json.load(f)
        closed = [t for t in trades if t.get("outcome") in ("WIN", "LOSS")][-n:]
        if not closed:
            return {"n": 0, "win_rate": 0.5, "avg_pnl": 0.0, "summary": "Aucun trade cloture"}
        wins     = [t for t in closed if t.get("outcome") == "WIN"]
        losses   = [t for t in closed if t.get("outcome") == "LOSS"]
        win_rate = len(wins) / len(closed)
        avg_pnl  = sum(t.get("pnl_pct", 0) for t in closed) / len(closed)
        # Meilleurs/pires symboles
        sym_pnl = {}
        for t in closed:
            s = t.get("symbol", "?")
            sym_pnl.setdefault(s, []).append(t.get("pnl_pct", 0))
        sym_avg = {s: sum(v) / len(v) for s, v in sym_pnl.items()}
        best_sym  = max(sym_avg, key=sym_avg.get, default="?")
        worst_sym = min(sym_avg, key=sym_avg.get, default="?")
        # Resume des 5 derniers
        recent_str = " | ".join(
            f"{'WIN' if t.get('outcome')=='WIN' else 'LOSS'} {t.get('action','?')} "
            f"{t.get('symbol','?')} {t.get('pnl_pct',0):+.1f}%"
            for t in closed[-5:]
        )
        return {
            "n":         len(closed),
            "win_rate":  win_rate,
            "avg_pnl":   avg_pnl,
            "best_sym":  best_sym,
            "worst_sym": worst_sym,
            "summary":   (f"{len(wins)}W/{len(losses)}L | "
                          f"win_rate {win_rate*100:.0f}% | "
                          f"avg_pnl {avg_pnl*100:+.2f}% | "
                          f"derniers: {recent_str}"),
        }
    except Exception as e:
        log.warning(f"get_perf_stats error: {e}")
        return {"n": 0, "win_rate": 0.5, "avg_pnl": 0.0, "summary": "Erreur lecture historique"}


def adaptive_min_confidence(perf: dict) -> float:
    """Ajuste MIN_CONFIDENCE selon les performances recentes (apprentissage)."""
    if perf["n"] < 6:
        return MIN_CONFIDENCE
    wr = perf["win_rate"]
    if wr >= 0.65:
        conf = max(0.55, MIN_CONFIDENCE - 0.05)
        log.info(f"Perf bonne (WR {wr*100:.0f}%) → confiance abaissee a {conf:.2f}")
    elif wr <= 0.40:
        conf = min(0.80, MIN_CONFIDENCE + 0.08)
        log.info(f"Perf faible (WR {wr*100:.0f}%) → confiance remontee a {conf:.2f}")
    else:
        conf = MIN_CONFIDENCE
    return conf

# ─── GESTION QUOTIDIENNE ─────────────────────────────────────
def check_daily_reset():
    now = datetime.now()
    if now.hour == 0 and now.minute < 2:
        state["daily_pnl"]           = 0.0
        state["trades_today"]        = []
        state["paused"]              = False
        state["paused_until"]        = 0
        state["loss_streak"]         = 0
        state["open_exits"]          = {}
        state["daily_start_balance"] = min(get_balance(), CAPITAL_LIMIT_USDT)
        log.info("Reset quotidien effectue")

def check_daily_loss():
    if not state["daily_start_balance"]:
        state["daily_start_balance"] = min(get_balance(), CAPITAL_LIMIT_USDT)
        return
    current = min(get_balance(), CAPITAL_LIMIT_USDT)
    pnl_pct = (current - state["daily_start_balance"]) / state["daily_start_balance"]
    state["daily_pnl"] = pnl_pct

    # Perte journaliere maximale atteinte → pause jusqu'a minuit
    if pnl_pct <= -DAILY_LOSS_CAP and not state["paused"]:
        state["paused"] = True
        now = datetime.now()
        midnight = datetime(now.year, now.month, now.day) + timedelta(days=1)
        state["paused_until"] = midnight.timestamp()
        msg = (f"BOT PAUSE JOURNALIERE — Perte: {pnl_pct*100:.2f}% "
               f"(cap -{DAILY_LOSS_CAP*100:.0f}%) — reprise a minuit")
        log.warning(msg)
        send_telegram(msg)
        return

    # Serie de pertes consecutives → pause 2h
    completed = [t for t in state["trades_today"] if t.get("pnl_pct") is not None]
    if len(completed) >= 2:
        recent = completed[-MAX_LOSS_STREAK:]
        if all(t.get("pnl_pct", 0) < 0 for t in recent) and len(recent) >= MAX_LOSS_STREAK:
            if state["loss_streak"] < MAX_LOSS_STREAK:
                state["loss_streak"] = MAX_LOSS_STREAK
            if not state["paused"]:
                state["paused"]       = True
                state["paused_until"] = time.time() + 7200  # 2h
                msg = (f"BOT PAUSE — {MAX_LOSS_STREAK} pertes consecutives — "
                       f"reprise dans 2h")
                log.warning(msg)
                send_telegram(msg)

# ─── BOUCLE PRINCIPALE ────────────────────────────────────────
def run_bot():
    log.info("Bot demarre — Mode TOUS ACTIFS Binance USDT")
    send_telegram(
        f"*Bot demarre v4.1*\n"
        f"Mode: {'TESTNET' if TESTNET else 'LIVE'} | Univers: toutes paires USDT\n"
        f"Max positions: {MAX_OPEN_POSITIONS} | Taille/trade: {int(MAX_TRADE_PCT*100)}%\n"
        f"Sources: TradingView + Binance Futures + Fear&Greed"
        + (f" + X/Twitter" if TWITTER_BEARER else "")
    )
    state["daily_start_balance"] = min(get_balance(), CAPITAL_LIMIT_USDT)
    log.info(f"Balance de depart: ${state['daily_start_balance']:.2f} USDT (cap: ${CAPITAL_LIMIT_USDT})")

    while True:
        try:
            check_daily_reset()
            check_daily_loss()
            sync_open_positions()

            # Gestion de la pause
            if state["paused"]:
                if time.time() < state.get("paused_until", 0):
                    remaining = int((state["paused_until"] - time.time()) / 60)
                    log.info(f"Bot en pause — {remaining} min restantes")
                else:
                    state["paused"] = False
                    log.info("Pause terminee — reprise du scan")
                time.sleep(SCAN_INTERVAL)
                continue

            # Max positions — polling rapide toutes les 5s (pas d'attente 60s)
            n_open = len(state["open_exits"])
            if n_open >= MAX_OPEN_POSITIONS:
                log.info(f"Max positions ({n_open}/{MAX_OPEN_POSITIONS}) — poll toutes les 5s")
                for _ in range(12):   # max 60s total
                    time.sleep(5)
                    sync_open_positions()
                    if len(state["open_exits"]) < MAX_OPEN_POSITIONS:
                        break
                continue

            # Univers de paires + signaux TV (cache adaptatif)
            symbols = get_all_symbols()
            state["scanning_symbols"] = symbols

            now_ts = time.time()
            # Cache TV plus court si marche volatile (ATR eleve)
            atr_pct = state.get("last_atr_pct", 1.0)
            tv_ttl  = 120 if atr_pct > 2.5 else TV_CACHE_SECS
            if now_ts - state["last_tv_update"] > tv_ttl:
                nb = max(1, len(symbols) // 100 + 1)
                log.info(f"Refresh TradingView ({len(symbols)} paires en {nb} batches, ttl={tv_ttl}s)...")
                tv = get_tv_signals(symbols)
                state["tv_signals"]     = tv
                state["last_tv_update"] = now_ts
            else:
                tv = state["tv_signals"]

            # Candidats LONG (signal positif) + SHORT (signal négatif)
            already_held = set(state["open_exits"].keys())
            half = max(2, TOP_CANDIDATES // 2)

            buy_cands = sorted(
                [(sym, tv.get(sym, {}).get("rec_all", 0) or 0)
                 for sym in symbols
                 if sym not in already_held
                 and (tv.get(sym, {}).get("rec_all", 0) or 0) > 0.15],
                key=lambda x: x[1], reverse=True
            )[:half]

            short_cands = sorted(
                [(sym, tv.get(sym, {}).get("rec_all", 0) or 0)
                 for sym in symbols
                 if sym not in already_held
                 and (tv.get(sym, {}).get("rec_all", 0) or 0) < -0.15],
                key=lambda x: x[1]  # plus négatif en premier
            )[:TOP_CANDIDATES - half]

            top_cands = buy_cands + short_cands

            if not top_cands:
                log.info(f"Aucun signal fort ({len(symbols)} paires scannees) — HOLD")
                time.sleep(SCAN_INTERVAL)
                continue

            log.info(f"Top candidats: {', '.join(f'{s}({r:+.2f})' for s, r in top_cands)} "
                     f"[{len(buy_cands)}L/{len(short_cands)}S]")
            state["current_scan"] = top_cands[0][0]

            # Contexte detaille — fetch en parallele (3 threads)
            def _fetch_ctx(sym_rec):
                sym, rec = sym_rec
                ctx                      = get_market_context(sym)
                ctx["tv_recommendation"] = rec
                ctx["tv_signals"]        = tv.get(sym, {})
                ctx["x_sentiment"]       = get_x_sentiment(sym)
                return ctx

            contexts = []
            with ThreadPoolExecutor(max_workers=3) as pool:
                futs = {pool.submit(_fetch_ctx, sr): sr[0] for sr in top_cands}
                for fut in as_completed(futs):
                    sym = futs[fut]
                    try:
                        contexts.append(fut.result())
                    except Exception as e:
                        log.warning(f"Contexte {sym} erreur: {e}")

            # Mettre a jour l'ATR moyen pour le cache TV adaptatif
            if contexts:
                state["last_atr_pct"] = sum(c.get("atr_pct", 1.0) for c in contexts) / len(contexts)

            if not contexts:
                time.sleep(SCAN_INTERVAL)
                continue

            # Stats + confiance adaptive
            perf     = get_perf_stats()
            min_conf = adaptive_min_confidence(perf)

            # Boucle multi-trade : jusqu'à 3 trades par cycle (ou MAX_OPEN_POSITIONS)
            remaining_ctx = list(contexts)
            trades_this_cycle = 0
            for _pass in range(3):
                if not remaining_ctx or len(state["open_exits"]) >= MAX_OPEN_POSITIONS:
                    break

                decision = ask_ai_multi(remaining_ctx, perf=perf)
                symbol   = decision.get("symbol", "NONE")

                if symbol == "NONE" or decision.get("action") == "HOLD":
                    log.info(f"IA HOLD — {decision.get('reasoning', '')}")
                    break

                ctx = next((c for c in remaining_ctx if c["symbol"] == symbol), None)
                if not ctx:
                    break

                approved, reason = risk_gate(decision, ctx, min_conf=min_conf)
                if not approved:
                    log.info(f"Risk Gate refus: {reason}")
                    # Retire ce candidat et essaie le suivant
                    remaining_ctx = [c for c in remaining_ctx if c["symbol"] != symbol]
                    continue

                execute_trade(decision, ctx)
                trades_this_cycle += 1
                remaining_ctx = [c for c in remaining_ctx if c["symbol"] != symbol]

            if trades_this_cycle == 0 and not any(
                d.get("action") != "HOLD" for d in [decision] if "decision" in dir()
            ):
                pass  # déjà loggué HOLD

        except KeyboardInterrupt:
            log.info("Arret manuel du bot")
            break
        except Exception as e:
            log.error(f"Erreur boucle principale: {e}")
            traceback.print_exc()

        time.sleep(SCAN_INTERVAL)

# ─── FLASK ───────────────────────────────────────────────────
app = Flask(__name__)

@app.after_request
def cors(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response

@app.route("/")
def dashboard():
    return send_from_directory(str(SCRIPT_DIR), "dashboard.html")

@app.route("/trades")
def trades_endpoint():
    log_file = SCRIPT_DIR / "trades.json"
    if log_file.exists():
        with open(log_file) as f:
            return jsonify(json.load(f))
    return jsonify([])

@app.route("/signals")
def signals_endpoint():
    tv  = state["tv_signals"]
    top = sorted(
        [(sym, data) for sym, data in tv.items()],
        key=lambda x: abs(x[1].get("rec_all", 0) or 0),
        reverse=True,
    )[:10]
    # Enrich avec funding rates
    enriched = {}
    for sym, data in top:
        futures = get_futures_context(sym)
        enriched[sym] = {**data, **futures}
    return jsonify({
        "scanning":     state["scanning_symbols"],
        "current":      state["current_scan"],
        "tv_signals":   enriched,
        "trending":     get_trending_coins()[:5],
        "last_updated": state["last_tv_update"],
    })

@app.route("/status")
def status():
    try:
        balance = min(get_balance(), CAPITAL_LIMIT_USDT)
    except:
        balance = None
    n_universe = len(state["scanning_symbols"])
    n_open     = len(state["open_exits"])
    pause_left = max(0, int((state.get("paused_until", 0) - time.time()) / 60))
    return jsonify({
        "status":          "paused" if state["paused"] else "running",
        "pause_min_left":  pause_left if state["paused"] else 0,
        "testnet":         TESTNET,
        "universe_size":   n_universe,
        "symbol":          f"ALL USDT ({n_universe} paires)",
        "daily_pnl_pct":   round(state["daily_pnl"] * 100, 2),
        "trades_today":    len(state["trades_today"]),
        "open_positions":  n_open,
        "max_positions":   MAX_OPEN_POSITIONS,
        "open_symbols":    list(state["open_exits"].keys()),
        "balance_usdt":    balance,
        "capital_limit":   CAPITAL_LIMIT_USDT,
        "last_signal":     state["last_signal"],
        "scanning":        state["scanning_symbols"][:8],
        "current_scan":    state["current_scan"],
        "loss_streak":     state.get("loss_streak", 0),
        "ai_provider":     f"Claude({AI_MODEL.split('-')[1]})" if _use_claude else f"Groq({GROQ_MODELS[_groq_model_idx % len(GROQ_MODELS)].split('-')[0]})",
    })

@app.route("/logs")
def logs_endpoint():
    log_file = SCRIPT_DIR / "bot.log"
    lines = []
    if log_file.exists():
        with open(log_file, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    return jsonify([l.rstrip("\n") for l in lines[-100:]])

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    log.info(f"Webhook TradingView recu: {data}")
    state["last_signal"] = data
    if not state["paused"]:
        threading.Thread(target=lambda: _handle_webhook(data), daemon=True).start()
    return jsonify({"status": "ok"})

def _handle_webhook(signal: dict):
    symbol = signal.get("symbol", "BTCUSDT")
    try:
        ctx                = get_market_context(symbol)
        ctx["tv_signals"]  = state["tv_signals"].get(symbol, {})
        ctx["x_sentiment"] = get_x_sentiment(symbol)
        decision           = ask_ai_multi([ctx])
        decision["symbol"] = symbol
        approved, reason   = risk_gate(decision, ctx)
        if approved and decision.get("action") != "HOLD":
            execute_trade(decision, ctx)
    except Exception as e:
        log.error(f"Webhook handler error: {e}")

# ─── ENTRYPOINT ───────────────────────────────────────────────
if __name__ == "__main__":
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    log.info("Dashboard + API sur http://localhost:5000")
    threading.Timer(1.5, lambda: webbrowser.open("http://localhost:5000")).start()
    app.run(host="0.0.0.0", port=5000, debug=False)
