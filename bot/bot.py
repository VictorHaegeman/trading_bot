"""
=============================================================
  AI TRADING BOT — Binance x Groq x TradingView x X
  Version 4.0 — Multi-Crypto — ROI-Optimized — Windows
=============================================================
  pip install python-binance groq requests python-dotenv flask

  .env requis (dans le meme dossier que bot.py) :
    BINANCE_API_KEY=...
    BINANCE_SECRET=...
    GROQ_API_KEY=...
    TESTNET=true              <- false pour le live
    TELEGRAM_TOKEN=...        <- optionnel
    TELEGRAM_CHAT_ID=...      <- optionnel
    TWITTER_BEARER_TOKEN=...  <- optionnel
=============================================================
"""

import os
import sys
import json
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
from flask import Flask, request, jsonify, send_from_directory
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
import webbrowser


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

TESTNET          = os.getenv("TESTNET", "true").lower() != "false"
BINANCE_API_KEY  = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_SECRET   = os.getenv("BINANCE_SECRET", "").strip()
GROQ_API_KEY     = os.getenv("GROQ_API_KEY", "").strip()
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TWITTER_BEARER   = os.getenv("TWITTER_BEARER_TOKEN", "").strip()

# Ignore placeholder values
if TELEGRAM_TOKEN and ("COLLE" in TELEGRAM_TOKEN or len(TELEGRAM_TOKEN) < 20):
    TELEGRAM_TOKEN = ""
if TELEGRAM_CHAT_ID and "COLLE" in TELEGRAM_CHAT_ID:
    TELEGRAM_CHAT_ID = ""

# Parametres de risque
MAX_TRADE_PCT         = 0.10    # 10% du capital par trade (max 10 positions simultanées)
DAILY_LOSS_CAP        = 0.04    # Pause si -4% dans la journee
CAPITAL_LIMIT_USDT    = float(os.getenv("CAPITAL_LIMIT_USDT", "1000"))
MAX_DAILY_TRADES      = int(os.getenv("MAX_DAILY_TRADES", "12"))   # 12 trades/jour max
MAX_LOSS_STREAK       = int(os.getenv("MAX_LOSS_STREAK", "3"))     # pause apres 3 pertes consecutives
MIN_CONFIDENCE        = float(os.getenv("MIN_CONFIDENCE", "0.65"))
MAX_OPEN_POSITIONS    = int(os.getenv("MAX_OPEN_POSITIONS", "8"))  # max positions simultanées
ALL_SYMBOLS_MIN_VOL   = float(os.getenv("ALL_SYMBOLS_MIN_VOL", "200000"))  # vol 24h min en USDT
TOP_CANDIDATES        = int(os.getenv("TOP_CANDIDATES", "5"))      # candidats analyses en detail
SCAN_INTERVAL         = 60
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
for name, val in [("BINANCE_API_KEY", BINANCE_API_KEY), ("BINANCE_SECRET", BINANCE_SECRET), ("GROQ_API_KEY", GROQ_API_KEY)]:
    print(f"[CHECK] {name}: {'OK' if val else 'MANQUANTE'}")
    if not val:
        log.error(f"{name} manquante — verifier le .env")
        sys.exit(1)

if TWITTER_BEARER:
    log.info("X/Twitter: cle detectee — sentiment active")
else:
    log.info("X/Twitter: pas de cle — sentiment desactive")

# ─── CLIENTS ─────────────────────────────────────────────────
if TESTNET:
    binance = Client(BINANCE_API_KEY, BINANCE_SECRET, testnet=True)
    log.info("MODE TESTNET — aucun vrai argent en jeu")
else:
    binance = Client(BINANCE_API_KEY, BINANCE_SECRET)
    log.info("MODE LIVE — attention argent reel")

# Sync horloge avec Binance (evite -1021 timestamp error)
try:
    server_ms = binance.get_server_time()["serverTime"]
    local_ms  = int(time.time() * 1000)
    binance.timestamp_offset = server_ms - local_ms
    log.info(f"Timestamp offset Binance: {binance.timestamp_offset}ms")
except Exception as e:
    log.warning(f"Impossible de syncer l'horloge Binance: {e}")

groq_client = Groq(api_key=GROQ_API_KEY)

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
def get_all_symbols() -> list:
    """Toutes les paires USDT de Binance avec volume suffisant, sans tokens levier."""
    try:
        excluded = ["DOWN", "UP", "BEAR", "BULL", "3L", "3S", "2L", "2S", "BUSD"]
        tickers = binance.get_ticker()
        pairs = [
            t for t in tickers
            if t["symbol"].endswith("USDT")
            and not any(x in t["symbol"] for x in excluded)
            and float(t.get("quoteVolume", 0)) > ALL_SYMBOLS_MIN_VOL
        ]
        pairs.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
        result = [t["symbol"] for t in pairs]
        log.info(f"Univers: {len(result)} paires USDT (vol>{'%.0fk' % (ALL_SYMBOLS_MIN_VOL/1000)}/24h)")
        return result
    except Exception as e:
        log.warning(f"get_all_symbols error: {e}")
        return ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]

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
    now = time.time()
    if now - _bal_cache["ts"] < 30:
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

def get_step_size(symbol: str) -> float:
    """Retourne le stepSize pour la precision de quantite."""
    try:
        info = binance.get_symbol_info(symbol)
        for f in info["filters"]:
            if f["filterType"] == "LOT_SIZE":
                return float(f["stepSize"])
    except:
        pass
    return 0.001

def round_qty(qty: float, step: float) -> float:
    if step <= 0:
        return qty
    precision = max(0, len(f"{step:.10f}".rstrip("0").split(".")[-1]))
    return round(round(qty / step) * step, precision)

def get_tick_size(symbol: str) -> float:
    """Tick size pour la precision des prix (OCO orders)."""
    try:
        info = binance.get_symbol_info(symbol)
        for f in info["filters"]:
            if f["filterType"] == "PRICE_FILTER":
                return float(f["tickSize"])
    except:
        pass
    return 0.01

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

    # Taille max dynamique basee sur ATR (risquer 1.5% du capital max)
    # SL typique = 1.5x ATR ; size = risk_amount / (1.5*ATR/price)
    risk_amount = balance * 0.015
    atr_sl_dist = 1.5 * atr / price if price > 0 else 0.02
    atr_based_size = round(risk_amount / atr_sl_dist, 2) if atr_sl_dist > 0 else balance * MAX_TRADE_PCT
    max_size = min(balance * MAX_TRADE_PCT, atr_based_size)

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
    }

# ─── DECISION IA MULTI-CRYPTO ─────────────────────────────────
def ask_ai_multi(contexts: list) -> dict:
    """Passe plusieurs paires candidates a Groq (Llama 3.3 70B)."""
    balance   = contexts[0]["balance_usdt"]
    max_trade = round(balance * MAX_TRADE_PCT, 2)
    fg        = contexts[0]["fear_greed_value"]
    trending  = get_trending_coins()

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
  Positions ouvertes: {len(ctx['open_positions'])}{x_str}
"""

    prompt = f"""Tu es un trader algorithmique expert. Tu geres un portefeuille spot sur Binance ({datetime.now().strftime('%H:%M UTC')}).

CONTEXTE MARCHE:
- Capital: ${balance} | Fear&Greed: {fg}/100 ({contexts[0]['fear_greed_label']})
- Trending CoinGecko: {', '.join(trending[:5]) if trending else 'N/A'}
- Serie de pertes consecutives: {state['loss_streak']} (max autorise: {MAX_LOSS_STREAK})

PAIRES CANDIDATES:
{cands_text}

CRITERES DE FILTRAGE STRICTS (elimine les setups faibles):
1. ALIGNEMENT 4H OBLIGATOIRE: BUY seulement si biais_4h = BULLISH ou NEUTRE
   → Si biais_4h = BEARISH: confidence requise > 0.82 minimum (contre-tendance)
2. RSI 1h: BUY uniquement entre 32 et 67 (evite surachats/surventes extremes)
3. VOLUME: vol_ratio >= 1.1x minimum pour confirmer le mouvement
4. L/S RATIO: si "SURCHARGE LONGS" (>65% longs) → eviter nouveaux BUY (retournement probable)
5. FUNDING: si "OVERLEV LONG" → signal baissier, reduire confiance de 0.10
6. Confidence minimum: {MIN_CONFIDENCE} — en dessous → HOLD automatique
7. R:R minimum: 1.8:1 (take_profit - entry doit etre >= 1.8x (entry - stop_loss))
8. Stop-loss base sur ATR: SL = entry - (1.5 × ATR) arrondi au tick, max 2.5% de l'entry
9. Take-profit = entry + (2.7 × ATR) minimum, max 4.5%
10. Taille: utilise la taille ATR-optimale si disponible (ajustee a la volatilite)

ACTION POSSIBLE: BUY uniquement (pas de SELL short sur spot)

Reponds UNIQUEMENT avec ce JSON valide (rien avant, rien apres):
{{
  "symbol": "SOLUSDT",
  "action": "BUY",
  "size_usdt": 120.00,
  "entry_price": 185.50,
  "stop_loss": 181.32,
  "take_profit": 196.80,
  "confidence": 0.71,
  "reasoning": "4h BULLISH + RSI 1h=52 non survendu + vol 1.4x + EMA50 support + funding neutre"
}}

Si aucun setup ne respecte tous les criteres:
{{"symbol": "NONE", "action": "HOLD", "size_usdt": null, "entry_price": null, "stop_loss": null, "take_profit": null, "confidence": 0.0, "reasoning": "raison precise"}}"""

    try:
        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "Tu es un expert en trading algorithmique. Reponds TOUJOURS et UNIQUEMENT avec un objet JSON valide, sans aucun texte avant ou apres."},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.1,
            max_tokens=300,
        )
        raw      = response.choices[0].message.content.strip().replace("```json", "").replace("```", "").strip()
        decision = json.loads(raw)
        log.info(f"IA → {decision.get('symbol','?')} {decision['action']} conf={decision.get('confidence','?')} | {decision.get('reasoning','')}")
        return decision
    except json.JSONDecodeError:
        log.error(f"JSON invalide de Groq: {raw}")
        return {"symbol": "NONE", "action": "HOLD", "confidence": 0, "reasoning": "JSON invalide"}
    except Exception as e:
        log.error(f"Groq error: {e}")
        return {"symbol": "NONE", "action": "HOLD", "confidence": 0, "reasoning": str(e)}

# ─── RISK GATE ────────────────────────────────────────────────
def risk_gate(decision: dict, context: dict) -> tuple:
    action = decision.get("action", "HOLD")
    if action == "HOLD":
        return True, "OK"

    # Pause temporaire (perte journaliere ou serie de pertes)
    if state["paused"]:
        if time.time() < state.get("paused_until", 0):
            remaining = int((state["paused_until"] - time.time()) / 60)
            return False, f"Bot en pause ({remaining} min restantes)"
        else:
            state["paused"] = False

    # Max trades quotidiens
    if len(state["trades_today"]) >= MAX_DAILY_TRADES:
        return False, f"Max trades atteint ({MAX_DAILY_TRADES}/jour)"

    # Serie de pertes consecutives
    if state["loss_streak"] >= MAX_LOSS_STREAK:
        return False, f"Serie de pertes ({state['loss_streak']}x) — pause forcee"

    # Confidence minimum
    conf = decision.get("confidence", 0)
    if conf < MIN_CONFIDENCE:
        return False, f"Confidence trop faible ({conf:.2f} < {MIN_CONFIDENCE})"

    # Stop-loss obligatoire
    if not decision.get("stop_loss"):
        return False, "Stop-loss manquant"

    entry  = decision.get("entry_price", 0)
    sl     = decision.get("stop_loss", 0)
    tp     = decision.get("take_profit", 0)

    # Taille de position
    size     = decision.get("size_usdt") or 0
    max_size = context.get("max_size_usdt") or (context["balance_usdt"] * MAX_TRADE_PCT)
    if size > max_size * 1.05:  # tolérance 5%
        decision["size_usdt"] = round(max_size, 2)
        log.info(f"Taille réduite: ${size} → ${decision['size_usdt']:.2f}")
    if decision["size_usdt"] < 10:
        return False, f"Taille trop petite (${decision['size_usdt']:.2f} < $10)"
    if decision["size_usdt"] > context["balance_usdt"]:
        return False, f"Balance insuffisante (${context['balance_usdt']:.2f} dispo)"

    # RSI 1h: eviter extremes
    rsi = context.get("rsi_14", 50)
    if action == "BUY" and rsi > 72:
        return False, f"RSI 1h surachete ({rsi} > 72)"

    # Volume minimum
    vol_ratio = context.get("volume_ratio", 1.0)
    if vol_ratio < 0.7:
        return False, f"Volume insuffisant ({vol_ratio}x < 0.7x)"

    # Biais 4h : contre-tendance requiert conf > 0.82
    bias_4h = context.get("bias_4h", "NEUTRE")
    if action == "BUY" and bias_4h == "BEARISH" and conf <= 0.82:
        return False, f"Contre-tendance 4h BEARISH (conf {conf:.2f} <= 0.82)"

    # Surcharge longs (risque de liquidation cascade)
    ls_label = context.get("ls_label", "")
    if action == "BUY" and ls_label == "SURCHARGE LONGS":
        return False, "Marche surcharge en longs — BUY risque"

    # R:R minimum 1.8
    if entry and sl and tp:
        risk   = abs(entry - sl)
        reward = abs(tp - entry)
        if risk > 0 and (reward / risk) < 1.8:
            return False, f"R:R insuffisant ({reward/risk:.2f} < 1.8)"

    # SL max 2.5% sous l'entry
    if entry and sl:
        sl_pct = abs(entry - sl) / entry * 100
        if sl_pct > 2.5:
            return False, f"SL trop loin ({sl_pct:.2f}% > 2.5%)"

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

        oco = binance.order_oco_sell(
            symbol=symbol,
            quantity=qty,
            price=str(tp),
            stopPrice=str(sl),
            stopLimitPrice=str(sl_limit),
            stopLimitTimeInForce="GTC",
        )
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


def sync_open_positions():
    """Retire de open_exits les OCO clos (TP ou SL declenche) et met a jour loss_streak."""
    to_remove = []
    for sym, info in list(state["open_exits"].items()):
        oco_id = info.get("oco_id")
        if not oco_id:
            continue
        try:
            oco    = binance.get_order_list(orderListId=int(oco_id))
            status = oco.get("listOrderStatus", "")
            if status not in ("ALL_DONE", "RESPONSE"):
                continue
            # OCO fini — chercher l'ordre FILLED pour savoir SL ou TP
            for ord_ref in oco.get("orders", []):
                try:
                    detail    = binance.get_order(symbol=sym, orderId=ord_ref["orderId"])
                    ord_type  = detail.get("type", "")
                    ord_st    = detail.get("status", "")
                    if ord_st != "FILLED":
                        continue
                    fill_px   = float(detail.get("price", 0) or detail.get("cummulativeQuoteQty", 0))
                    entry_px  = info.get("entry_price", fill_px)
                    sl_px     = info.get("sl", 0)
                    if fill_px and fill_px <= sl_px * 1.002:  # ±0.2% = SL
                        state["loss_streak"] = state.get("loss_streak", 0) + 1
                        pnl = (fill_px - entry_px) / entry_px * 100 if entry_px else 0
                        log.warning(f"SL {sym} @ ${fill_px:.4g} | PnL {pnl:+.2f}% | streak {state['loss_streak']}")
                        send_telegram(f"SL *{sym}* @ `${fill_px:.4g}` | PnL `{pnl:+.2f}%`")
                    else:
                        state["loss_streak"] = 0
                        pnl = (fill_px - entry_px) / entry_px * 100 if entry_px else 0
                        log.info(f"TP {sym} @ ${fill_px:.4g} | PnL {pnl:+.2f}%")
                        send_telegram(f"TP *{sym}* @ `${fill_px:.4g}` | PnL `{pnl:+.2f}%`")
                    break
                except Exception:
                    pass
            to_remove.append(sym)
        except Exception as e:
            log.warning(f"sync OCO check {sym}: {e}")

    for sym in to_remove:
        del state["open_exits"][sym]
    if to_remove:
        log.info(f"Positions fermees: {', '.join(to_remove)}")


# ─── EXECUTION ────────────────────────────────────────────────
def execute_trade(decision: dict, context: dict):
    action = decision["action"]
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

        # Placement OCO si SL + TP definis
        if sl and tp:
            oco_id = place_exit_orders(symbol, qty, sl, tp, entry_price=fill_price)
            if oco_id:
                trade_log["oco_id"] = oco_id
                save_trade_log(trade_log)  # mise a jour avec l'id OCO

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
        logs.append(trade)
        with open(log_file, "w") as f:
            json.dump(logs, f, indent=2)
    except Exception as e:
        log.error(f"Erreur save trade log: {e}")

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

            # Max positions ouvertes atteint
            n_open = len(state["open_exits"])
            if n_open >= MAX_OPEN_POSITIONS:
                log.info(f"Max positions ({n_open}/{MAX_OPEN_POSITIONS}) — en attente de fermeture")
                time.sleep(SCAN_INTERVAL)
                continue

            # Univers de paires + signaux TV (avec cache 5min)
            symbols = get_all_symbols()
            state["scanning_symbols"] = symbols

            now_ts = time.time()
            if now_ts - state["last_tv_update"] > TV_CACHE_SECS:
                nb = max(1, len(symbols) // 100 + 1)
                log.info(f"Refresh TradingView ({len(symbols)} paires en {nb} batches)...")
                tv = get_tv_signals(symbols)
                state["tv_signals"]     = tv
                state["last_tv_update"] = now_ts
            else:
                tv = state["tv_signals"]

            # Candidats BUY uniquement, excluant les positions deja ouvertes
            already_held = set(state["open_exits"].keys())
            candidates   = [
                (sym, tv.get(sym, {}).get("rec_all", 0) or 0)
                for sym in symbols
                if sym not in already_held
                and (tv.get(sym, {}).get("rec_all", 0) or 0) > 0.15
            ]
            candidates.sort(key=lambda x: x[1], reverse=True)
            top_cands = candidates[:TOP_CANDIDATES]

            if not top_cands:
                log.info(f"Aucun signal BUY fort ({len(symbols)} paires scannees) — HOLD")
                time.sleep(SCAN_INTERVAL)
                continue

            log.info(f"Top candidats: {', '.join(f'{s}({r:+.2f})' for s, r in top_cands)}")
            state["current_scan"] = top_cands[0][0]

            # Contexte detaille pour chaque candidat
            contexts = []
            for sym, rec in top_cands:
                try:
                    ctx                      = get_market_context(sym)
                    ctx["tv_recommendation"] = rec
                    ctx["tv_signals"]        = tv.get(sym, {})
                    ctx["x_sentiment"]       = get_x_sentiment(sym)
                    contexts.append(ctx)
                except Exception as e:
                    log.warning(f"Contexte {sym} erreur: {e}")

            if not contexts:
                time.sleep(SCAN_INTERVAL)
                continue

            # Décision IA
            decision = ask_ai_multi(contexts)
            symbol   = decision.get("symbol", "NONE")

            if symbol == "NONE" or decision.get("action") == "HOLD":
                log.info(f"IA HOLD — {decision.get('reasoning', '')}")
            else:
                ctx = next((c for c in contexts if c["symbol"] == symbol), contexts[0])
                approved, reason = risk_gate(decision, ctx)
                if not approved:
                    log.info(f"Risk Gate refus: {reason}")
                    if decision.get("confidence", 0) >= MIN_CONFIDENCE:
                        send_telegram(f"*Trade refuse*\n{symbol}: {reason}")
                else:
                    execute_trade(decision, ctx)

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
