"""
=============================================================
  AI TRADING BOT — Binance x Groq x TradingView x X
  Version 3.1 — Multi-Crypto — Testnet Safe — Windows
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
from datetime import datetime
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
MAX_TRADE_PCT   = 0.20   # Max 20% du capital par trade
DAILY_LOSS_CAP  = 0.05   # Pause si -5% dans la journee
SCAN_INTERVAL   = 60     # Secondes entre analyses
TOP_SYMBOLS_N   = 15     # Nombre de paires a surveiller
TV_CACHE_SECS   = 300    # Refresh TradingView toutes les 5min
CG_CACHE_SECS   = 600    # Refresh CoinGecko trending toutes les 10min

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

groq_client = Groq(api_key=GROQ_API_KEY)

# ─── ETAT GLOBAL ─────────────────────────────────────────────
state = {
    "daily_pnl":           0.0,
    "daily_start_balance": None,
    "paused":              False,
    "trades_today":        [],
    "last_signal":         None,
    "scanning_symbols":    [],
    "tv_signals":          {},
    "last_tv_update":      0,
    "current_scan":        "—",
    "trending":            [],
    "last_cg_update":      0,
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
def get_top_symbols(n: int = TOP_SYMBOLS_N) -> list:
    """Top N paires USDT par volume 24h (exclut les tokens levier)."""
    try:
        tickers = binance.get_ticker()
        pairs = [
            t for t in tickers
            if t["symbol"].endswith("USDT")
            and not any(x in t["symbol"] for x in ["DOWN", "UP", "BEAR", "BULL", "3L", "3S"])
            and float(t.get("quoteVolume", 0)) > 500_000
        ]
        pairs.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
        result = [t["symbol"] for t in pairs[:n]]
        log.info(f"Top {n} paires: {', '.join(result[:5])}...")
        return result
    except Exception as e:
        log.warning(f"get_top_symbols error: {e}")
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

# ─── TRADINGVIEW SCREENER ─────────────────────────────────────
def get_tv_signals(symbols: list) -> dict:
    """Signaux TradingView via screener public (sans auth)."""
    try:
        url = "https://scanner.tradingview.com/crypto/scan"
        tickers = [f"BINANCE:{s}" for s in symbols]
        payload = {
            "symbols": {"tickers": tickers},
            "columns": [
                "name", "close", "RSI", "RSI[1]",
                "EMA20", "EMA50",
                "MACD.macd", "MACD.signal",
                "Recommend.All", "Recommend.MA", "Recommend.Other",
                "relative_volume_10d_calc", "change",
            ],
        }
        headers = {
            "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer":      "https://www.tradingview.com",
            "Origin":       "https://www.tradingview.com",
            "Content-Type": "application/json",
        }
        r = http.post(url, json=payload, headers=headers, timeout=15)
        results = {}
        for row in r.json().get("data", []):
            sym = row["s"].replace("BINANCE:", "")
            d   = row["d"]
            rec = float(d[8]) if d[8] is not None else 0.0
            results[sym] = {
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
        log.info(f"TradingView: {len(results)} signaux recus")
        return results
    except Exception as e:
        log.error(f"TradingView screener error: {e}")
        return {}

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
    klines     = get_klines(symbol, interval="1h", limit=50)
    closes     = [k["close"] for k in klines]
    price      = get_price(symbol)
    rsi        = get_rsi(closes)
    fg         = get_fear_greed()      # cached
    balance    = get_balance()         # cached
    ema20      = sum(closes[-20:]) / 20
    ema50      = sum(closes[-50:]) / min(50, len(closes))

    klines_15m = get_klines(symbol, interval="15m", limit=20)
    vols       = [k["volume"] for k in klines_15m]
    vol_ratio  = round(vols[-1] / (sum(vols) / len(vols)), 2) if vols else 1.0

    futures = get_futures_context(symbol)

    return {
        "symbol":           symbol,
        "price":            price,
        "rsi_14":           rsi,
        "ema20":            round(ema20, 6),
        "ema50":            round(ema50, 6),
        "price_vs_ema20":   "above" if price > ema20 else "below",
        "price_vs_ema50":   "above" if price > ema50 else "below",
        "volume_ratio":     vol_ratio,
        "fear_greed_value": fg["value"],
        "fear_greed_label": fg["label"],
        "balance_usdt":     round(balance, 2),
        "daily_pnl_pct":    round(state["daily_pnl"], 4),
        "open_positions":   get_open_positions(symbol),
        "funding_rate":     futures["funding_rate"],
        "funding_label":    futures["funding_label"],
        "open_interest":    futures["open_interest"],
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
        tv  = ctx.get("tv_signals", {})
        rec = tv.get("rec_all", ctx.get("tv_recommendation", 0)) or 0
        x   = ctx.get("x_sentiment", {})
        x_str = f"  X/Twitter: {x['label']} ({x['score']:.0f}/100, {x['count']} tweets)" if x.get("available") else ""
        trending_flag = " [TRENDING CG]" if ctx["symbol"] in trending else ""
        fr = ctx.get("funding_rate")
        fr_str = f"  Funding: {fr:+.4f}% ({ctx.get('funding_label','N/A')})" if fr is not None else ""
        cands_text += f"""
▸ {ctx['symbol']}{trending_flag}
  Prix: ${ctx['price']:,.6g}  |  RSI(14): {ctx['rsi_14']}  |  Vol ratio: {ctx['volume_ratio']}x
  EMA20: ${ctx['ema20']:,.6g} ({ctx['price_vs_ema20']}) | EMA50: ${ctx['ema50']:,.6g} ({ctx['price_vs_ema50']})
  TradingView: {tv.get('label','N/A')} (score {rec:+.2f}) | MACD: {tv.get('macd',0) or 0:.4g} vs {tv.get('macd_signal',0) or 0:.4g}
  Positions ouvertes: {len(ctx['open_positions'])}{fr_str}{x_str}
"""

    prompt = f"""Tu es un trader algorithmique expert sur Binance. Analyse ces {len(contexts)} paires et choisis le MEILLEUR setup maintenant.

CONTEXTE GLOBAL ({datetime.now().strftime('%H:%M UTC')}):
- Capital USDT disponible: ${balance}  |  Max par trade: ${max_trade} (20%)
- Fear & Greed Index: {fg}/100 ({contexts[0]['fear_greed_label']})
- Coins trending (CoinGecko): {', '.join(trending[:5]) if trending else 'N/A'}

PAIRES CANDIDATES (preselectionnees par signal fort TradingView):
{cands_text}

REGLES OBLIGATOIRES:
1. Choisis UNE seule paire — ou HOLD si aucun setup vraiment convaincant
2. Stop-loss OBLIGATOIRE, max 3% sous le prix d'entree
3. Risk/Reward minimum 1.5:1
4. Confidence < 0.55 → HOLD obligatoire
5. Positions deja ouvertes → HOLD sauf signal exceptionnel (>0.85)

Reponds UNIQUEMENT avec ce JSON exact, rien d'autre:
{{
  "symbol": "ETHUSDT",
  "action": "BUY",
  "size_usdt": {max_trade},
  "entry_price": 3500.00,
  "stop_loss": 3395.00,
  "take_profit": 3658.00,
  "confidence": 0.72,
  "reasoning": "RSI survendu + signal TV ACHAT + EMA50 support tenu"
}}

Si aucun setup: {{"symbol": "NONE", "action": "HOLD", "size_usdt": null, "confidence": 0, "reasoning": "Aucun setup clair"}}"""

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
    if state["paused"]:
        return False, "Bot en pause (daily loss cap atteint)"
    if decision.get("confidence", 0) < 0.55:
        return False, f"Confidence trop faible ({decision['confidence']} < 0.55)"
    if not decision.get("stop_loss"):
        return False, "Stop-loss manquant"
    size     = decision.get("size_usdt") or 0
    max_size = context["balance_usdt"] * MAX_TRADE_PCT
    if size > max_size:
        return False, f"Taille trop grande (${size} > max ${max_size:.0f})"
    if size > context["balance_usdt"]:
        return False, f"Balance insuffisante (${context['balance_usdt']:.2f} dispo)"
    entry  = decision.get("entry_price", 0)
    sl     = decision.get("stop_loss", 0)
    tp     = decision.get("take_profit", 0)
    if entry and sl and tp:
        risk   = abs(entry - sl)
        reward = abs(tp - entry)
        if risk > 0 and (reward / risk) < 1.5:
            return False, f"R:R insuffisant ({reward/risk:.2f} < 1.5)"
    return True, "OK"

# ─── EXECUTION ────────────────────────────────────────────────
def execute_trade(decision: dict, context: dict):
    action = decision["action"]
    if action == "HOLD":
        return

    symbol = decision.get("symbol") or context["symbol"]
    side   = "BUY" if action == "BUY" else "SELL"
    price  = context["price"]
    size   = decision["size_usdt"]

    step = get_step_size(symbol)
    qty  = round_qty(size / price, step)

    try:
        order = binance.order_market(symbol=symbol, side=side, quantity=qty)
        log.info(f"ORDRE EXECUTE: {side} {qty} {symbol} @ ~${price:,.6g}")

        trade_log = {
            "timestamp":   datetime.now().isoformat(),
            "symbol":      symbol,
            "action":      action,
            "qty":         qty,
            "entry_price": price,
            "stop_loss":   decision.get("stop_loss"),
            "take_profit": decision.get("take_profit"),
            "size_usdt":   size,
            "reasoning":   decision.get("reasoning"),
            "confidence":  decision.get("confidence"),
            "order_id":    order["orderId"],
        }
        state["trades_today"].append(trade_log)
        save_trade_log(trade_log)

        sl_pct = abs(price - decision["stop_loss"]) / price * 100 if decision.get("stop_loss") else 0
        tp_pct = abs(decision["take_profit"] - price) / price * 100 if decision.get("take_profit") else 0
        send_telegram(
            f"{'BUY' if action=='BUY' else 'SELL'} *{action} {symbol}*\n"
            f"Prix: `${price:,.6g}` | Taille: `${size}` USDT\n"
            f"SL: `-{sl_pct:.1f}%` | TP: `+{tp_pct:.1f}%`\n"
            f"Conf: `{decision['confidence']}` | _{decision.get('reasoning','-')}_"
        )
    except BinanceAPIException as e:
        log.error(f"Binance error: {e}")
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
        state["daily_start_balance"] = get_balance()
        log.info("Reset quotidien effectue")

def check_daily_loss():
    if not state["daily_start_balance"]:
        state["daily_start_balance"] = get_balance()
        return
    current = get_balance()
    pnl_pct = (current - state["daily_start_balance"]) / state["daily_start_balance"]
    state["daily_pnl"] = pnl_pct
    if pnl_pct <= -DAILY_LOSS_CAP and not state["paused"]:
        state["paused"] = True
        msg = f"BOT PAUSE — Perte journaliere: {pnl_pct*100:.2f}% (cap -{DAILY_LOSS_CAP*100:.0f}%)"
        log.warning(msg)
        send_telegram(msg)

# ─── BOUCLE PRINCIPALE ────────────────────────────────────────
def run_bot():
    log.info(f"Bot demarre — Mode Multi-Crypto (Top {TOP_SYMBOLS_N} USDT)")
    send_telegram(
        f"*Bot demarre*\n"
        f"Mode: {'TESTNET' if TESTNET else 'LIVE'}\n"
        f"Cryptos: Top {TOP_SYMBOLS_N} paires USDT\n"
        f"Sources: TradingView + Binance + Fear&Greed"
        + (f" + X/Twitter" if TWITTER_BEARER else "")
    )
    state["daily_start_balance"] = get_balance()
    log.info(f"Balance de depart: ${state['daily_start_balance']:.2f} USDT")

    while True:
        try:
            check_daily_reset()
            check_daily_loss()

            if state["paused"]:
                log.info("Bot en pause — daily loss cap atteint")
                time.sleep(SCAN_INTERVAL)
                continue

            symbols = get_top_symbols(TOP_SYMBOLS_N)
            state["scanning_symbols"] = symbols

            now_ts = time.time()
            if now_ts - state["last_tv_update"] > TV_CACHE_SECS:
                log.info(f"Refresh TradingView ({len(symbols)} paires)...")
                tv = get_tv_signals(symbols)
                state["tv_signals"]     = tv
                state["last_tv_update"] = now_ts
            else:
                tv = state["tv_signals"]

            candidates = [
                (sym, tv.get(sym, {}).get("rec_all", 0) or 0)
                for sym in symbols
                if abs(tv.get(sym, {}).get("rec_all", 0) or 0) > 0.15
            ]
            candidates.sort(key=lambda x: abs(x[1]), reverse=True)
            top3 = candidates[:3]

            if not top3:
                log.info("Aucun signal TV fort detecte — HOLD ce cycle")
                time.sleep(SCAN_INTERVAL)
                continue

            log.info(f"Candidats: {', '.join(f'{s}({r:+.2f})' for s, r in top3)}")
            state["current_scan"] = top3[0][0]

            contexts = []
            for sym, rec in top3:
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

            decision = ask_ai_multi(contexts)
            symbol   = decision.get("symbol", "NONE")

            if symbol == "NONE" or decision.get("action") == "HOLD":
                log.info(f"HOLD — {decision.get('reasoning', '')}")
            else:
                ctx = next((c for c in contexts if c["symbol"] == symbol), contexts[0])
                approved, reason = risk_gate(decision, ctx)
                if not approved:
                    log.info(f"Risk Gate refus: {reason}")
                    if decision.get("confidence", 0) >= 0.55:
                        send_telegram(f"*Trade refuse (Risk Gate)*\n{reason}")
                else:
                    execute_trade(decision, ctx)

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
        balance = get_balance()
    except:
        balance = None
    return jsonify({
        "status":        "paused" if state["paused"] else "running",
        "testnet":       TESTNET,
        "symbol":        f"MULTI-CRYPTO ({TOP_SYMBOLS_N} paires)",
        "daily_pnl_pct": round(state["daily_pnl"] * 100, 2),
        "trades_today":  len(state["trades_today"]),
        "balance_usdt":  balance,
        "last_signal":   state["last_signal"],
        "scanning":      state["scanning_symbols"][:5],
        "current_scan":  state["current_scan"],
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
