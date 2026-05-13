import json
import time
from pathlib import Path

import yfinance as yf

SCRIPT_DIR = Path(__file__).parent.absolute()
ROOT_DIR = SCRIPT_DIR.parent
DATA_DIR = ROOT_DIR / "data"
SYMBOLS_FILE = DATA_DIR / "eu_symbol.json"
QUOTES_FILE = DATA_DIR / "eu_quotes.json"

LIQUIDITY_THRESHOLD = 10_000_000
BREAKOUT_PCT = 0.02
BREAKOUT_WINDOW = 20
VOLUME_SPIKE_MULTIPLIER = 2.0
API_PAUSE_SECONDS = 0.4

DATA_DIR.mkdir(parents=True, exist_ok=True)

with open(SYMBOLS_FILE, "r", encoding="utf-8") as f:
    raw_symbols = json.load(f)
    symbols = raw_symbols["items"] if isinstance(raw_symbols, dict) and "items" in raw_symbols else raw_symbols


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def numeric_avg(values):
    nums = [v for v in values if isinstance(v, (int, float))]
    return sum(nums) / len(nums) if nums else None


def is_liquid(avg_dollar, threshold=LIQUIDITY_THRESHOLD):
    return bool(avg_dollar and avg_dollar >= threshold)


def has_catalyst(news_items, min_count=1):
    return len(news_items) >= min_count


def is_breakout(close_price, prior_high, pct=BREAKOUT_PCT):
    return bool(close_price and prior_high and close_price >= prior_high * (1 + pct))


def has_volume_spike(current_volume, avg_volume, mult=VOLUME_SPIKE_MULTIPLIER):
    return bool(current_volume and avg_volume and current_volume >= avg_volume * mult)


def compute_signal(liquid, catalyst, breakout, volume_spike_2x):
    if liquid and catalyst and (breakout or volume_spike_2x):
        return "Buy"
    if liquid:
        return "Hold"
    return "Sell"


def get_news_items(ticker_obj):
    try:
        news = ticker_obj.news
        return news if isinstance(news, list) else []
    except Exception:
        return []


results = []
errors = []

for item in symbols:
    symbol = item.get("symbol") or item.get("ticker") or item.get("yahooSymbol")
    if not symbol:
        errors.append({"symbol": None, "error": f"Missing symbol for item {item}"})
        continue

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="2mo", interval="1d", auto_adjust=False)

        if hist is None or hist.empty:
            errors.append({"symbol": symbol, "error": "No price history returned"})
            continue

        hist = hist.tail(BREAKOUT_WINDOW + 2).copy()
        latest = hist.iloc[-1]
        prior = hist.iloc[:-1]
        recent_for_avg = prior.tail(BREAKOUT_WINDOW)

        latest_close = to_float(latest.get("Close"))
        latest_high = to_float(latest.get("High"))
        latest_low = to_float(latest.get("Low"))
        latest_open = to_float(latest.get("Open"))
        latest_volume = to_float(latest.get("Volume"))

        prev_close = to_float(prior.iloc[-1].get("Close")) if not prior.empty else None
        recent_volumes = [to_float(v) for v in recent_for_avg["Volume"].tolist()] if "Volume" in recent_for_avg else []
        recent_closes = [to_float(v) for v in recent_for_avg["Close"].tolist()] if "Close" in recent_for_avg else []

        avg_volume_20 = numeric_avg(recent_volumes)
        avg_dollar_volume_20 = avg_volume_20 * latest_close if avg_volume_20 and latest_close else None
        prior_high = max([c for c in recent_closes if c is not None], default=latest_close)

        news_items = get_news_items(ticker)
        liquid = is_liquid(avg_dollar_volume_20)
        catalyst = has_catalyst(news_items)
        breakout = is_breakout(latest_close, prior_high)
        volume_spike_2x = has_volume_spike(latest_volume, avg_volume_20)
        signal = compute_signal(liquid, catalyst, breakout, volume_spike_2x)

        headline = None
        if news_items:
            first = news_items[0]
            headline = first.get("title") or first.get("headline")

        quote_timestamp = None
        try:
            quote_timestamp = str(hist.index[-1])
        except Exception:
            quote_timestamp = None

        change = latest_close - prev_close if latest_close is not None and prev_close is not None else None
        percent_change = (change / prev_close * 100) if change is not None and prev_close not in (None, 0) else None

        results.append({
            "company": item.get("company"),
            "ticker": item.get("ticker", symbol),
            "symbol": symbol,
            "column": item.get("column"),
            "currentPrice": latest_close,
            "change": round(change, 4) if change is not None else None,
            "percentChange": round(percent_change, 4) if percent_change is not None else None,
            "high": latest_high,
            "low": latest_low,
            "open": latest_open,
            "prevClose": prev_close,
            "volume": round(latest_volume) if latest_volume is not None else None,
            "avgVolume20": round(avg_volume_20) if avg_volume_20 is not None else None,
            "avgDollarVolume20": round(avg_dollar_volume_20) if avg_dollar_volume_20 is not None else None,
            "liquid": liquid,
            "catalyst": catalyst,
            "catalystHeadline": headline[:100] if headline else None,
            "breakout": breakout,
            "breakoutLevel": prior_high,
            "volumeSpike2x": volume_spike_2x,
            "quoteTimestamp": quote_timestamp,
            "signal": signal,
            "dataSource": "Yahoo Finance / yfinance"
        })

        time.sleep(API_PAUSE_SECONDS)

    except Exception as e:
        errors.append({"symbol": symbol, "error": str(e)})

payload = {
    "updatedAt": int(time.time()),
    "source": "yahoo_finance",
    "settings": {
        "liquidityThreshold": LIQUIDITY_THRESHOLD,
        "breakoutWindow": BREAKOUT_WINDOW,
        "breakoutPct": BREAKOUT_PCT,
        "volumeSpikeMultiplier": VOLUME_SPIKE_MULTIPLIER,
    },
    "items": results,
    "errors": errors,
}

with open(QUOTES_FILE, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2)

print(f"Updated {len(results)} quotes, {len(errors)} errors")
print(f"Output: {QUOTES_FILE}")
