import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API_KEY = os.environ["TWELVEDATA_API_KEY"]

SCRIPT_DIR = Path(__file__).parent.absolute()
ROOT_DIR = SCRIPT_DIR.parent
DATA_DIR = ROOT_DIR / "data"
SYMBOLS_FILE = DATA_DIR / "eu_symbol.json"
QUOTES_FILE = DATA_DIR / "eu_quotes.json"

LIQUIDITY_THRESHOLD = 10_000_000
BREAKOUT_PCT = 0.02
BREAKOUT_WINDOW = 20
NEWS_LOOKBACK_DAYS = 7
VOLUME_SPIKE_MULTIPLIER = 2.0
API_PAUSE_SECONDS = 0.8

DATA_DIR.mkdir(parents=True, exist_ok=True)

with open(SYMBOLS_FILE, "r", encoding="utf-8") as f:
    raw_symbols = json.load(f)
    symbols = raw_symbols["items"] if isinstance(raw_symbols, dict) and "items" in raw_symbols else raw_symbols


def fetch_json(base_url, params):
    query = dict(params)
    query["apikey"] = API_KEY
    url = f"{base_url}?{urlencode(query)}"
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=25) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_quote(symbol):
    data = fetch_json("https://api.twelvedata.com/quote", {"symbol": symbol})
    if isinstance(data, dict) and data.get("status") == "error":
        raise RuntimeError(data.get("message", "Quote error"))
    return data


def fetch_time_series(symbol, outputsize=30):
    data = fetch_json(
        "https://api.twelvedata.com/time_series",
        {
            "symbol": symbol,
            "interval": "1day",
            "outputsize": outputsize,
            "order": "DESC",
            "format": "JSON",
        },
    )
    if isinstance(data, dict) and data.get("status") == "error":
        raise RuntimeError(data.get("message", "Time series error"))
    return data


def fetch_news(symbol, outputsize=10):
    try:
        data = fetch_json(
            "https://api.twelvedata.com/news",
            {
                "symbol": symbol,
                "outputsize": outputsize,
            },
        )
        if isinstance(data, dict) and data.get("status") == "error":
            return []
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
            return data["data"]
        return []
    except Exception:
        return []


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


def extract_candles_metrics(ts_data):
    values = ts_data.get("values", []) if isinstance(ts_data, dict) else []
    if not values:
        return {}

    latest = values[0]
    recent = values[:BREAKOUT_WINDOW]
    prior = values[1:BREAKOUT_WINDOW + 1]

    latest_close = to_float(latest.get("close"))
    latest_high = to_float(latest.get("high"))
    latest_low = to_float(latest.get("low"))
    latest_open = to_float(latest.get("open"))
    latest_volume = to_float(latest.get("volume"))

    recent_volumes = [to_float(v.get("volume")) for v in recent]
    prior_closes = [to_float(v.get("close")) for v in prior]

    avg_volume_20 = numeric_avg(recent_volumes[1:] if len(recent_volumes) > 1 else recent_volumes)
    avg_dollar_volume_20 = avg_volume_20 * latest_close if avg_volume_20 and latest_close else None
    prior_high = max([c for c in prior_closes if c is not None], default=latest_close)
    prev_close = to_float(values[1].get("close")) if len(values) > 1 else None

    return {
        "latest_close": latest_close,
        "latest_high": latest_high,
        "latest_low": latest_low,
        "latest_open": latest_open,
        "latest_volume": latest_volume,
        "avg_volume_20": avg_volume_20,
        "avg_dollar_volume_20": avg_dollar_volume_20,
        "prior_high": prior_high,
        "prev_close": prev_close,
        "latest_datetime": latest.get("datetime"),
    }


results = []
errors = []

for item in symbols:
    symbol = item.get("symbol") or item.get("ticker") or item.get("finnhubSymbol")
    if not symbol:
        errors.append({"symbol": None, "error": f"Missing symbol for item {item}"})
        continue

    try:
        quote = fetch_quote(symbol)
        time.sleep(API_PAUSE_SECONDS)

        ts_data = fetch_time_series(symbol, outputsize=30)
        time.sleep(API_PAUSE_SECONDS)

        news_items = fetch_news(symbol, outputsize=10)
        time.sleep(API_PAUSE_SECONDS)

        candle_metrics = extract_candles_metrics(ts_data)
        if not candle_metrics:
            errors.append({"symbol": symbol, "error": "No time series data"})
            continue

        current_price = to_float(quote.get("close")) or candle_metrics["latest_close"]
        prev_close = to_float(quote.get("previous_close")) or candle_metrics["prev_close"]
        change = current_price - prev_close if current_price is not None and prev_close is not None else None
        percent_change = (change / prev_close * 100) if change is not None and prev_close not in (None, 0) else None

        liquid = is_liquid(candle_metrics["avg_dollar_volume_20"])
        catalyst = has_catalyst(news_items)
        breakout = is_breakout(current_price, candle_metrics["prior_high"])
        volume_spike_2x = has_volume_spike(candle_metrics["latest_volume"], candle_metrics["avg_volume_20"])
        signal = compute_signal(liquid, catalyst, breakout, volume_spike_2x)

        headline = None
        if news_items:
            first = news_items[0]
            headline = first.get("title") or first.get("headline")

        results.append({
            "company": item.get("company"),
            "ticker": item.get("ticker", symbol),
            "symbol": symbol,
            "column": item.get("column"),
            "currentPrice": current_price,
            "change": round(change, 4) if change is not None else None,
            "percentChange": round(percent_change, 4) if percent_change is not None else None,
            "high": candle_metrics["latest_high"],
            "low": candle_metrics["latest_low"],
            "open": candle_metrics["latest_open"],
            "prevClose": prev_close,
            "volume": round(candle_metrics["latest_volume"]) if candle_metrics["latest_volume"] is not None else None,
            "avgVolume20": round(candle_metrics["avg_volume_20"]) if candle_metrics["avg_volume_20"] is not None else None,
            "avgDollarVolume20": round(candle_metrics["avg_dollar_volume_20"]) if candle_metrics["avg_dollar_volume_20"] is not None else None,
            "liquid": liquid,
            "catalyst": catalyst,
            "catalystHeadline": headline[:100] if headline else None,
            "breakout": breakout,
            "breakoutLevel": candle_metrics["prior_high"],
            "volumeSpike2x": volume_spike_2x,
            "quoteTimestamp": quote.get("datetime") or candle_metrics["latest_datetime"],
            "signal": signal,
            "dataSource": "Twelve Data",
        })

    except HTTPError as e:
        errors.append({"symbol": symbol, "error": f"HTTP {e.code}"})
    except URLError as e:
        errors.append({"symbol": symbol, "error": f"Network: {e.reason}"})
    except Exception as e:
        errors.append({"symbol": symbol, "error": str(e)})

payload = {
    "updatedAt": int(time.time()),
    "source": "twelve_data",
    "settings": {
        "liquidityThreshold": LIQUIDITY_THRESHOLD,
        "breakoutWindow": BREAKOUT_WINDOW,
        "breakoutPct": BREAKOUT_PCT,
        "newsLookbackDays": NEWS_LOOKBACK_DAYS,
        "volumeSpikeMultiplier": VOLUME_SPIKE_MULTIPLIER,
    },
    "items": results,
    "errors": errors,
}

with open(QUOTES_FILE, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2)

print(f"Updated {len(results)} quotes, {len(errors)} errors")
print(f"Output: {QUOTES_FILE}")
