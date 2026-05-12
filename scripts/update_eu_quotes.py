import json
import os
import time
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from pathlib import Path

API_KEY = os.environ["ALPHA_VANTAGE_API_KEY"]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))
DATA_DIR = os.path.join(ROOT_DIR, "data")
SYMBOLS_FILE = DATA_DIR / 'symbols.json'
QUOTES_FILE = DATA_DIR / 'quotes.json'
os.makedirs(DATA_DIR, exist_ok=True)

with open(SYMBOLS_FILE, "r", encoding="utf-8") as f:
    symbols = json.load(f)["items"]

results = []
errors = []

for item in symbols:
    symbol = item["symbol"]
    try:
        end_date = time.strftime("%Y-%m-%d", time.gmtime())
        start_days = 30
        candles_url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=compact&apikey={API_KEY}"
        candles_req = Request(candles_url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(candles_req, timeout=20) as resp:
            candle_data = json.loads(resp.read().decode("utf-8"))

        if "Error Message" in candle_data or "Note" in candle_data:
            errors.append({"symbol": symbol, "error": candle_data.get("Error Message", candle_data.get("Note", "Unknown error"))})
            continue

        ts_dict = candle_data.get("Time Series (Daily)", {})
        if not ts_dict:
            errors.append({"symbol": symbol, "error": "No time series data"})
            continue

        candles = [{"date": d, **v} for d, v in ts_dict.items()]
        latest = candles[0]
        recent = candles[1:min(21, len(candles))]

        latest_close = float(latest["4. close"])
        latest_volume = int(latest["5. volume"])
        latest_high = float(latest["2. high"])
        vol_atr = float(latest["3. low"])
        vol_open = float(latest["1. open"])
        
        vols = [int(c["5. volume"]) for c in recent if "5. volume" in c and c["5. volume"]]
        avg_vol = sum(vols) / len(vols) if vols else 0
        avg_dollar_vol = avg_vol * latest_close if avg_vol else 0
        
        closes = [float(c["4. close"]) for c in recent if "4. close" in c]
        prior_high = max(closes[:-1]) if len(closes) > 1 else latest_close

        try:
            news_url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={symbol}&time_from={time.strftime('%Y%m%dT%H%M', time.gmtime(time.time()-7*86400))}&limit=10&apikey={API_KEY}"
            news_req = Request(news_url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(news_req, timeout=15) as nresp:
                news_data = json.loads(nresp.read().decode("utf-8"))
            news_items = news_data.get("feed", [])
        except Exception as e:
            news_items = []

        def is_liquid(avg_dollar, threshold=10_000_000):
            return bool(avg_dollar and avg_dollar >= threshold)
        def has_catalyst(news, min_count=1):
            return len(news) >= min_count
        def is_breakout(close, ph, pct=0.02):
            return bool(close and ph and close >= ph * (1 + pct))
        def has_vol_spike(cur, avg, mult=2.0):
            return bool(cur and avg and cur >= avg * mult)

        liquid = is_liquid(avg_dollar_vol)
        catalyst = has_catalyst(news_items)
        breakout = is_breakout(latest_close, prior_high, 0.02)
        vol_spike_2x = has_vol_spike(latest_volume, avg_vol, 2.0)

        if liquid and catalyst and (breakout or vol_spike_2x):
            signal = "Buy"
        elif liquid:
            signal = "Hold"
        else:
            signal = "Sell"

        results.append({
            "company": item["company"],
            "ticker": item["ticker"],
            "symbol": symbol,
            "column": item["column"],
            "currentPrice": latest_close,
            "change": latest_close - float(candles[1]["4. close"]) if len(candles) > 1 else 0,
            "high": latest_high,
            "low": vol_atr,
            "open": vol_open,
            "volume": latest_volume,
            "avgVolume20": round(avg_vol),
            "avgDollarVolume20": round(avg_dollar_vol),
            "liquid": liquid,
            "catalyst": catalyst,
            "catalystHeadline": news_items[0]["title"][:80] if news_items else None,
            "breakout": breakout,
            "volumeSpike2x": vol_spike_2x,
            "signal": signal,
            "dataSource": "Alpha Vantage"
        })

        time.sleep(0.25)

    except HTTPError as e:
        errors.append({"symbol": symbol, "error": f"HTTP {e.code}"})
    except URLError as e:
        errors.append({"symbol": symbol, "error": f"Network: {e.reason}"})
    except Exception as e:
        errors.append({"symbol": symbol, "error": str(e)})

payload = {
    "updatedAt": int(time.time()),
    "source": "alpha_vantage",
    "settings": {
        "liquidityThreshold": 10_000_000,
        "breakoutWindow": 20,
        "newsLookbackDays": 7,
        "volumeSpikeMultiplier": 2.0
    },
    "items": results,
    "errors": errors
}

with open(QUOTES_FILE, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2)

print(f"Updated {len(results)} quotes, {len(errors)} errors"); print(f"Output: {QUOTES_FILE}")