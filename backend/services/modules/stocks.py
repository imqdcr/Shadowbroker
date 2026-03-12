import concurrent.futures
import logging

import yfinance as yf

logger = logging.getLogger(__name__)

KEY = "stocks"
TIER = "slow"
DEFAULT = {}


def _fetch_ticker(symbol: str, period: str = "2d"):
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period)
        if len(hist) >= 1:
            current_price = hist["Close"].iloc[-1]
            prev_close = hist["Close"].iloc[0] if len(hist) > 1 else current_price
            change_percent = ((current_price - prev_close) / prev_close) * 100 if prev_close else 0
            return symbol, {
                "price": round(float(current_price), 2),
                "change_percent": round(float(change_percent), 2),
                "up": bool(change_percent >= 0),
            }
    except Exception as e:
        logger.warning(f"Could not fetch data for {symbol}: {e}")
    return symbol, None


def fetch():
    tickers = ["RTX", "LMT", "NOC", "GD", "BA", "PLTR"]
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results = pool.map(lambda t: _fetch_ticker(t, "2d"), tickers)
        stocks_data = {sym: data for sym, data in results if data}
        return stocks_data if stocks_data else None
    except Exception as e:
        logger.error(f"Error fetching stocks: {e}")
    return None
