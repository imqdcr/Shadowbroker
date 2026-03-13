import concurrent.futures
import logging

import yfinance as yf

logger = logging.getLogger(__name__)

KEY = "oil"
TIER = "slow"
DEFAULT = {}


def _fetch_ticker(symbol: str, period: str = "5d"):
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period)
        if len(hist) >= 1:
            current_price = hist["Close"].iloc[-1]
            prev_close = hist["Close"].iloc[0] if len(hist) > 1 else current_price
            change_percent = ((current_price - prev_close) / prev_close) * 100 if prev_close else 0
            return {
                "price": round(float(current_price), 2),
                "change_percent": round(float(change_percent), 2),
                "up": bool(change_percent >= 0),
            }
    except Exception as e:
        logger.warning(f"Could not fetch ticker {symbol}: {e}")
    return None


def fetch():
    tickers = {"WTI Crude": "CL=F", "Brent Crude": "BZ=F"}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = pool.map(
                lambda item: (_fetch_ticker(item[1], "5d"), item[0]),
                tickers.items(),
            )
        oil_data = {name: data for data, name in results if data}
        return oil_data if oil_data else None
    except Exception as e:
        logger.error(f"Error fetching oil prices: {e}")
    return None