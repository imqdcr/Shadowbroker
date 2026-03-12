import logging
from services.network_utils import fetch_with_curl

logger = logging.getLogger(__name__)

KEY = "weather"
TIER = "slow"
DEFAULT = None


def fetch():
    try:
        url = "https://api.rainviewer.com/public/weather-maps.json"
        response = fetch_with_curl(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if "radar" in data and "past" in data["radar"]:
                latest_time = data["radar"]["past"][-1]["time"]
                return {"time": latest_time, "host": data.get("host", "https://tilecache.rainviewer.com")}
    except Exception as e:
        logger.error(f"Error fetching weather: {e}")
    return None
