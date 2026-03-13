import logging

logger = logging.getLogger(__name__)

KEY = "liveuamap"
TIER = "custom"
INTERVAL_HOURS = 12
DEFAULT = []
ENABLED = True


def fetch():
    try:
        from .scraper import fetch_liveuamap
        data = fetch_liveuamap()
        logger.info(f"LiveUAMap: {len(data) if data else 0} events")
        return data
    except Exception as e:
        logger.error(f"Error fetching LiveUAMap: {e}")
        return None