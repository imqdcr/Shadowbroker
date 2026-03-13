import logging

logger = logging.getLogger(__name__)

KEYS = ["frontlines", "gdelt"]
TIER = "custom"
INTERVAL_MINUTES = 30
DEFAULT = {"frontlines": None, "gdelt": []}
ENABLED = True


def fetch(current_state=None):
    from .fetcher import fetch_ukraine_frontlines, fetch_global_military_incidents
    result = {}
    try:
        result["frontlines"] = fetch_ukraine_frontlines()
    except Exception as e:
        logger.error(f"Geopolitics: frontlines error: {e}")
        result["frontlines"] = None
    try:
        result["gdelt"] = fetch_global_military_incidents()
    except Exception as e:
        logger.error(f"Geopolitics: GDELT error: {e}")
        result["gdelt"] = []
    return result