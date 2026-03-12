import logging

logger = logging.getLogger(__name__)

KEYS = ["frontlines", "gdelt"]
TIER = "custom"
INTERVAL_MINUTES = 30
DEFAULT = {"frontlines": None, "gdelt": []}


def fetch(current_state=None):
    from services.geopolitics import fetch_ukraine_frontlines, fetch_global_military_incidents
    result = {}
    try:
        logger.info("Fetching Geopolitics data...")
        frontlines = fetch_ukraine_frontlines()
        if frontlines:
            result["frontlines"] = frontlines

        gdelt = fetch_global_military_incidents()
        if gdelt is not None:
            result["gdelt"] = gdelt
    except Exception as e:
        logger.error(f"Error fetching geopolitics: {e}")
    return result
