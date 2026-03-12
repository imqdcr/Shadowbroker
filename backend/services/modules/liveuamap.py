import logging

logger = logging.getLogger(__name__)

KEY = "liveuamap"
TIER = "custom"
INTERVAL_HOURS = 12
DEFAULT = []


def fetch():
    try:
        from services.liveuamap_scraper import fetch_liveuamap
        result = fetch_liveuamap()
        logger.info(f"Liveuamap: {len(result) if result else 0} events")
        return result if result else None
    except Exception as e:
        logger.error(f"Liveuamap scraper error: {e}")
    return None
