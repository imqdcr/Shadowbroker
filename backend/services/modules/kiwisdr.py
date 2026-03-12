import logging

logger = logging.getLogger(__name__)

KEY = "kiwisdr"
TIER = "slow"
DEFAULT = []


def fetch():
    try:
        from services.kiwisdr_fetcher import fetch_kiwisdr_nodes
        return fetch_kiwisdr_nodes()
    except Exception as e:
        logger.error(f"Error fetching KiwiSDR nodes: {e}")
    return None
