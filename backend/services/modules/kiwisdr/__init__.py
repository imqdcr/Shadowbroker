import logging

logger = logging.getLogger(__name__)

KEY = "kiwisdr"
TIER = "slow"
DEFAULT = []
ENABLED = True


def fetch():
    try:
        from .fetcher import fetch_kiwisdr_nodes
        nodes = fetch_kiwisdr_nodes()
        logger.info(f"KiwiSDR: {len(nodes) if nodes else 0} nodes")
        return nodes
    except Exception as e:
        logger.error(f"Error fetching KiwiSDR: {e}")
        return None