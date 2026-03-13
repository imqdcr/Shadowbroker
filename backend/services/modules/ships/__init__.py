import logging

logger = logging.getLogger(__name__)

KEY = "ships"
TIER = "fast"
DEFAULT = []
ENABLED = True


def fetch():
    from .ais_stream import get_ais_vessels
    from .carrier_tracker import get_carrier_positions
    try:
        vessels = get_ais_vessels()
        carriers = get_carrier_positions()
        combined = list(vessels) + list(carriers)
        logger.info(f"Ships: {len(vessels)} AIS vessels, {len(carriers)} carrier positions")
        return combined if combined else None
    except Exception as e:
        logger.error(f"Error fetching ships: {e}")
        return None