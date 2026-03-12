import logging

logger = logging.getLogger(__name__)

KEY = "ships"
TIER = "fast"
DEFAULT = []


def fetch():
    from services.ais_stream import get_ais_vessels
    from services.carrier_tracker import get_carrier_positions
    carriers = get_carrier_positions()
    ais_vessels = get_ais_vessels()
    ships = carriers + ais_vessels
    logger.info(f"Ships: {len(carriers)} carriers + {len(ais_vessels)} AIS vessels")
    return ships
