import csv
import heapq
import io
import logging

from services.network_utils import fetch_with_curl

logger = logging.getLogger(__name__)

KEY = "firms_fires"
TIER = "slow"
DEFAULT = []


def fetch():
    fires = []
    try:
        url = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_Global_24h.csv"
        response = fetch_with_curl(url, timeout=30)
        if response.status_code == 200:
            reader = csv.DictReader(io.StringIO(response.text))
            all_rows = []
            for row in reader:
                try:
                    lat = float(row.get("latitude", 0))
                    lng = float(row.get("longitude", 0))
                    frp = float(row.get("frp", 0))
                    conf = row.get("confidence", "nominal")
                    daynight = row.get("daynight", "")
                    bright = float(row.get("bright_ti4", 0))
                    all_rows.append({
                        "lat": lat,
                        "lng": lng,
                        "frp": frp,
                        "brightness": bright,
                        "confidence": conf,
                        "daynight": daynight,
                        "acq_date": row.get("acq_date", ""),
                        "acq_time": row.get("acq_time", ""),
                    })
                except (ValueError, TypeError):
                    continue
            fires = heapq.nlargest(5000, all_rows, key=lambda x: x["frp"])
        logger.info(f"FIRMS fires: {len(fires)} hotspots (from {response.status_code})")
    except Exception as e:
        logger.error(f"Error fetching FIRMS fires: {e}")
    return fires if fires else None
