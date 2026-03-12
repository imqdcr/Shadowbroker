import logging
from services.network_utils import fetch_with_curl

logger = logging.getLogger(__name__)

KEY = "space_weather"
TIER = "slow"
DEFAULT = None


def fetch():
    try:
        kp_resp = fetch_with_curl("https://services.swpc.noaa.gov/json/planetary_k_index_1m.json", timeout=10)
        kp_value = None
        kp_text = "QUIET"
        if kp_resp.status_code == 200:
            kp_data = kp_resp.json()
            if kp_data:
                latest_kp = kp_data[-1]
                kp_value = float(latest_kp.get("kp_index", 0))
                if kp_value >= 7:
                    kp_text = f"STORM G{min(int(kp_value) - 4, 5)}"
                elif kp_value >= 5:
                    kp_text = f"STORM G{min(int(kp_value) - 4, 5)}"
                elif kp_value >= 4:
                    kp_text = "ACTIVE"
                elif kp_value >= 3:
                    kp_text = "UNSETTLED"

        events = []
        ev_resp = fetch_with_curl("https://services.swpc.noaa.gov/json/edited_events.json", timeout=10)
        if ev_resp.status_code == 200:
            all_events = ev_resp.json()
            for ev in all_events[-10:]:
                events.append({
                    "type": ev.get("type", ""),
                    "begin": ev.get("begin", ""),
                    "end": ev.get("end", ""),
                    "classtype": ev.get("classtype", ""),
                })

        logger.info(f"Space weather: Kp={kp_value} ({kp_text}), {len(events)} events")
        return {
            "kp_index": kp_value,
            "kp_text": kp_text,
            "events": events,
        }
    except Exception as e:
        logger.error(f"Error fetching space weather: {e}")
    return None
