import logging
import math
import re
import time
from datetime import datetime

from sgp4.api import Satrec, WGS72, jday

from services.network_utils import fetch_with_curl
from .propagator import SAT_INTEL_DB, gmst, fetch_from_tle_api

logger = logging.getLogger(__name__)

KEY = "satellites"
TIER = "fast"
DEFAULT = []
ENABLED = True

_sat_gp_cache: dict = {"data": None, "last_fetch": 0}


def fetch():
    sats = []
    try:
        now_ts = time.time()
        if _sat_gp_cache["data"] is None or (now_ts - _sat_gp_cache["last_fetch"]) > 1800:
            for url in [
                "https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=json",
                "https://celestrak.com/NORAD/elements/gp.php?GROUP=active&FORMAT=json",
            ]:
                try:
                    response = fetch_with_curl(url, timeout=8)
                    if response.status_code == 200:
                        gp_data = response.json()
                        if isinstance(gp_data, list) and len(gp_data) > 100:
                            _sat_gp_cache["data"] = gp_data
                            _sat_gp_cache["last_fetch"] = now_ts
                            logger.info(f"Satellites: Downloaded {len(gp_data)} GP records from {url}")
                            break
                except Exception as e:
                    logger.warning(f"Satellites: Failed to fetch from {url}: {e}")

            if _sat_gp_cache["data"] is None:
                logger.info("Satellites: CelesTrak unreachable, trying TLE fallback API...")
                try:
                    fallback_data = fetch_from_tle_api()
                    if fallback_data and len(fallback_data) > 10:
                        _sat_gp_cache["data"] = fallback_data
                        _sat_gp_cache["last_fetch"] = now_ts
                        logger.info(f"Satellites: Got {len(fallback_data)} records from TLE fallback API")
                except Exception as e:
                    logger.error(f"Satellites: TLE fallback also failed: {e}")

        data = _sat_gp_cache["data"]
        if not data:
            logger.warning("No satellite GP data available from any source")
            return None

        classified = []
        for sat in data:
            name = sat.get("OBJECT_NAME", "UNKNOWN").upper()
            intel = None
            for key_str, meta in SAT_INTEL_DB:
                if key_str.upper() in name:
                    intel = dict(meta)
                    break
            if not intel:
                continue
            entry = {
                "id": sat.get("NORAD_CAT_ID"),
                "name": sat.get("OBJECT_NAME", "UNKNOWN"),
                "MEAN_MOTION": sat.get("MEAN_MOTION"),
                "ECCENTRICITY": sat.get("ECCENTRICITY"),
                "INCLINATION": sat.get("INCLINATION"),
                "RA_OF_ASC_NODE": sat.get("RA_OF_ASC_NODE"),
                "ARG_OF_PERICENTER": sat.get("ARG_OF_PERICENTER"),
                "MEAN_ANOMALY": sat.get("MEAN_ANOMALY"),
                "BSTAR": sat.get("BSTAR"),
                "EPOCH": sat.get("EPOCH"),
            }
            entry.update(intel)
            classified.append(entry)

        logger.info(f"Satellites: {len(classified)} intel-classified out of {len(data)} total")

        now = datetime.utcnow()
        jd, fr = jday(now.year, now.month, now.day, now.hour, now.minute,
                      now.second + now.microsecond / 1e6)

        for s in classified:
            try:
                mean_motion = s.get("MEAN_MOTION")
                ecc = s.get("ECCENTRICITY")
                incl = s.get("INCLINATION")
                raan = s.get("RA_OF_ASC_NODE")
                argp = s.get("ARG_OF_PERICENTER")
                ma = s.get("MEAN_ANOMALY")
                bstar = s.get("BSTAR", 0)
                epoch_str = s.get("EPOCH")
                norad_id = s.get("id", 0)

                if mean_motion is None or ecc is None or incl is None:
                    continue

                epoch_dt = datetime.strptime(epoch_str[:19], "%Y-%m-%dT%H:%M:%S")
                epoch_jd, epoch_fr = jday(epoch_dt.year, epoch_dt.month, epoch_dt.day,
                                          epoch_dt.hour, epoch_dt.minute, epoch_dt.second)

                sat_obj = Satrec()
                sat_obj.sgp4init(
                    WGS72, "i", norad_id,
                    (epoch_jd + epoch_fr) - 2433281.5,
                    bstar, 0.0, 0.0, ecc,
                    math.radians(argp), math.radians(incl),
                    math.radians(ma),
                    mean_motion * 2 * math.pi / 1440.0,
                    math.radians(raan),
                )

                e, r, v = sat_obj.sgp4(jd, fr)
                if e != 0:
                    continue

                x, y, z = r
                g = gmst(jd + fr)
                lng_rad = math.atan2(y, x) - g
                lat_rad = math.atan2(z, math.sqrt(x * x + y * y))
                alt_km = math.sqrt(x * x + y * y + z * z) - 6371.0

                s["lat"] = round(math.degrees(lat_rad), 4)
                lng_deg = math.degrees(lng_rad) % 360
                s["lng"] = round(lng_deg - 360 if lng_deg > 180 else lng_deg, 4)
                s["alt_km"] = round(alt_km, 1)

                vx, vy, vz = v
                omega_e = 7.2921159e-5
                vx_g = vx + omega_e * y
                vy_g = vy - omega_e * x
                vz_g = vz
                cos_lat = math.cos(lat_rad)
                sin_lat = math.sin(lat_rad)
                cos_lng = math.cos(lng_rad + g)
                sin_lng = math.sin(lng_rad + g)
                v_east = -sin_lng * vx_g + cos_lng * vy_g
                v_north = (-sin_lat * cos_lng * vx_g - sin_lat * sin_lng * vy_g
                           + cos_lat * vz_g)
                ground_speed_kms = math.sqrt(v_east ** 2 + v_north ** 2)
                s["speed_knots"] = round(ground_speed_kms * 1943.84, 1)
                heading_rad = math.atan2(v_east, v_north)
                s["heading"] = round(math.degrees(heading_rad) % 360, 1)

                usa_match = re.search(r"USA[\s\-]*(\d+)", s.get("name", ""))
                if usa_match:
                    s["wiki"] = f"https://en.wikipedia.org/wiki/USA-{usa_match.group(1)}"

                for k in ("MEAN_MOTION", "ECCENTRICITY", "INCLINATION",
                          "RA_OF_ASC_NODE", "ARG_OF_PERICENTER", "MEAN_ANOMALY",
                          "BSTAR", "EPOCH", "tle1", "tle2"):
                    s.pop(k, None)
                sats.append(s)
            except Exception:
                continue

        logger.info(f"Satellites: {len(classified)} classified, {len(sats)} positioned")
    except Exception as e:
        logger.error(f"Error fetching satellites: {e}")

    return sats if sats else None
