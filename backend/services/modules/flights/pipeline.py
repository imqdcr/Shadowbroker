"""
pipeline.py — Flight data processing pipeline.

Steps:
  1. categorise_civilian()  — split raw ADS-B into commercial / private_jet /
                               private_ga / tracked (via plane-alert DB)
  2. process_military()     — split military raw into military / uavs
  3. merge_tracked()        — unify civilian + military tracked lists
  4. smart_merge()          — protect against partial API failures (>50% drop)
  5. accumulate_trails()    — build per-aircraft position history
  6. detect_jamming()       — GPS/GNSS degradation zones from NACp field
  7. detect_holding()       — flag aircraft circling in holding patterns
"""

import logging
import math
import re
import threading
import time
from datetime import datetime

from .enrichment import enrich_with_plane_alert, enrich_with_tracked_names
from .routes import dynamic_routes_cache, routes_fetch_in_progress, fetch_routes_background
from .state import (
    flight_trails, _trails_lock, _MAX_TRACKED_TRAILS,
    _RE_AIRLINE_CODE_1, _RE_AIRLINE_CODE_2, _HELI_TYPES_BACKEND,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Aircraft type sets
# ---------------------------------------------------------------------------
PRIVATE_JET_TYPES = {
    # Gulfstream
    "G150", "G200", "G280", "GLEX", "G500", "G550", "G600", "G650", "G700",
    "GLF2", "GLF3", "GLF4", "GLF5", "GLF6", "GL5T", "GL7T", "GV", "GIV",
    # Bombardier
    "CL30", "CL35", "CL60", "BD70", "BD10",
    "CRJ1", "CRJ2",
    # Cessna Citation
    "C25A", "C25B", "C25C", "C500", "C501", "C510", "C525", "C526",
    "C550", "C560", "C56X", "C680", "C68A", "C700", "C750",
    # Dassault Falcon
    "FA10", "FA20", "FA50", "FA7X", "FA8X", "F900", "F2TH", "ASTR",
    # Embraer Business Jets
    "E35L", "E545", "E550", "E55P", "LEGA", "PH10", "PH30",
    # Learjet
    "LJ23", "LJ24", "LJ25", "LJ28", "LJ31", "LJ35", "LJ36",
    "LJ40", "LJ45", "LJ55", "LJ60", "LJ70", "LJ75",
    # Hawker / Beechcraft
    "H25A", "H25B", "H25C", "HA4T", "BE40", "PRM1",
    # Other
    "HDJT", "PC24", "EA50", "SF50", "GALX",
}

_UAV_TYPE_CODES = {"Q9", "R4", "TB2", "MALE", "HALE", "HERM", "HRON"}
_UAV_CALLSIGN_PREFIXES = ("FORTE", "GHAWK", "REAP", "BAMS", "UAV", "UAS")
_UAV_MODEL_KEYWORDS = (
    "RQ-", "MQ-", "RQ4", "MQ9", "MQ4", "MQ1",
    "REAPER", "GLOBALHAWK", "TRITON", "PREDATOR", "HERMES", "HERON", "BAYRAKTAR",
)
_UAV_WIKI = {
    "RQ4":       "https://en.wikipedia.org/wiki/Northrop_Grumman_RQ-4_Global_Hawk",
    "RQ-4":      "https://en.wikipedia.org/wiki/Northrop_Grumman_RQ-4_Global_Hawk",
    "MQ4":       "https://en.wikipedia.org/wiki/Northrop_Grumman_MQ-4C_Triton",
    "MQ-4":      "https://en.wikipedia.org/wiki/Northrop_Grumman_MQ-4C_Triton",
    "MQ9":       "https://en.wikipedia.org/wiki/General_Atomics_MQ-9_Reaper",
    "MQ-9":      "https://en.wikipedia.org/wiki/General_Atomics_MQ-9_Reaper",
    "MQ1":       "https://en.wikipedia.org/wiki/General_Atomics_MQ-1C_Gray_Eagle",
    "MQ-1":      "https://en.wikipedia.org/wiki/General_Atomics_MQ-1C_Gray_Eagle",
    "REAPER":    "https://en.wikipedia.org/wiki/General_Atomics_MQ-9_Reaper",
    "GLOBALHAWK":"https://en.wikipedia.org/wiki/Northrop_Grumman_RQ-4_Global_Hawk",
    "TRITON":    "https://en.wikipedia.org/wiki/Northrop_Grumman_MQ-4C_Triton",
    "PREDATOR":  "https://en.wikipedia.org/wiki/General_Atomics_MQ-1_Predator",
    "HERMES":    "https://en.wikipedia.org/wiki/Elbit_Hermes_900",
    "HERON":     "https://en.wikipedia.org/wiki/IAI_Heron",
    "BAYRAKTAR": "https://en.wikipedia.org/wiki/Bayraktar_TB2",
}


def _classify_uav(model: str, callsign: str) -> tuple:
    """Return (is_uav, uav_type, wiki_url) or (False, None, None)."""
    model_up = model.upper().replace(" ", "")
    callsign_up = callsign.upper().strip()

    if model_up in _UAV_TYPE_CODES:
        uav_type = "HALE Surveillance" if model_up in ("R4", "HALE") else "MALE ISR"
        return True, uav_type, _UAV_WIKI.get(model_up, "")

    for prefix in _UAV_CALLSIGN_PREFIXES:
        if callsign_up.startswith(prefix):
            uav_type = "HALE Surveillance" if prefix in ("FORTE", "GHAWK", "BAMS") else "MALE ISR"
            wiki = _UAV_WIKI.get(prefix, "")
            if prefix == "FORTE":
                wiki = _UAV_WIKI["RQ4"]
            elif prefix == "BAMS":
                wiki = _UAV_WIKI["MQ4"]
            return True, uav_type, wiki

    for kw in _UAV_MODEL_KEYWORDS:
        if kw in model_up:
            if any(h in model_up for h in ("RQ4", "RQ-4", "GLOBALHAWK")):
                return True, "HALE Surveillance", _UAV_WIKI.get(kw, "")
            elif any(h in model_up for h in ("MQ4", "MQ-4", "TRITON")):
                return True, "HALE Maritime Surveillance", _UAV_WIKI.get(kw, "")
            elif any(h in model_up for h in ("MQ9", "MQ-9", "REAPER")):
                return True, "MALE Strike/ISR", _UAV_WIKI.get(kw, "")
            elif any(h in model_up for h in ("MQ1", "MQ-1", "PREDATOR")):
                return True, "MALE ISR/Strike", _UAV_WIKI.get(kw, "")
            elif "BAYRAKTAR" in model_up or "TB2" in model_up:
                return True, "MALE Strike", _UAV_WIKI.get("BAYRAKTAR", "")
            elif "HERMES" in model_up:
                return True, "MALE ISR", _UAV_WIKI.get("HERMES", "")
            elif "HERON" in model_up:
                return True, "MALE ISR", _UAV_WIKI.get("HERON", "")
            return True, "MALE ISR", _UAV_WIKI.get(kw, "")

    return False, None, None


# ---------------------------------------------------------------------------
# Main pipeline entry point
# ---------------------------------------------------------------------------

def process(civilian_raw: list, military_raw: list, current_state: dict) -> dict:
    """
    Run the full processing pipeline.  Returns a dict keyed by all KEYS
    declared in flights/__init__.py.
    """
    flights, commercial, private_jets, private_ga, tracked_civ = _categorise_civilian(civilian_raw)
    military, uavs, tracked_mil = _process_military(military_raw, current_state)

    tracked = _merge_tracked(tracked_civ, tracked_mil, current_state)

    result = _smart_merge(commercial, private_jets, private_ga, tracked, military, uavs, flights, current_state)

    _accumulate_trails(result)
    result["gps_jamming"] = _detect_jamming(result.get("flights", []))
    _detect_holding(result)

    return result


# ---------------------------------------------------------------------------
# Step 1 — Categorise civilian raw ADS-B
# ---------------------------------------------------------------------------

def _categorise_civilian(raw: list) -> tuple:
    """Return (all_flights, commercial, private_jets, private_ga, tracked)."""
    flights = []
    commercial = []
    private_jets = []
    private_ga = []
    tracked = []

    if not routes_fetch_in_progress and raw:
        threading.Thread(target=fetch_routes_background, args=(raw,), daemon=True).start()

    for f in raw:
        try:
            lat = f.get("lat")
            lng = f.get("lon")
            if lat is None or lng is None:
                continue

            model_upper = f.get("t", "").upper()
            if model_upper == "TWR":
                continue

            flight_str = str(f.get("flight", "UNKNOWN")).strip() or str(f.get("hex", "Unknown"))

            origin_loc, dest_loc = None, None
            origin_name, dest_name = "UNKNOWN", "UNKNOWN"
            if flight_str in dynamic_routes_cache:
                cached = dynamic_routes_cache[flight_str]
                origin_name = cached["orig_name"]
                dest_name = cached["dest_name"]
                origin_loc = cached["orig_loc"]
                dest_loc = cached["dest_loc"]

            airline_code = ""
            match = _RE_AIRLINE_CODE_1.match(flight_str)
            if not match:
                match = _RE_AIRLINE_CODE_2.match(flight_str)
            if match:
                airline_code = match.group(1)

            alt_raw = f.get("alt_baro")
            alt_value = (alt_raw * 0.3048) if isinstance(alt_raw, (int, float)) else 0
            gs_knots = f.get("gs")
            speed_knots = round(gs_knots, 1) if isinstance(gs_knots, (int, float)) else None
            ac_category = "heli" if model_upper in _HELI_TYPES_BACKEND else "plane"

            entry = {
                "callsign": flight_str,
                "country": f.get("r", "N/A"),
                "lng": float(lng),
                "lat": float(lat),
                "alt": alt_value,
                "heading": f.get("track") or 0,
                "type": "flight",
                "origin_loc": origin_loc,
                "dest_loc": dest_loc,
                "origin_name": origin_name,
                "dest_name": dest_name,
                "registration": f.get("r", "N/A"),
                "model": f.get("t", "Unknown"),
                "icao24": f.get("hex", ""),
                "speed_knots": speed_knots,
                "squawk": f.get("squawk", ""),
                "airline_code": airline_code,
                "aircraft_category": ac_category,
                "nac_p": f.get("nac_p"),
            }
            flights.append(entry)

            enrich_with_plane_alert(entry)
            enrich_with_tracked_names(entry)

            callsign = entry.get("callsign", "").strip().upper()
            is_commercial_format = bool(re.match(r'^[A-Z]{3}\d{1,4}[A-Z]{0,2}$', callsign))

            if entry.get("alert_category"):
                entry["type"] = "tracked_flight"
                tracked.append(entry)
            elif entry.get("airline_code") or is_commercial_format:
                entry["type"] = "commercial_flight"
                commercial.append(entry)
            elif entry.get("model", "").upper() in PRIVATE_JET_TYPES:
                entry["type"] = "private_jet"
                private_jets.append(entry)
            else:
                entry["type"] = "private_ga"
                private_ga.append(entry)

        except Exception as e:
            logger.error(f"Civilian categorise error: {e}")

    return flights, commercial, private_jets, private_ga, tracked


# ---------------------------------------------------------------------------
# Step 2 — Process military raw
# ---------------------------------------------------------------------------

def _process_military(raw: list, current_state: dict) -> tuple:
    """Return (military_flights, uavs, tracked_from_military)."""
    military = []
    uavs = []
    tracked_mil = []

    for f in raw:
        try:
            lat = f.get("lat")
            lng = f.get("lon")
            if lat is None or lng is None:
                continue

            model = str(f.get("t", "UNKNOWN")).upper()
            if model == "TWR":
                continue

            callsign = str(f.get("flight", "MIL-UNKN")).strip()
            alt_raw = f.get("alt_baro")
            alt_value = (alt_raw * 0.3048) if isinstance(alt_raw, (int, float)) else 0
            gs_knots = f.get("gs")
            speed_knots = round(gs_knots, 1) if isinstance(gs_knots, (int, float)) else None

            is_uav, uav_type, wiki_url = _classify_uav(model, callsign)
            if is_uav:
                uavs.append({
                    "id": f"uav-{f.get('hex', '')}",
                    "callsign": callsign,
                    "aircraft_model": f.get("t", "Unknown"),
                    "lat": float(lat),
                    "lng": float(lng),
                    "alt": alt_value,
                    "heading": f.get("track") or 0,
                    "speed_knots": speed_knots,
                    "country": f.get("r", "Unknown"),
                    "uav_type": uav_type,
                    "wiki": wiki_url or "",
                    "type": "uav",
                    "registration": f.get("r", "N/A"),
                    "icao24": f.get("hex", ""),
                    "squawk": f.get("squawk", ""),
                })
                continue

            mil_cat = "default"
            if "H" in model and any(c.isdigit() for c in model):
                mil_cat = "heli"
            elif any(k in model for k in ["K35", "K46", "A33"]):
                mil_cat = "tanker"
            elif any(k in model for k in ["F16", "F35", "F22", "F15", "F18", "T38", "T6", "A10"]):
                mil_cat = "fighter"
            elif any(k in model for k in ["C17", "C5", "C130", "C30", "A400", "V22"]):
                mil_cat = "cargo"
            elif any(k in model for k in ["P8", "E3", "E8", "U2"]):
                mil_cat = "recon"

            mf = {
                "callsign": callsign,
                "country": f.get("r", "Military Asset"),
                "lng": float(lng),
                "lat": float(lat),
                "alt": alt_value,
                "heading": f.get("track") or 0,
                "type": "military_flight",
                "military_type": mil_cat,
                "origin_loc": None,
                "dest_loc": None,
                "origin_name": "UNKNOWN",
                "dest_name": "UNKNOWN",
                "registration": f.get("r", "N/A"),
                "model": f.get("t", "Unknown"),
                "icao24": f.get("hex", ""),
                "speed_knots": speed_knots,
                "squawk": f.get("squawk", ""),
            }
            enrich_with_plane_alert(mf)
            if mf.get("alert_category"):
                mf["type"] = "tracked_flight"
                tracked_mil.append(mf)
            else:
                military.append(mf)

        except Exception as e:
            logger.error(f"Military process error: {e}")

    if not military and not uavs:
        logger.warning("No military flights retrieved — keeping previous data if available")

    logger.info(f"UAVs: {len(uavs)} detected. Military: {len(military)}. Tracked from mil: {len(tracked_mil)}")
    return military, uavs, tracked_mil


# ---------------------------------------------------------------------------
# Step 3 — Merge tracked lists
# ---------------------------------------------------------------------------

def _merge_tracked(tracked_civ: list, tracked_mil: list, current_state: dict) -> list:
    existing = current_state.get("tracked_flights", [])

    # Build fresh map from this cycle
    fresh_map = {}
    for t in tracked_civ + tracked_mil:
        icao = t.get("icao24", "").upper()
        if icao:
            fresh_map[icao] = t

    merged = []
    seen = set()
    for old_t in existing:
        icao = old_t.get("icao24", "").upper()
        if icao in fresh_map:
            fresh = fresh_map[icao]
            for key in ("alert_category", "alert_operator", "alert_special", "alert_flag"):
                if key in old_t and key not in fresh:
                    fresh[key] = old_t[key]
            merged.append(fresh)
        else:
            merged.append(old_t)
        seen.add(icao)

    for icao, t in fresh_map.items():
        if icao not in seen:
            merged.append(t)

    logger.info(f"Tracked flights: {len(merged)} total ({len(fresh_map)} fresh)")
    return merged


# ---------------------------------------------------------------------------
# Step 4 — Smart merge (protect against partial API failures)
# ---------------------------------------------------------------------------

def _smart_merge(commercial, private_jets, private_ga, tracked, military, uavs,
                 flights, current_state: dict) -> dict:
    now = time.time()
    prev_commercial = len(current_state.get("commercial_flights", []))
    prev_total = (prev_commercial
                  + len(current_state.get("private_jets", []))
                  + len(current_state.get("private_flights", [])))
    new_total = len(commercial) + len(private_jets) + len(private_ga)

    def _merge_cat(new_list, old_list, max_stale_s=120):
        by_icao = {}
        for f in old_list:
            icao = f.get("icao24", "")
            if icao:
                f.setdefault("_seen_at", now)
                if (now - f.get("_seen_at", now)) < max_stale_s:
                    by_icao[icao] = f
        for f in new_list:
            icao = f.get("icao24", "")
            if icao:
                f["_seen_at"] = now
                by_icao[icao] = f
            else:
                by_icao[id(f)] = f
        return list(by_icao.values())

    result = {
        "military_flights": military,
        "uavs": uavs,
        "tracked_flights": tracked,
        "flights": flights,
    }

    if new_total == 0:
        logger.warning("No civilian flights — skipping overwrite to protect the map.")
        result["commercial_flights"] = current_state.get("commercial_flights", [])
        result["private_jets"] = current_state.get("private_jets", [])
        result["private_flights"] = current_state.get("private_flights", [])
    elif prev_total > 100 and new_total < prev_total * 0.5:
        logger.warning(f"Flight count dropped {prev_total}→{new_total} (>50%). Keeping previous.")
        result["commercial_flights"] = current_state.get("commercial_flights", [])
        result["private_jets"] = current_state.get("private_jets", [])
        result["private_flights"] = current_state.get("private_flights", [])
    else:
        result["commercial_flights"] = _merge_cat(commercial, current_state.get("commercial_flights", []))
        result["private_jets"] = _merge_cat(private_jets, current_state.get("private_jets", []))
        result["private_flights"] = _merge_cat(private_ga, current_state.get("private_flights", []))

    return result


# ---------------------------------------------------------------------------
# Step 5 — Trail accumulation
# ---------------------------------------------------------------------------

def _accumulate_trail(f: dict, now_ts: float, check_route: bool = True) -> tuple:
    hex_id = f.get("icao24", "").lower()
    if not hex_id:
        return 0, None
    if check_route and f.get("origin_name", "UNKNOWN") != "UNKNOWN":
        f["trail"] = []
        return 0, hex_id
    lat, lng, alt = f.get("lat"), f.get("lng"), f.get("alt", 0)
    if lat is None or lng is None:
        f["trail"] = flight_trails.get(hex_id, {}).get("points", [])
        return 0, hex_id
    point = [round(lat, 5), round(lng, 5), round(alt, 1), round(now_ts)]
    if hex_id not in flight_trails:
        flight_trails[hex_id] = {"points": [], "last_seen": now_ts}
    td = flight_trails[hex_id]
    if td["points"] and td["points"][-1][0] == point[0] and td["points"][-1][1] == point[1]:
        td["last_seen"] = now_ts
    else:
        td["points"].append(point)
        td["last_seen"] = now_ts
    if len(td["points"]) > 200:
        td["points"] = td["points"][-200:]
    f["trail"] = td["points"]
    return 1, hex_id


def _accumulate_trails(result: dict) -> None:
    now_ts = datetime.utcnow().timestamp()
    seen_hexes: set = set()
    trail_count = 0
    stale_keys = []

    with _trails_lock:
        for flist in (result.get("commercial_flights", []),
                      result.get("private_jets", []),
                      result.get("private_flights", []),
                      result.get("tracked_flights", [])):
            for f in flist:
                count, hex_id = _accumulate_trail(f, now_ts, check_route=True)
                trail_count += count
                if hex_id:
                    seen_hexes.add(hex_id)

        for mf in result.get("military_flights", []):
            count, hex_id = _accumulate_trail(mf, now_ts, check_route=False)
            trail_count += count
            if hex_id:
                seen_hexes.add(hex_id)

        tracked_hexes = {t.get("icao24", "").lower() for t in result.get("tracked_flights", [])}
        for k, v in flight_trails.items():
            cutoff = now_ts - 1800 if k in tracked_hexes else now_ts - 600
            if v["last_seen"] < cutoff:
                stale_keys.append(k)
        for k in stale_keys:
            del flight_trails[k]

        if len(flight_trails) > _MAX_TRACKED_TRAILS:
            sorted_keys = sorted(flight_trails.keys(), key=lambda k: flight_trails[k]["last_seen"])
            for k in sorted_keys[:len(flight_trails) - _MAX_TRACKED_TRAILS]:
                del flight_trails[k]

    logger.info(f"Trails: {trail_count} active, {len(stale_keys)} pruned, {len(flight_trails)} total")


# ---------------------------------------------------------------------------
# Step 6 — GPS/GNSS jamming detection
# ---------------------------------------------------------------------------

def _detect_jamming(raw_flights: list) -> list:
    jamming_grid: dict = {}
    for rf in raw_flights:
        rlat = rf.get("lat")
        rlng = rf.get("lng") or rf.get("lon")
        if rlat is None or rlng is None:
            continue
        nacp = rf.get("nac_p")
        if nacp is None:
            continue
        grid_key = f"{int(rlat)},{int(rlng)}"
        if grid_key not in jamming_grid:
            jamming_grid[grid_key] = {"degraded": 0, "total": 0}
        jamming_grid[grid_key]["total"] += 1
        if nacp < 8:
            jamming_grid[grid_key]["degraded"] += 1

    zones = []
    for gk, counts in jamming_grid.items():
        if counts["total"] < 3:
            continue
        ratio = counts["degraded"] / counts["total"]
        if ratio > 0.25:
            lat_i, lng_i = gk.split(",")
            severity = "low" if ratio < 0.5 else "medium" if ratio < 0.75 else "high"
            zones.append({
                "lat": int(lat_i) + 0.5,
                "lng": int(lng_i) + 0.5,
                "severity": severity,
                "ratio": round(ratio, 2),
                "degraded": counts["degraded"],
                "total": counts["total"],
            })
    if zones:
        logger.info(f"GPS Jamming: {len(zones)} zones detected")
    return zones


# ---------------------------------------------------------------------------
# Step 7 — Holding pattern detection
# ---------------------------------------------------------------------------

def _detect_holding(result: dict) -> None:
    all_lists = [
        result.get("commercial_flights", []),
        result.get("private_jets", []),
        result.get("private_flights", []),
        result.get("tracked_flights", []),
        result.get("military_flights", []),
    ]
    holding_count = 0
    for flist in all_lists:
        for f in flist:
            hex_id = f.get("icao24", "").lower()
            trail = flight_trails.get(hex_id, {}).get("points", [])
            if len(trail) < 6:
                f["holding"] = False
                continue
            pts = trail[-8:]
            total_turn = 0.0
            prev_bearing = 0.0
            for i in range(1, len(pts)):
                lat1, lng1 = math.radians(pts[i-1][0]), math.radians(pts[i-1][1])
                lat2, lng2 = math.radians(pts[i][0]), math.radians(pts[i][1])
                dlng = lng2 - lng1
                x = math.sin(dlng) * math.cos(lat2)
                y = (math.cos(lat1) * math.sin(lat2)
                     - math.sin(lat1) * math.cos(lat2) * math.cos(dlng))
                bearing = math.degrees(math.atan2(x, y)) % 360
                if i > 1:
                    delta = abs(bearing - prev_bearing)
                    if delta > 180:
                        delta = 360 - delta
                    total_turn += delta
                prev_bearing = bearing
            f["holding"] = total_turn > 300
            if f["holding"]:
                holding_count += 1
    if holding_count:
        logger.info(f"Holding patterns: {holding_count} aircraft circling")
