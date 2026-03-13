from fastapi import APIRouter
from services.network_utils import fetch_with_curl

router = APIRouter(prefix="/api")


@router.get("/route/{callsign}")
async def get_flight_route(callsign: str, lat: float = 0.0, lng: float = 0.0):
    r = fetch_with_curl(
        "https://api.adsb.lol/api/0/routeset",
        method="POST",
        json_data={"planes": [{"callsign": callsign, "lat": lat, "lng": lng}]},
        timeout=10,
    )
    if r and r.status_code == 200:
        data = r.json()
        route_list = []
        if isinstance(data, dict):
            route_list = data.get("value", [])
        elif isinstance(data, list):
            route_list = data
        if route_list and len(route_list) > 0:
            route = route_list[0]
            airports = route.get("_airports", [])
            if len(airports) >= 2:
                orig = airports[0]
                dest = airports[-1]
                return {
                    "orig_loc": [orig.get("lon", 0), orig.get("lat", 0)],
                    "dest_loc": [dest.get("lon", 0), dest.get("lat", 0)],
                    "origin_name": f"{orig.get('iata', '') or orig.get('icao', '')}: {orig.get('name', 'Unknown')}",
                    "dest_name": f"{dest.get('iata', '') or dest.get('icao', '')}: {dest.get('name', 'Unknown')}",
                }
    return {}
