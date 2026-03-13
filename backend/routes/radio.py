from fastapi import APIRouter
from services.radio_intercept import (find_nearest_openmhz_system,
                                      find_nearest_openmhz_systems_list,
                                      get_openmhz_systems,
                                      get_recent_openmhz_calls,
                                      get_top_broadcastify_feeds)

router = APIRouter(prefix="/api")


@router.get("/radio/top")
async def get_top_radios():
    return get_top_broadcastify_feeds()


@router.get("/radio/openmhz/systems")
async def api_get_openmhz_systems():
    return get_openmhz_systems()


@router.get("/radio/openmhz/calls/{sys_name}")
async def api_get_openmhz_calls(sys_name: str):
    return get_recent_openmhz_calls(sys_name)


@router.get("/radio/nearest")
async def api_get_nearest_radio(lat: float, lng: float):
    return find_nearest_openmhz_system(lat, lng)


@router.get("/radio/nearest-list")
async def api_get_nearest_radios_list(lat: float, lng: float, limit: int = 5):
    return find_nearest_openmhz_systems_list(lat, lng, limit=limit)
