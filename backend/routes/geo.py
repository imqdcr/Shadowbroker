from fastapi import APIRouter
from services.region_dossier import get_region_dossier
from services.sentinel_search import search_sentinel2_scene

router = APIRouter(prefix="/api")


@router.get("/region-dossier")
def api_region_dossier(lat: float, lng: float):
    return get_region_dossier(lat, lng)


@router.get("/sentinel2/search")
def api_sentinel2_search(lat: float, lng: float):
    return search_sentinel2_scene(lat, lng)
