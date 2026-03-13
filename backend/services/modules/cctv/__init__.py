import logging

logger = logging.getLogger(__name__)

KEY = "cctv"
TIER = "custom"
INTERVAL_MINUTES = 30
DEFAULT = []
ENABLED = True


def fetch():
    from .pipeline import (AustinTXIngestor, LTASingaporeIngestor,
                           NYCDOTIngestor, TFLJamCamIngestor, get_all_cameras)

    logger.info("Running CCTV Pipeline Ingestion...")
    for ingestor in (
        TFLJamCamIngestor,
        LTASingaporeIngestor,
        AustinTXIngestor,
        NYCDOTIngestor,
    ):
        try:
            ingestor().ingest()
        except Exception as e:
            logger.error(f"Failed {ingestor.__name__} cctv ingest: {e}")
    try:
        cameras = get_all_cameras()
        logger.info(f"CCTV: {len(cameras)} cameras")
        return cameras
    except Exception as e:
        logger.error(f"Error fetching cctv from DB: {e}")
    return None
