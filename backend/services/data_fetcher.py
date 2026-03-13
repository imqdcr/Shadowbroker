import logging
import math
import time
import threading
import concurrent.futures
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
load_dotenv()
from services.modules.cctv.pipeline import init_db
from services.module_loader import loader

logger = logging.getLogger(__name__)





# In-memory store
latest_data = {
    "last_updated": None,
    "news": [],
    "stocks": {},
    "oil": {},
    "flights": [],
    "ships": [],
    "military_flights": [],
    "tracked_flights": [],
    "cctv": [],
    "weather": None,
    # bikeshare removed per user request
    "traffic": [],
    "earthquakes": [],
    "uavs": [],
    "frontlines": None,
    "gdelt": [],
    "liveuamap": [],
    "kiwisdr": [],
    "space_weather": None,
    "internet_outages": [],
    "firms_fires": [],
    "datacenters": []
}

# Per-source freshness timestamps — updated each time a fetch function completes successfully
source_timestamps = {}

def _mark_fresh(*keys):
    """Record the current UTC time for one or more data source keys."""
    now = datetime.utcnow().isoformat()
    for k in keys:
        source_timestamps[k] = now

# Thread lock for safe reads/writes to latest_data
_data_lock = threading.Lock()

# Auto-discover all enabled modules from services/modules/.
# Must happen after latest_data is defined so init_latest_data can write defaults.
loader.load()
loader.init_latest_data(latest_data)


def update_fast_data():
    """Fast-tier: moving entities that need frequent updates (every 60s)."""
    logger.info("Fast-tier data update starting...")
    loader.run_fast_modules()
    with _data_lock:
        latest_data['last_updated'] = datetime.utcnow().isoformat()
    logger.info("Fast-tier update complete.")

def update_slow_data():
    """Slow-tier: feeds that change infrequently (every 30min)."""
    logger.info("Slow-tier data update starting...")
    loader.run_slow_modules()
    logger.info("Slow-tier update complete.")

def update_all_data():
    """Full update — runs on startup. Fast and slow tiers run IN PARALLEL for fastest startup."""
    logger.info("Full data update starting (parallel)...")
    # Run fast + slow in parallel so the user sees data ASAP
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(update_fast_data)
        f2 = pool.submit(update_slow_data)
        concurrent.futures.wait([f1, f2])
    logger.info("Full data update complete.")

scheduler = BackgroundScheduler()

def start_scheduler():
    init_db()
    
    # Run full update once on startup
    scheduler.add_job(update_all_data, 'date', run_date=datetime.now())
    
    # Fast tier: every 60 seconds (flights, ships, military+UAVs, satellites)
    scheduler.add_job(update_fast_data, 'interval', seconds=60)
    
    # Slow tier: every 30 minutes (news, stocks, weather, geopolitics)
    scheduler.add_job(update_slow_data, 'interval', minutes=30)
    
    loader.start_all(None, latest_data, _data_lock, _mark_fresh, scheduler)

    scheduler.start()

def stop_scheduler():
    loader.stop_all()
    scheduler.shutdown()

def get_latest_data():
    with _data_lock:
        return dict(latest_data)

