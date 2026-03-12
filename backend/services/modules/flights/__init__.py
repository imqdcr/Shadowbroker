"""
flights/ — Unified ADS-B flight tracking module.

Handles: commercial, private, military, UAVs, tracked, GPS jamming, trails.

Pipeline:
  fetcher.py   → fetch_civilian_raw(), fetch_military_raw()
  pipeline.py  → process(civilian_raw, military_raw, current_state)
"""

import logging

logger = logging.getLogger(__name__)

KEYS = [
    "commercial_flights",
    "private_jets",
    "private_flights",
    "military_flights",
    "tracked_flights",
    "uavs",
    "gps_jamming",
    "flights",
]
TIER = "fast"
DEFAULT = {k: [] for k in KEYS}
ENABLED = True


def fetch(current_state=None):
    from .fetcher import fetch_civilian_raw, fetch_military_raw
    from .pipeline import process

    if current_state is None:
        current_state = {}

    civilian_raw, _ = fetch_civilian_raw()
    military_raw = fetch_military_raw()

    return process(civilian_raw, military_raw, current_state)
