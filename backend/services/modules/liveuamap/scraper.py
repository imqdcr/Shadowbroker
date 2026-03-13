import base64
import json
import logging
import re
import urllib.parse

import cloudscraper

logger = logging.getLogger(__name__)

_REGIONS = [
    {"name": "Ukraine",           "url": "https://liveuamap.com"},
    {"name": "Middle East",       "url": "https://mideast.liveuamap.com"},
    {"name": "Israel-Palestine",  "url": "https://israelpalestine.liveuamap.com"},
    {"name": "Syria",             "url": "https://syria.liveuamap.com"},
]

_scraper = cloudscraper.create_scraper(
    browser={"browser": "chrome", "platform": "windows", "desktop": True}
)


def fetch_liveuamap():
    all_markers = []
    seen_ids = set()

    for region in _REGIONS:
        try:
            resp = _scraper.get(region["url"], timeout=20)
            resp.raise_for_status()
            html = resp.text

            m = re.search(r"var\s+ovens\s*=\s*(.*?);(?!\s*function)", html, re.DOTALL)
            if not m:
                logger.warning(f"No 'ovens' data found for {region['name']}")
                continue

            json_str = m.group(1).strip()

            # Encoded form: base64(url-encoded JSON) wrapped in quotes
            if json_str.startswith(("'", '"')):
                json_str = json_str.strip("\"'")
                json_str = base64.b64decode(urllib.parse.unquote(json_str)).decode("utf-8")

            markers = json.loads(json_str)
            # Some regions double-encode: parsed result is a string, not a list
            if isinstance(markers, str):
                markers = json.loads(markers)
            if not isinstance(markers, list):
                logger.warning(f"Unexpected ovens type for {region['name']}: {type(markers)}")
                continue
            for marker in markers:
                mid = marker.get("id")
                if mid and mid not in seen_ids:
                    seen_ids.add(mid)
                    all_markers.append({
                        "id":        mid,
                        "type":      "liveuamap",
                        "title":     marker.get("s") or marker.get("title") or "Unknown Event",
                        "lat":       marker.get("lat"),
                        "lng":       marker.get("lng"),
                        "timestamp": marker.get("time", ""),
                        "link":      marker.get("link", region["url"]),
                        "region":    region["name"],
                    })

        except Exception as e:
            logger.error(f"Liveuamap {region['name']} failed: {e}")

    logger.info(f"Liveuamap: {len(all_markers)} unique markers")
    return all_markers
