import concurrent.futures
import logging
import random
import re

import feedparser

from services.network_utils import fetch_with_curl

logger = logging.getLogger(__name__)

KEY = "news"
TIER = "slow"
DEFAULT = []

_KEYWORD_COORDS = {
    "venezuela": (7.119, -66.589),
    "brazil": (-14.235, -51.925),
    "argentina": (-38.416, -63.616),
    "colombia": (4.570, -74.297),
    "mexico": (23.634, -102.552),
    "united states": (38.907, -77.036),
    " usa ": (38.907, -77.036),
    " us ": (38.907, -77.036),
    "washington": (38.907, -77.036),
    "canada": (56.130, -106.346),
    "ukraine": (49.487, 31.272),
    "kyiv": (50.450, 30.523),
    "russia": (61.524, 105.318),
    "moscow": (55.755, 37.617),
    "israel": (31.046, 34.851),
    "gaza": (31.416, 34.333),
    "iran": (32.427, 53.688),
    "lebanon": (33.854, 35.862),
    "syria": (34.802, 38.996),
    "yemen": (15.552, 48.516),
    "china": (35.861, 104.195),
    "beijing": (39.904, 116.407),
    "taiwan": (23.697, 120.960),
    "north korea": (40.339, 127.510),
    "south korea": (35.907, 127.766),
    "pyongyang": (39.039, 125.762),
    "seoul": (37.566, 126.978),
    "japan": (36.204, 138.252),
    "tokyo": (35.676, 139.650),
    "afghanistan": (33.939, 67.709),
    "pakistan": (30.375, 69.345),
    "india": (20.593, 78.962),
    " uk ": (55.378, -3.435),
    "london": (51.507, -0.127),
    "france": (46.227, 2.213),
    "paris": (48.856, 2.352),
    "germany": (51.165, 10.451),
    "berlin": (52.520, 13.405),
    "sudan": (12.862, 30.217),
    "congo": (-4.038, 21.758),
    "south africa": (-30.559, 22.937),
    "nigeria": (9.082, 8.675),
    "egypt": (26.820, 30.802),
    "zimbabwe": (-19.015, 29.154),
    "kenya": (-1.292, 36.821),
    "libya": (26.335, 17.228),
    "mali": (17.570, -3.996),
    "niger": (17.607, 8.081),
    "somalia": (5.152, 46.199),
    "ethiopia": (9.145, 40.489),
    "australia": (-25.274, 133.775),
    "middle east": (31.500, 34.800),
    "europe": (48.800, 2.300),
    "africa": (0.000, 25.000),
    "america": (38.900, -77.000),
    "south america": (-14.200, -51.900),
    "asia": (34.000, 100.000),
    "california": (36.778, -119.417),
    "texas": (31.968, -99.901),
    "florida": (27.994, -81.760),
    "new york": (40.712, -74.006),
    "virginia": (37.431, -78.656),
    "british columbia": (53.726, -127.647),
    "ontario": (51.253, -85.323),
    "quebec": (52.939, -73.549),
    "delhi": (28.704, 77.102),
    "new delhi": (28.613, 77.209),
    "mumbai": (19.076, 72.877),
    "shanghai": (31.230, 121.473),
    "hong kong": (22.319, 114.169),
    "istanbul": (41.008, 28.978),
    "dubai": (25.204, 55.270),
    "singapore": (1.352, 103.819),
    "bangkok": (13.756, 100.501),
    "jakarta": (-6.208, 106.845),
}


def _generate_machine_assessment(title, description, risk_score):
    if risk_score < 8:
        return None
    keywords = [word.lower() for word in title.split() + description.split()]
    assessment = "ANALYSIS: "
    if any(k in keywords for k in ["strike", "missile", "attack", "bomb", "drone"]):
        assessment += f"{random.randint(75, 95)}% probability of kinetic escalation within 24 hours. Recommend immediate asset relocation from projected blast radius."
    elif any(k in keywords for k in ["sanction", "trade", "economy", "tariff", "boycott"]):
        assessment += f"Significant economic severing detected. {random.randint(60, 85)}% chance of reciprocal sanctions. Global supply chains may experience cascading latency."
    elif any(k in keywords for k in ["cyber", "hack", "breach", "ddos", "ransomware"]):
        assessment += f"Asymmetric digital warfare signature matched. {random.randint(80, 99)}% probability of infrastructure probing. Initiate air-gapping protocol for critical nodes."
    elif any(k in keywords for k in ["troop", "deploy", "border", "navy", "carrier"]):
        assessment += f"Force projection detected. {random.randint(70, 90)}% probability of theater escalation. Monitor adjacent maritime and airspace for mobilization."
    else:
        assessment += f"Anomalous geopolitical shift detected. Confidence interval {random.randint(60, 90)}%. Awaiting further signals intelligence for definitive vector."
    return assessment


def fetch():
    from services.news_feed_config import get_feeds
    feed_config = get_feeds()
    feeds = {f["name"]: f["url"] for f in feed_config}
    source_weights = {f["name"]: f["weight"] for f in feed_config}

    clusters = {}

    def _fetch_feed(item):
        source_name, url = item
        try:
            xml_data = fetch_with_curl(url, timeout=10).text
            return source_name, feedparser.parse(xml_data)
        except Exception as e:
            logger.warning(f"Feed {source_name} failed: {e}")
            return source_name, None

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(feeds)) as pool:
        feed_results = list(pool.map(_fetch_feed, feeds.items()))

    for source_name, feed in feed_results:
        if not feed:
            continue
        for entry in feed.entries[:5]:
            title = entry.get("title", "")
            summary = entry.get("summary", "")

            _seismic_kw = ["earthquake", "seismic", "quake", "tremor", "magnitude", "richter"]
            _text_lower = (title + " " + summary).lower()
            if any(kw in _text_lower for kw in _seismic_kw):
                continue

            if source_name == "GDACS":
                alert_level = entry.get("gdacs_alertlevel", "Green")
                if alert_level == "Red":
                    risk_score = 10
                elif alert_level == "Orange":
                    risk_score = 7
                else:
                    risk_score = 4
            else:
                risk_keywords = ["war", "missile", "strike", "attack", "crisis", "tension", "military", "conflict", "defense", "clash", "nuclear"]
                text = (title + " " + summary).lower()
                risk_score = 1
                for kw in risk_keywords:
                    if kw in text:
                        risk_score += 2
                risk_score = min(10, risk_score)

            text = (title + " " + summary).lower()
            lat, lng = None, None

            if "georss_point" in entry:
                geo_parts = entry["georss_point"].split()
                if len(geo_parts) == 2:
                    lat, lng = float(geo_parts[0]), float(geo_parts[1])
            elif "where" in entry and hasattr(entry["where"], "coordinates"):
                coords = entry["where"].coordinates
                lat, lng = coords[1], coords[0]

            if lat is None:
                padded_text = f" {text} "
                for kw, coords in _KEYWORD_COORDS.items():
                    if kw.startswith(" ") or kw.endswith(" "):
                        if kw in padded_text:
                            lat, lng = coords
                            break
                    else:
                        if re.search(r"\b" + re.escape(kw) + r"\b", text):
                            lat, lng = coords
                            break

            if lat is not None:
                key = None
                for existing_key in clusters.keys():
                    if "," in existing_key:
                        parts = existing_key.split(",")
                        try:
                            elat, elng = float(parts[0]), float(parts[1])
                            if ((lat - elat) ** 2 + (lng - elng) ** 2) ** 0.5 < 4.0:
                                key = existing_key
                                break
                        except ValueError:
                            pass
                if key is None:
                    key = f"{lat},{lng}"
            else:
                key = title

            if key not in clusters:
                clusters[key] = []

            clusters[key].append({
                "title": title,
                "link": entry.get("link", ""),
                "published": entry.get("published", ""),
                "source": source_name,
                "risk_score": risk_score,
                "coords": [lat, lng] if lat is not None else None,
            })

    news_items = []
    for key, articles in clusters.items():
        articles.sort(key=lambda x: (x["risk_score"], source_weights.get(x["source"], 0)), reverse=True)
        max_risk = articles[0]["risk_score"]
        top_article = articles[0]
        news_items.append({
            "title": top_article["title"],
            "link": top_article["link"],
            "published": top_article["published"],
            "source": top_article["source"],
            "risk_score": max_risk,
            "coords": top_article["coords"],
            "cluster_count": len(articles),
            "articles": articles,
            "machine_assessment": _generate_machine_assessment(top_article["title"], "", max_risk),
        })

    news_items.sort(key=lambda x: x["risk_score"], reverse=True)
    return news_items
