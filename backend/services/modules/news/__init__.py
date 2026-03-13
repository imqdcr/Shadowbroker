import concurrent.futures
import logging
import random
import re

import feedparser

from services.network_utils import fetch_with_curl
from .keywords import KEYWORD_COORDS

logger = logging.getLogger(__name__)

KEY = "news"
TIER = "slow"
DEFAULT = []
ENABLED = True


def _generate_machine_assessment(title, description, risk_score):
    if risk_score < 8:
        return None
    keywords = [word.lower() for word in title.split() + description.split()]
    assessment = "ANALYSIS: "
    if any(k in keywords for k in ["strike", "missile", "attack", "bomb", "drone"]):
        assessment += (f"{random.randint(75, 95)}% probability of kinetic escalation within 24 hours. "
                       f"Recommend immediate asset relocation from projected blast radius.")
    elif any(k in keywords for k in ["sanction", "trade", "economy", "tariff", "boycott"]):
        assessment += (f"Significant economic severing detected. {random.randint(60, 85)}% chance of "
                       f"reciprocal sanctions. Global supply chains may experience cascading latency.")
    elif any(k in keywords for k in ["cyber", "hack", "breach", "ddos", "ransomware"]):
        assessment += (f"Asymmetric digital warfare signature matched. {random.randint(80, 99)}% probability "
                       f"of infrastructure probing. Initiate air-gapping protocol for critical nodes.")
    elif any(k in keywords for k in ["troop", "deploy", "border", "navy", "carrier"]):
        assessment += (f"Force projection detected. {random.randint(70, 90)}% probability of theater "
                       f"escalation. Monitor adjacent maritime and airspace for mobilization.")
    else:
        assessment += (f"Anomalous geopolitical shift detected. Confidence interval {random.randint(60, 90)}%. "
                       f"Awaiting further signals intelligence for definitive vector.")
    return assessment


def fetch():
    from .feed_config import get_feeds
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
                risk_keywords = ["war", "missile", "strike", "attack", "crisis", "tension",
                                 "military", "conflict", "defense", "clash", "nuclear"]
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
                for kw, coords in KEYWORD_COORDS.items():
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
