import hashlib
import io
import json
import logging
import os
import re
import time
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from minio import Minio

from sources_config import SOURCES

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", "")
MINIO_ACCESS = os.getenv("MINIO_ACCESS", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET", "minioadmin")
TOPIC = "raw-articles"
PIPELINE_VERSION = os.getenv("PIPELINE_VERSION", "1.0.0")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def make_id(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def normalize_theme(category: str, source_name: str) -> str:
    t = (category or "actualites").strip()
    if not t or t.lower() == "general":
        return "actualites"
    return t[:180]


def fetch_page(url: str, timeout=(8, 20)):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        logger.warning("Erreur fetch %s: %s", url, e)
        return None


def fetch_article_preview(url: str, max_chars: int = 800) -> str:
    soup = fetch_page(url, timeout=8)
    if not soup:
        return ""
    meta = soup.find("meta", attrs={"property": "og:description"}) or soup.find(
        "meta", attrs={"name": "description"}
    )
    if meta and meta.get("content"):
        text = meta["content"].strip()
        if len(text) >= 40:
            return text[:max_chars]
    parts = []
    for p in soup.find_all("p")[:8]:
        t = p.get_text(" ", strip=True)
        if len(t) > 40:
            parts.append(t)
        if sum(len(x) for x in parts) >= max_chars:
            break
    out = " ".join(parts).strip()
    return out[:max_chars] if out else ""


def _resolve_href(source: dict, href: str) -> str:
    base = source.get("base_url") or source["url"]
    return urljoin(base, href)


def _clean_flat_title(title: str) -> str:
    title = re.sub(r"[•\u2022]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title[:500]


def parse_articles(soup: BeautifulSoup, source: dict) -> list:
    articles = []
    limit = int(os.getenv("MAX_ARTICLES_PER_SOURCE", "20"))

    if source.get("flat_links"):
        cards = soup.select(source["article_selector"])[: max(limit * 3, 30)]
        seen = set()
        for el in cards:
            link_tag = el if el.name == "a" else el.select_one("a[href]")
            if not link_tag or not link_tag.has_attr("href"):
                continue
            href = _resolve_href(source, link_tag["href"].strip())
            title = link_tag.get_text(" ", strip=True)
            title = _clean_flat_title(title)
            if not title or len(title) < 12 or not href.startswith("http"):
                continue
            key = href.split("?")[0]
            if key in seen:
                continue
            seen.add(key)
            if len(articles) >= limit:
                break

            pub_date = ""
            author = "Inconnu"
            category = "General"

            content = ""
            if os.getenv("FETCH_ARTICLE_BODY", "0").lower() in ("1", "true", "yes"):
                content = fetch_article_preview(href)

            articles.append(
                {
                    "id": make_id(href),
                    "title": title,
                    "author": author,
                    "pub_date": pub_date or datetime.utcnow().isoformat(),
                    "category": category,
                    "theme": normalize_theme(category, source["name"]),
                    "content": content,
                    "source": source["name"],
                    "url": href,
                    "lang": source["lang"],
                    "country_code": source.get("country_code", "XX"),
                    "region": source.get("region", "INTL"),
                    "scraped_at": datetime.utcnow().isoformat(),
                    "ingestion_mode": "batch",
                    "pipeline_version": PIPELINE_VERSION,
                }
            )
        return articles

    cards = soup.select(source["article_selector"])[:limit]
    for card in cards:
        try:
            title_tag = card.select_one(source["title_sel"]) if source.get("title_sel") else None
            link_tag = card.select_one(source["link_sel"]) if source.get("link_sel") else None
            date_tag = card.select_one(source["date_sel"]) if source.get("date_sel") else None
            auth_tag = card.select_one(source["author_sel"]) if source.get("author_sel") else None
            cat_tag = card.select_one(source["category_sel"]) if source.get("category_sel") else None

            title = title_tag.get_text(" ", strip=True) if title_tag else ""
            href = link_tag["href"].strip() if link_tag and link_tag.has_attr("href") else ""
            pub_date = (
                (date_tag.get("datetime") or date_tag.get_text(strip=True)) if date_tag else ""
            )
            author = auth_tag.get_text(strip=True) if auth_tag else "Inconnu"
            category = cat_tag.get_text(strip=True) if cat_tag else "General"

            href = _resolve_href(source, href)

            if not title or not href:
                continue

            content = ""
            if os.getenv("FETCH_ARTICLE_BODY", "0").lower() in ("1", "true", "yes"):
                content = fetch_article_preview(href)

            articles.append(
                {
                    "id": make_id(href),
                    "title": title,
                    "author": author,
                    "pub_date": pub_date or datetime.utcnow().isoformat(),
                    "category": category,
                    "theme": normalize_theme(category, source["name"]),
                    "content": content,
                    "source": source["name"],
                    "url": href,
                    "lang": source["lang"],
                    "country_code": source.get("country_code", "XX"),
                    "region": source.get("region", "INTL"),
                    "scraped_at": datetime.utcnow().isoformat(),
                    "ingestion_mode": "batch",
                    "pipeline_version": PIPELINE_VERSION,
                }
            )
        except Exception as e:
            logger.debug("Erreur parsing card %s: %s", source.get("name"), e)

    return articles


def get_producer() -> KafkaProducer:
    # max_block_ms par défaut = 600000 (10 min) : si Kafka n'est pas prêt, la tâche Airflow semble « figée » puis échoue.
    max_wait = int(os.getenv("KAFKA_MAX_BLOCK_MS", "45000"))
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        retries=3,
        max_block_ms=max_wait,
        request_timeout_ms=min(max_wait, 60000),
    )


def publish(producer: KafkaProducer, article: dict):
    producer.send(TOPIC, value=article)
    logger.info("[Kafka] %s | %s", article["source"], article["title"][:55])


def get_minio() -> Minio:
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)


def save_bronze(client: Minio, article: dict):
    today = datetime.utcnow().strftime("%Y/%m/%d")
    key = f"{article['source']}/{today}/{article['id']}.json"
    payload = json.dumps(article, ensure_ascii=False).encode("utf-8")
    client.put_object(
        "bronze",
        key,
        io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )
    logger.info("[MinIO bronze] %s", key)


def run_once():
    producer = get_producer()
    minio = get_minio()
    total = 0

    only = os.getenv("ONLY_SOURCES", "").strip().lower()
    sources = SOURCES
    if only:
        names = {n.strip() for n in only.split(",") if n.strip()}
        sources = [s for s in SOURCES if s["name"].lower() in names]
        if not sources:
            logger.warning("ONLY_SOURCES=%r inconnu — toutes les sources.", only)
            sources = SOURCES

    for source in sources:
        logger.info("=== Scraping %s (%s) ===", source["name"], source.get("country_code"))
        soup = fetch_page(source["url"])
        if not soup:
            continue
        articles = parse_articles(soup, source)
        logger.info("  %s articles", len(articles))

        for art in articles:
            try:
                publish(producer, art)
                save_bronze(minio, art)
                total += 1
            except Exception as e:
                logger.error("Erreur publish/save: %s", e)

        time.sleep(2)

    producer.flush()
    logger.info("Run terminé — %s articles", total)


if __name__ == "__main__":
    if os.getenv("SCRAPER_ONCE", "").lower() in ("1", "true", "yes"):
        run_once()
    else:
        while True:
            try:
                run_once()
            except Exception as e:
                logger.error("Erreur run_once: %s", e)
            logger.info("Prochaine exécution dans %ss...", os.getenv("SLEEP_INTERVAL", "3600"))
            time.sleep(int(os.getenv("SLEEP_INTERVAL", "3600")))
