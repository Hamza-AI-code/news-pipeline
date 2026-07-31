import io
import json
import logging
import re
from datetime import datetime

from kafka import KafkaConsumer
from minio import Minio
from langdetect import detect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "raw-articles"
GROUP_ID = "silver-consumer"

mc = Minio("minio:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)


def clean_article(article: dict) -> dict:
    article["content"] = re.sub(r"<[^>]+>", "", article.get("content", ""))
    article["content"] = re.sub(r"\s+", " ", article["content"]).strip()
    article["title"] = re.sub(r"\s+", " ", article.get("title", "")).strip()
    article.setdefault("category", "General")
    article.setdefault("theme", article.get("category") or "actualites")
    article.setdefault("country_code", "XX")
    article.setdefault("region", "INTL")
    article.setdefault("author", "Inconnu")
    article["ingestion_mode"] = "streaming"
    article.setdefault("pipeline_version", "kafka-consumer")

    try:
        text = article["title"] + " " + article["content"]
        if len(text.strip()) > 10:
            article["lang"] = detect(text)
    except Exception:
        pass

    article["silver_at"] = datetime.utcnow().isoformat()
    return article


def save_silver(article: dict):
    today = datetime.utcnow().strftime("%Y/%m/%d")
    key = f"{article['source']}/{today}/{article['id']}.json"
    data = json.dumps(article, ensure_ascii=False).encode("utf-8")
    mc.put_object("silver", key, io.BytesIO(data), length=len(data), content_type="application/json")


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    logger.info("Consumer streaming démarré — topic %s", TOPIC)
    for msg in consumer:
        try:
            article = clean_article(msg.value)
            save_silver(article)
            logger.info("[Stream→Silver] %s | %s", article["source"], article["title"][:50])
        except Exception as e:
            logger.error("Erreur traitement message: %s", e)


if __name__ == "__main__":
    main()
