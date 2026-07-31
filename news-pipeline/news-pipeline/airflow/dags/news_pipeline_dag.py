from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def run_scraper(**ctx):
    import os
    import subprocess
    import sys

    env = {**os.environ, "SCRAPER_ONCE": "1"}
    result = subprocess.run(
        [sys.executable, "/opt/scraper/scraper.py"],
        capture_output=True,
        text=True,
        timeout=900,
        env=env,
    )
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"Scraper exit {result.returncode}. STDERR: {result.stderr!r} STDOUT tail: {result.stdout[-2000:]!r}"
        )


def bronze_to_silver(**ctx):
    import io
    import json
    import re
    from datetime import datetime

    from langdetect import detect
    from minio import Minio

    mc = Minio("minio:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
    today = datetime.utcnow().strftime("%Y/%m/%d")
    processed = 0

    objects = mc.list_objects("bronze", recursive=True)
    for obj in objects:
        if today not in obj.object_name:
            continue
        response = mc.get_object("bronze", obj.object_name)
        article = json.loads(response.read().decode("utf-8"))

        article["content"] = re.sub(r"<[^>]+>", "", article.get("content", ""))
        article["content"] = re.sub(r"\s+", " ", article["content"]).strip()
        article["title"] = re.sub(r"\s+", " ", article.get("title", "")).strip()

        cat = (article.get("category") or "General").strip()
        article["theme"] = (article.get("theme") or cat or "actualites")[:200]
        article.setdefault("country_code", "XX")
        article.setdefault("region", "INTL")
        article.setdefault("ingestion_mode", "batch")
        article.setdefault("pipeline_version", "airflow")

        try:
            blob = article["title"] + " " + article["content"]
            if len(blob.strip()) > 12:
                article["lang"] = detect(blob)
        except Exception:
            pass

        article["silver_at"] = datetime.utcnow().isoformat()

        payload = json.dumps(article, ensure_ascii=False).encode("utf-8")
        mc.put_object(
            "silver",
            obj.object_name,
            io.BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )
        processed += 1

    print(f"Silver: {processed} articles transformés")


def silver_to_gold(**ctx):
    import io
    import json
    import re
    from collections import Counter
    from datetime import datetime

    import psycopg2
    from minio import Minio

    mc = Minio("minio:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
    conn = psycopg2.connect(host="postgres", dbname="news_dw", user="datauser", password="datapass")
    cur = conn.cursor()

    today_path = datetime.utcnow().strftime("%Y/%m/%d")
    day = datetime.utcnow().date()
    articles = []

    objects = mc.list_objects("silver", recursive=True)
    for obj in objects:
        if today_path not in obj.object_name:
            continue
        response = mc.get_object("silver", obj.object_name)
        articles.append(json.loads(response.read().decode("utf-8")))

    if not articles:
        print("Aucun article silver pour aujourd'hui")
        cur.close()
        conn.close()
        return

    for art in articles:
        cur.execute(
            """
            INSERT INTO articles (
                id, title, author, pub_date, category, theme, content, source, url, lang,
                country_code, region, ingestion_mode, pipeline_version, scraped_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO NOTHING
            """,
            (
                art.get("id"),
                art.get("title"),
                art.get("author"),
                art.get("pub_date"),
                art.get("category"),
                art.get("theme") or art.get("category") or "actualites",
                art.get("content"),
                art.get("source"),
                art.get("url"),
                art.get("lang"),
                art.get("country_code") or "XX",
                art.get("region") or "INTL",
                art.get("ingestion_mode") or "batch",
                art.get("pipeline_version"),
                art.get("scraped_at"),
            ),
        )

    source_counts = Counter(a["source"] for a in articles)
    for source, cnt in source_counts.items():
        cur.execute(
            """
            INSERT INTO articles_by_source (date, source, article_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, source) DO UPDATE SET article_count = EXCLUDED.article_count
            """,
            (day, source, cnt),
        )

    cat_counts = Counter((a.get("category") or "General") for a in articles)
    for category, cnt in cat_counts.items():
        cur.execute(
            """
            INSERT INTO articles_by_category (date, category, article_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, category) DO UPDATE SET article_count = EXCLUDED.article_count
            """,
            (day, category, cnt),
        )

    lang_counts = Counter((a.get("lang") or "?") for a in articles)
    for lang, cnt in lang_counts.items():
        cur.execute(
            """
            INSERT INTO articles_by_lang (date, lang, article_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, lang) DO UPDATE SET article_count = EXCLUDED.article_count
            """,
            (day, lang, cnt),
        )

    theme_counts = Counter(
        str(a.get("theme") or a.get("category") or "actualites")[:200] for a in articles
    )
    for theme, cnt in theme_counts.items():
        cur.execute(
            """
            INSERT INTO articles_by_theme (date, theme, article_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, theme) DO UPDATE SET article_count = EXCLUDED.article_count
            """,
            (day, theme, cnt),
        )

    country_counts = Counter((a.get("country_code") or "XX") for a in articles)
    for cc, cnt in country_counts.items():
        cur.execute(
            """
            INSERT INTO articles_by_country (date, country_code, article_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, country_code) DO UPDATE SET article_count = EXCLUDED.article_count
            """,
            (day, cc, cnt),
        )

    stop_words = {
        "le",
        "la",
        "les",
        "de",
        "du",
        "et",
        "en",
        "un",
        "une",
        "à",
        "au",
        "des",
        "est",
        "l",
        "d",
        "the",
        "and",
        "for",
        "that",
        "with",
        "from",
        "this",
        "have",
        "has",
        "was",
        "were",
        "are",
        "will",
        "into",
        "about",
    }
    words = Counter()
    for art in articles:
        blob = (art.get("title", "") + " " + art.get("content", "")).lower()
        tokens = re.findall(r"[\w\u0600-\u06FF]{4,}", blob)
        words.update(w for w in tokens if w not in stop_words)

    for word, cnt in words.most_common(50):
        cur.execute(
            """
            INSERT INTO top_keywords (date, keyword, frequency)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, keyword) DO UPDATE SET frequency = EXCLUDED.frequency
            """,
            (day, word[:200], cnt),
        )

    cur.execute("SELECT COUNT(*) FROM articles WHERE DATE(scraped_at)=%s", (day,))
    total_day = cur.fetchone()[0]
    cur.execute(
        """
        INSERT INTO articles_by_day (date, article_count)
        VALUES (%s, %s)
        ON CONFLICT (date) DO UPDATE SET article_count = EXCLUDED.article_count
        """,
        (day, total_day),
    )

    summary = {
        "date": str(day),
        "batch_articles": len(articles),
        "total_articles_in_dw_today": total_day,
        "by_source": dict(source_counts),
        "by_country": dict(country_counts),
        "by_theme_top": [{"theme": t, "count": c} for t, c in theme_counts.most_common(10)],
    }
    raw = json.dumps(summary, ensure_ascii=False).encode("utf-8")
    mc.put_object(
        "gold",
        f"daily/{day}/summary.json",
        io.BytesIO(raw),
        length=len(raw),
        content_type="application/json",
    )

    cur.execute(
        "UPDATE data_catalog SET updated_at = NOW() WHERE dataset_name = %s",
        ("minio.gold.daily_summary",),
    )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Gold: {len(articles)} articles chargés — résumé MinIO gold/daily/{day}/summary.json")


def data_quality_check(**ctx):
    import os

    import psycopg2
    from datetime import datetime

    conn = psycopg2.connect(host="postgres", dbname="news_dw", user="datauser", password="datapass")
    cur = conn.cursor()
    today = datetime.utcnow().date()
    issues = []

    cur.execute(
        "SELECT COUNT(*) FROM articles WHERE (title IS NULL OR title='') AND DATE(scraped_at)=%s",
        (today,),
    )
    n = cur.fetchone()[0]
    if n > 0:
        issues.append({"check": "missing_title", "count": n, "severity": "high", "dimension": "completeness"})

    cur.execute(
        "SELECT COUNT(*) FROM articles WHERE (pub_date IS NULL OR pub_date='') AND DATE(scraped_at)=%s",
        (today,),
    )
    n = cur.fetchone()[0]
    if n > 0:
        issues.append({"check": "missing_date", "count": n, "severity": "medium", "dimension": "completeness"})

    cur.execute(
        "SELECT COUNT(*) FROM articles WHERE LENGTH(COALESCE(content,'')) < 20 AND DATE(scraped_at)=%s",
        (today,),
    )
    n = cur.fetchone()[0]
    if n > 0:
        issues.append({"check": "short_content", "count": n, "severity": "low", "dimension": "validity"})

    cur.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT url FROM articles WHERE DATE(scraped_at)=%s
          GROUP BY url HAVING COUNT(*) > 1
        ) t
        """,
        (today,),
    )
    n = cur.fetchone()[0]
    if n > 0:
        issues.append({"check": "duplicate_url", "count": n, "severity": "high", "dimension": "coherence"})

    cur.execute(
        """
        SELECT COUNT(*) FROM articles
        WHERE DATE(scraped_at)=%s AND url NOT LIKE 'http%%'
        """,
        (today,),
    )
    n = cur.fetchone()[0]
    if n > 0:
        issues.append({"check": "invalid_url", "count": n, "severity": "high", "dimension": "validity"})

    for issue in issues:
        cur.execute(
            """
            INSERT INTO dq_checks (check_date, check_name, issue_count, severity, dq_dimension)
            VALUES (%s,%s,%s,%s,%s)
            """,
            (today, issue["check"], issue["count"], issue["severity"], issue["dimension"]),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"DQ: {len(issues)} contrôle(s) avec écart — dimensions complétude / validité / cohérence")

    strict = os.getenv("DQ_STRICT", "0").lower() in ("1", "true", "yes")
    if strict and any(i["severity"] == "high" for i in issues):
        raise RuntimeError(f"DQ stricte — anomalies critiques: {[i for i in issues if i['severity']=='high']}")


with DAG(
    dag_id="news_pipeline",
    default_args=default_args,
    description="Plateforme données médias : scraping, lac MinIO, médaille, DW, qualité, gouvernance",
    schedule_interval="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["news", "bigdata", "medallion", "governance"],
) as dag:

    t_scrape = PythonOperator(task_id="scrape_articles", python_callable=run_scraper)
    t_silver = PythonOperator(task_id="bronze_to_silver", python_callable=bronze_to_silver)
    t_gold = PythonOperator(task_id="silver_to_gold", python_callable=silver_to_gold)
    t_dq = PythonOperator(task_id="data_quality_check", python_callable=data_quality_check)

    t_scrape >> t_silver >> t_gold >> t_dq
