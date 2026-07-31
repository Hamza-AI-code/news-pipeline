-- Data Warehouse + catalogage (cahier des charges : DW, qualité, gouvernance)

CREATE TABLE IF NOT EXISTS articles (
    id                VARCHAR(32) PRIMARY KEY,
    title             TEXT,
    author            VARCHAR(255),
    pub_date          VARCHAR(100),
    category          VARCHAR(100),
    theme             VARCHAR(200),
    content           TEXT,
    source            VARCHAR(100),
    url               TEXT,
    lang              VARCHAR(10),
    country_code      VARCHAR(3),
    region            VARCHAR(10),
    ingestion_mode    VARCHAR(20) DEFAULT 'batch',
    pipeline_version  VARCHAR(32),
    scraped_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS articles_by_source (
    date          DATE,
    source        VARCHAR(100),
    article_count INT,
    PRIMARY KEY (date, source)
);

CREATE TABLE IF NOT EXISTS articles_by_category (
    date          DATE,
    category      VARCHAR(100),
    article_count INT,
    PRIMARY KEY (date, category)
);

CREATE TABLE IF NOT EXISTS articles_by_lang (
    date          DATE,
    lang          VARCHAR(10),
    article_count INT,
    PRIMARY KEY (date, lang)
);

CREATE TABLE IF NOT EXISTS articles_by_day (
    date          DATE PRIMARY KEY,
    article_count INT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS articles_by_theme (
    date          DATE,
    theme         VARCHAR(200),
    article_count INT,
    PRIMARY KEY (date, theme)
);

CREATE TABLE IF NOT EXISTS articles_by_country (
    date          DATE,
    country_code  VARCHAR(3),
    article_count INT,
    PRIMARY KEY (date, country_code)
);

CREATE TABLE IF NOT EXISTS top_keywords (
    date      DATE,
    keyword   VARCHAR(200),
    frequency INT,
    PRIMARY KEY (date, keyword)
);

CREATE TABLE IF NOT EXISTS dq_checks (
    id            SERIAL PRIMARY KEY,
    check_date    DATE,
    check_name    VARCHAR(100),
    issue_count   INT,
    severity      VARCHAR(20),
    dq_dimension  VARCHAR(32),
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS data_catalog (
    dataset_name  VARCHAR(160) PRIMARY KEY,
    description   TEXT,
    layer           VARCHAR(20),
    owner           VARCHAR(80),
    pii_level       VARCHAR(40) DEFAULT 'none',
    updated_at      TIMESTAMP DEFAULT NOW()
);

INSERT INTO data_catalog (dataset_name, description, layer, owner, pii_level) VALUES
('minio.bronze.articles', 'Articles bruts JSON (scraping + Kafka), historique complet.', 'bronze', 'data_team', 'none'),
('minio.silver.articles', 'Articles nettoyés, HTML supprimé, langue détectée.', 'silver', 'data_team', 'none'),
('minio.gold.daily_summary', 'Résumés analytiques journaliers (médaille gold objet).', 'gold', 'data_team', 'none'),
('dw.articles', 'Fait article à grain fin pour analyses.', 'gold', 'data_team', 'low'),
('dw.articles_by_source', 'Volume par source média par jour.', 'gold', 'data_team', 'none'),
('dw.articles_by_theme', 'Volume par thème / rubrique par jour.', 'gold', 'data_team', 'none'),
('dw.articles_by_country', 'Volume par pays (marché du média) par jour.', 'gold', 'data_team', 'none'),
('dw.top_keywords', 'Mots-clés fréquents pour tendances.', 'gold', 'data_team', 'none'),
('kafka.raw-articles', 'Flux streaming : un événement JSON par article publié.', 'ingestion', 'data_team', 'low'),
('gouvernance.fake_news', 'Piste future : signaux de désinformation (MVP = collecte + qualité).', 'gouvernance', 'data_team', 'none')
ON CONFLICT (dataset_name) DO NOTHING;

CREATE OR REPLACE VIEW v_trends_7d AS
SELECT
    source,
    SUM(article_count) AS total_articles,
    ROUND(AVG(article_count), 1) AS avg_per_day
FROM articles_by_source
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY source
ORDER BY total_articles DESC;

CREATE OR REPLACE VIEW v_top_keywords_week AS
SELECT
    keyword,
    SUM(frequency) AS total_freq
FROM top_keywords
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY keyword
ORDER BY total_freq DESC
LIMIT 30;

CREATE OR REPLACE VIEW v_today_by_source AS
SELECT source, article_count
FROM articles_by_source
WHERE date = CURRENT_DATE
ORDER BY article_count DESC;

CREATE OR REPLACE VIEW v_today_by_country AS
SELECT country_code, article_count
FROM articles_by_country
WHERE date = CURRENT_DATE
ORDER BY article_count DESC;

CREATE OR REPLACE VIEW v_today_by_theme AS
SELECT theme, article_count
FROM articles_by_theme
WHERE date = CURRENT_DATE
ORDER BY article_count DESC;

CREATE OR REPLACE VIEW v_articles_last_7_days AS
SELECT date, article_count
FROM articles_by_day
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY date;

CREATE OR REPLACE VIEW v_dq_latest AS
SELECT check_date, check_name, issue_count, severity, dq_dimension, created_at
FROM dq_checks
ORDER BY created_at DESC
LIMIT 50;
