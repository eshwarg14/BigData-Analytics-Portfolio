-- ============================================================
-- movies_analytics.hql — Apache Hive Movies & Directors Analytics
-- ============================================================
-- Demonstrates: Partitioning, Bucketing, HiveQL Joins,
--               Aggregations, Sampling, and Partition Pruning
--
-- Dataset: movies.csv (30 records, 6 genres)
--          directors.csv (30 records)
-- ============================================================

-- ── SETUP: Configure Hive for dynamic partitioning ───────────
SET hive.exec.dynamic.partition       = true;
SET hive.exec.dynamic.partition.mode  = nonstrict;
SET hive.enforce.bucketing            = true;
SET hive.exec.mode.local.auto         = false;

-- Use (or create) the movies database
CREATE DATABASE IF NOT EXISTS movies;
USE movies;

-- ─────────────────────────────────────────────────────────────
-- STEP 1: Staging Table (flat, no partitioning)
-- Loads raw CSV data including genre column for partition insert
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS movies_staging (
    movie_id     INT,
    title        STRING,
    genre        STRING,
    release_year INT,
    language     STRING,
    rating       FLOAT,
    budget_cr    FLOAT,
    revenue_cr   FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

-- Load raw CSV into staging
LOAD DATA LOCAL INPATH '/home/eshwar/Downloads/A8/movies.csv'
INTO TABLE movies_staging;

-- Verify staging load
SELECT COUNT(*) AS total_records FROM movies_staging;

-- ─────────────────────────────────────────────────────────────
-- STEP 2: Partitioned + Bucketed ORC Table
-- Partitioned BY genre  → separate HDFS subdirectory per genre
-- Bucketed BY movie_id INTO 4 BUCKETS → hash-based file splits
-- Stored as ORC → columnar format for fast analytical queries
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS movies (
    movie_id     INT,
    title        STRING,
    release_year INT,
    language     STRING,
    rating       FLOAT,
    budget_cr    FLOAT,
    revenue_cr   FLOAT
)
PARTITIONED BY (genre STRING)
CLUSTERED BY (movie_id) INTO 4 BUCKETS
STORED AS ORC;

-- ─────────────────────────────────────────────────────────────
-- STEP 3: Partition Inserts — one INSERT per genre
-- Each INSERT writes to a separate HDFS directory:
--   /user/hive/warehouse/movies.db/movies/genre=Action/
--   /user/hive/warehouse/movies.db/movies/genre=Comedy/
--   ...
-- ─────────────────────────────────────────────────────────────
INSERT INTO TABLE movies PARTITION (genre='Action')
SELECT movie_id, title, release_year, language, rating, budget_cr, revenue_cr
FROM movies_staging WHERE genre = 'Action';

INSERT INTO TABLE movies PARTITION (genre='Comedy')
SELECT movie_id, title, release_year, language, rating, budget_cr, revenue_cr
FROM movies_staging WHERE genre = 'Comedy';

INSERT INTO TABLE movies PARTITION (genre='Drama')
SELECT movie_id, title, release_year, language, rating, budget_cr, revenue_cr
FROM movies_staging WHERE genre = 'Drama';

INSERT INTO TABLE movies PARTITION (genre='Sci-Fi')
SELECT movie_id, title, release_year, language, rating, budget_cr, revenue_cr
FROM movies_staging WHERE genre = 'Sci-Fi';

INSERT INTO TABLE movies PARTITION (genre='Historical')
SELECT movie_id, title, release_year, language, rating, budget_cr, revenue_cr
FROM movies_staging WHERE genre = 'Historical';

INSERT INTO TABLE movies PARTITION (genre='Thriller')
SELECT movie_id, title, release_year, language, rating, budget_cr, revenue_cr
FROM movies_staging WHERE genre = 'Thriller';

-- ─────────────────────────────────────────────────────────────
-- STEP 4: Partition Pruning Query
-- WHERE genre = 'Action' → Hive reads ONLY the Action partition
-- Avoids full table scan → significant performance gain at scale
-- ─────────────────────────────────────────────────────────────
SELECT title, release_year, rating, revenue_cr
FROM movies
WHERE genre = 'Action'
ORDER BY revenue_cr DESC;

-- ─────────────────────────────────────────────────────────────
-- STEP 5: Bucketing Sampling
-- TABLESAMPLE reads only Bucket 1 of 4 → 25% of data per partition
-- Useful for statistical sampling without full table scan
-- ─────────────────────────────────────────────────────────────
SELECT movie_id, title, genre
FROM movies
TABLESAMPLE(BUCKET 1 OUT OF 4 ON movie_id);

-- ─────────────────────────────────────────────────────────────
-- STEP 6: Average Rating and Revenue by Genre
-- Aggregates rating and revenue statistics per genre category
-- ─────────────────────────────────────────────────────────────
SELECT
    genre,
    ROUND(AVG(rating),     2) AS avg_rating,
    ROUND(AVG(revenue_cr), 2) AS avg_revenue_cr,
    COUNT(*)                   AS total_movies
FROM movies
GROUP BY genre
ORDER BY avg_revenue_cr DESC;

-- ─────────────────────────────────────────────────────────────
-- STEP 7: Top-Grossing Movie per Genre
-- First pass: find max revenue per genre
-- ─────────────────────────────────────────────────────────────
SELECT genre, MAX(revenue_cr) AS max_revenue_cr
FROM movies
GROUP BY genre;

-- Second pass: match max revenue back to movie title
SELECT genre, title, revenue_cr
FROM movies
WHERE
    (genre = 'Action'     AND revenue_cr = 1250) OR
    (genre = 'Comedy'     AND revenue_cr = 460)  OR
    (genre = 'Drama'      AND revenue_cr = 150)  OR
    (genre = 'Sci-Fi'     AND revenue_cr = 4800) OR
    (genre = 'Historical' AND revenue_cr = 585)  OR
    (genre = 'Thriller'   AND revenue_cr = 650)
ORDER BY revenue_cr DESC;

-- ─────────────────────────────────────────────────────────────
-- STEP 8: Create Directors Table
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS directors (
    director_id   INT,
    movie_id      INT,
    director_name STRING,
    nationality   STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

LOAD DATA LOCAL INPATH '/home/eshwar/Downloads/A8/directors.csv'
INTO TABLE directors;

-- Verify directors load
SELECT COUNT(*) AS director_count FROM directors;

-- ─────────────────────────────────────────────────────────────
-- STEP 9: JOIN — Movies with Directors
-- Inner join on movie_id to get director details alongside movie info
-- Returns top 10 movies by rating with director attribution
-- ─────────────────────────────────────────────────────────────
SELECT
    m.title,
    m.genre,
    m.rating,
    m.release_year,
    m.revenue_cr,
    d.director_name,
    d.nationality
FROM movies m
JOIN directors d ON m.movie_id = d.movie_id
ORDER BY m.rating DESC
LIMIT 10;

-- ─────────────────────────────────────────────────────────────
-- STEP 10: Additional Analytics Queries
-- ─────────────────────────────────────────────────────────────

-- Budget vs Revenue efficiency (ROI) per movie
SELECT
    title,
    genre,
    budget_cr,
    revenue_cr,
    ROUND((revenue_cr - budget_cr), 2)             AS profit_cr,
    ROUND((revenue_cr / budget_cr) * 100, 1)       AS roi_percent
FROM movies
WHERE budget_cr > 0
ORDER BY roi_percent DESC
LIMIT 10;

-- Movies with above-average rating per genre
SELECT m.title, m.genre, m.rating, genre_avg.avg_rating
FROM movies m
JOIN (
    SELECT genre, ROUND(AVG(rating), 2) AS avg_rating
    FROM movies
    GROUP BY genre
) genre_avg ON m.genre = genre_avg.genre
WHERE m.rating > genre_avg.avg_rating
ORDER BY m.genre, m.rating DESC;

-- Language-wise movie count and average rating
SELECT
    language,
    COUNT(*)           AS movie_count,
    ROUND(AVG(rating), 2) AS avg_rating,
    SUM(revenue_cr)    AS total_revenue_cr
FROM movies
GROUP BY language
ORDER BY movie_count DESC;
