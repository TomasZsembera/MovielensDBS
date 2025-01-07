CREATE DATABASE PUMA_movielens_DB;

CREATE SCHEMA PUMA_MOVIELENS_DB.staging;

USE SCHEMA PUMA_MOVIELENS_DB.staging;

CREATE TABLE age_group_staging (
    id INT PRIMARY KEY,
    name VARCHAR(45)
);

CREATE TABLE occupations_staging (
    id INT PRIMARY KEY,
    name VARCHAR(255)
);


CREATE OR REPLACE TABLE users_staging (
    id INT PRIMARY KEY,
    gender CHAR(45),
    zip_code VARCHAR(255),
    age INT,
    occupation_id INT,
    FOREIGN KEY (age) REFERENCES age_group_staging(id),
    FOREIGN KEY (occupation_id) REFERENCES occupations_staging(id)
);

CREATE TABLE movies_staging (
    id INT PRIMARY KEY,
    title VARCHAR(255),
    release_year CHAR(4)
);

CREATE TABLE genres_staging (
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE genres_movies_staging (
    id INT PRIMARY KEY,
    movie_id INT,
    genre_id INT,
    FOREIGN KEY (movie_id) REFERENCES movies_staging(id),
    FOREIGN KEY (genre_id) REFERENCES genres_staging(id)
);

CREATE TABLE tags_staging (
    id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    tags VARCHAR(4000),
    created_at DATETIME,
    FOREIGN KEY(user_id) REFERENCES users_staging(id),
    FOREIGN KEY(movie_id) REFERENCES movies_staging(id)
);

CREATE TABLE ratings_staging (
    id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    rating INT,
    rated_at DATETIME,
    FOREIGN KEY(user_id) REFERENCES users_staging(id),
    FOREIGN KEY(movie_id) REFERENCES movies_staging(id)
);


CREATE OR REPLACE STAGE movielens_stage;

list @movielens_stage;

COPY INTO occupations_staging
FROM @movielens_stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO age_group_staging
FROM @movielens_stage/age_group.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO users_staging
FROM @movielens_stage/users1.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO genres_staging
FROM @movielens_stage/genres.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genres_movies_staging
FROM @movielens_stage/genres_movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO movies_staging
FROM @movielens_stage/movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO ratings_staging
FROM @movielens_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO tags_staging
FROM @movielens_stage/tags.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- CREATE TABLE dim_movies AS 
-- SELECT DISTINCT
--     m.title AS dim_moviesId,
--     g.name AS genre,
--     m.release_year AS release_year,
-- FROM movies_staging m 
-- JOIN genres_movies gm ON m.id = gm.id
-- JOIN genres g ON gm.id = g.id; 

CREATE OR REPLACE TABLE dim_movies AS
SELECT DISTINCT
    m.id AS dim_moviesId,
    m.title AS title,
    g.name AS genre,
    m.release_year AS release_year
FROM movies_staging m
JOIN genres_movies_staging gm ON m.id = gm.movie_id
JOIN genres_staging g ON gm.genre_id = g.id;



-- CREATE OR REPLACE TABLE dim_users AS 
-- SELECT DISTINCT
--     u.id AS dim_userId,
--     u.zip_code AS zip_code,
--     a.name AS age_group,
--     o.name AS occupation,
--     u.gender AS gender, 
-- FROM users_staging u 
-- JOIN occupations_staging o ON u.id = o.id
-- JOIN age_group_staging a ON u.id = a.id;

CREATE OR REPLACE TABLE dim_users AS 
SELECT DISTINCT
    u.id AS dim_userId,
    u.zip_code AS zip_code,
    a.name AS age_group,
    o.name AS occupation,
    u.gender AS gender
FROM users_staging u 
JOIN occupations_staging o ON u.occupation_id = o.id
JOIN age_group_staging a ON u.age = a.id;


CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE)) AS dim_dateId,
    CAST(rated_at AS DATE) AS date,
    DATE_PART(day, rated_at) AS day,
    CASE DATE_PART(day, rated_at) + 1
        WHEN 0 THEN 'Nedeľa'
        WHEN 1 THEN 'Pondelok'
        WHEN 2 THEN 'Utorok'
        WHEN 3 THEN 'Streda'
        WHEN 4 THEN 'Štvrtok'
        WHEN 5 THEN 'Piatok'
        WHEN 6 THEN 'Sobota'
    END AS day_as_string,
    DATE_PART(month, rated_at) AS month,
    DATE_PART(year, rated_at) AS year,
FROM ratings_staging;

CREATE OR REPLACE TABLE fact_ratings AS
SELECT 
    r.id AS fact_ratingId,
    r.rated_at AS rated_at,
    u.dim_userId AS dim_userId,
    m.dim_moviesId AS dim_moviesId,
    d.dim_dateId AS dim_dateId,
    r.rating AS rating,
    LISTAGG(t.tags, ', ') WITHIN GROUP (ORDER BY t.tags) AS tags,
FROM ratings_staging r
JOIN dim_users u ON r.user_id = u.dim_userId
JOIN dim_movies m ON r.movie_id = m.dim_moviesId
JOIN dim_date d ON CAST(r.rated_at AS DATE) = d.date
LEFT JOIN tags_staging t ON r.user_id = t.user_id AND r.movie_id = t.movie_id
GROUP BY r.id, r.rated_at, u.dim_userId, m.dim_moviesId, d.dim_dateId, r.rating;



DROP TABLE IF EXISTS age_group_staging; 
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS users_staging;

    
    


