CREATE DATABASE PUMA_movielens_DB;

CREATE SCHEMA PUMA_MOVIELENS_DB.staging;

USE SCHEMA PUMA_MOVIELENS_DB.staging;

CREATE TABLE age_group_staging (
    id INT PRIMARY KEY,
    name VARCHAR(45)
);

select * from age_group_staging;

CREATE TABLE occupations_staging (
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

select * from movies_staging;


CREATE OR REPLACE TABLE users_staging (
    id INT PRIMARY KEY,
    age INT,
    gender CHAR(45),
    occupation_id INT,
    zip_code VARCHAR(255),
    FOREIGN KEY (age) REFERENCES age_group_staging(id),
    FOREIGN KEY (occupation_id) REFERENCES occupations_staging(id)
);

select * from users_staging;

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
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

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



CREATE OR REPLACE TABLE dim_movies AS
SELECT DISTINCT 
    m.id AS dim_movieid,
    m.title AS title,
    m.release_year AS release_year,
    g.name AS genre,
    COALESCE(LISTAGG(t.tags, ', ') WITHIN GROUP (ORDER BY t.tags), 'No Tags') AS tags
FROM movies_staging m
JOIN genres_movies_staging gr ON m.id = gr.movie_id
JOIN genres_staging g ON gr.genre_id = g.id
LEFT JOIN tags_staging t ON m.id = t.movie_id
GROUP BY m.id, m.title, m.release_year, g.name;


select * from dim_movies;


CREATE OR REPLACE TABLE dim_users AS
SELECT DISTINCT
    u.id AS dim_userid,
    ag.name AS age_group,
    u.gender,
    o.name AS occupation,
    u.zip_code
FROM users_staging u
JOIN age_group_staging ag ON u.age = ag.id
JOIN occupations_staging o ON u.occupation_id = o.id;

select * from dim_users;


CREATE OR REPLACE TABLE dim_tags AS
SELECT DISTINCT
    tg.id,
    tg.movie_id,
    tg.user_id,
    tg.tags AS tag,
    tg.created_at
FROM tags_staging tg;

select * from dim_tags;


CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE)) AS dim_dateId,
    CAST(rated_at AS DATE) AS date,
    DATE_PART(day, rated_at) AS day,
    CASE DATE_PART(dow, rated_at)
        WHEN 0 THEN 'Nedeľa'
        WHEN 1 THEN 'Pondelok'
        WHEN 2 THEN 'Utorok'
        WHEN 3 THEN 'Streda'
        WHEN 4 THEN 'Štvrtok'
        WHEN 5 THEN 'Piatok'
        WHEN 6 THEN 'Sobota'
    END AS day_as_string,
    DATE_PART(month, rated_at) AS month,
    DATE_PART(year, rated_at) AS year
FROM ratings_staging;


select * from dim_date;

SELECT * FROM dim_date LIMIT 10;



CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY DATE_PART(hour, rated_at), DATE_PART(minute, rated_at), DATE_PART(second, rated_at)) AS dim_timeid,
    DATE_PART(hour, rated_at) AS hour,
    DATE_PART(minute, rated_at) AS minute,
    DATE_PART(second, rated_at) AS second
FROM ratings_staging
GROUP BY 
    DATE_PART(hour, rated_at),
    DATE_PART(minute, rated_at),
    DATE_PART(second, rated_at)
ORDER BY 
    DATE_PART(hour, rated_at),
    DATE_PART(minute, rated_at),
    DATE_PART(second, rated_at);

select * from dim_time;


CREATE OR REPLACE TABLE fact_rating AS
SELECT
    r.id AS fact_ratingid,
    r.rated_at AS rated_at,
    r.rating,
    COALESCE(LISTAGG(tg.movie_id || '-' || tg.user_id, ',') WITHIN GROUP (ORDER BY tg.movie_id, tg.user_id), '') AS tags,
    d.dim_dateid AS dim_dateid,
    t.dim_timeid AS dim_timeid,
    u.dim_userid AS dim_userid,
    m.dim_movieid AS dim_movieid
FROM ratings_staging r
JOIN dim_date d ON CAST(r.rated_at AS DATE) = d.date
JOIN dim_time t ON DATE_PART(hour, r.rated_at) = t.hour
                AND DATE_PART(minute, r.rated_at) = t.minute
                AND DATE_PART(second, r.rated_at) = t.second
LEFT JOIN dim_users u ON r.user_id = u.dim_userid
LEFT JOIN dim_movies m ON r.movie_id = m.dim_movieid
LEFT JOIN dim_tags tg ON r.movie_id = tg.movie_id
GROUP BY r.id, r.rated_at, r.rating, d.dim_dateid, t.dim_timeid, u.dim_userid, m.dim_movieid
ORDER BY r.id;


select * from fact_rating;

DROP TABLE IF EXISTS age_group_staging; 
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS users_staging;

    
    


