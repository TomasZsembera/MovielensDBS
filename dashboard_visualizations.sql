-- 1. Top 10 najviac hodnotených filmov
SELECT 
    m.title AS movie_title,
    COUNT(f.fact_ratingId) AS total_ratings
FROM fact_ratings f
JOIN dim_movies m ON f.dim_moviesId = m.dim_moviesId
GROUP BY m.title
ORDER BY total_ratings DESC
LIMIT 10;

-- 2. Počet vydaných filmov pre každý žáner v roku 2000
SELECT 
    g.name AS genre,
    COUNT(m.dim_moviesId) AS movie_count
FROM dim_movies m
JOIN genres_movies_staging gm ON m.dim_moviesId = gm.movie_id
JOIN genres_staging g ON gm.genre_id = g.id
WHERE m.release_year = '2000'
GROUP BY g.name
ORDER BY movie_count DESC;

-- 3. Trend priemerného hodnotenia filmov podľa roku vydania
SELECT 
    m.release_year AS year,
    AVG(f.rating) AS avg_rating
FROM fact_ratings f
JOIN dim_movies m ON f.dim_moviesId = m.dim_moviesId
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year
ORDER BY m.release_year;

-- 4. Počet filmov podľa žánru
SELECT genre, COUNT(*) AS movie_count
FROM dim_movies
GROUP BY genre
ORDER BY movie_count DESC;

-- 5. Počet filmov podľa roku vydania
SELECT release_year, COUNT(*) AS movie_count
FROM dim_movies
GROUP BY release_year
ORDER BY release_year;

-- 6. Priemerné hodnotenie filmov podľa žánru
SELECT genre, AVG(rating) AS average_rating
FROM fact_ratings fr
JOIN dim_movies dm ON fr.dim_moviesId = dm.dim_moviesId
GROUP BY genre
ORDER BY average_rating DESC;