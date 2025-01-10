-- 1. Top 10 najviac hodnotených filmov
SELECT 
    m.title AS movie_title,
    COUNT(f.fact_ratingId) AS total_ratings
FROM fact_ratings f
JOIN dim_movies m ON f.dim_moviesId = m.dim_moviesId
GROUP BY m.title
ORDER BY total_ratings DESC
LIMIT 10;

-- 2. Najčastejšie používané tagy (TOP 10)
SELECT 
    t.tag AS tag, 
    COUNT(t.dim_tagid) AS num_uses
FROM dim_tags t
GROUP BY t.tag
ORDER BY num_uses DESC
LIMIT 10;
-- 3. Trend priemerného hodnotenia filmov podľa roku vydania
SELECT 
    m.release_year AS year,
    AVG(f.rating) AS avg_rating
FROM fact_ratings f
JOIN dim_movies m ON f.dim_moviesId = m.dim_moviesId
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year
ORDER BY m.release_year;

-- 4. TOP 10 filmov podľa priemerného hodnotenia
SELECT 
    m.title AS movie_title, 
    ROUND(AVG(r.rating), 2) AS average_rating, 
    COUNT(r.rating) AS num_ratings
FROM fact_rating r
JOIN dim_movies m ON r.dim_movieid = m.dim_movieid
GROUP BY m.title
HAVING COUNT(r.rating) >= 50
ORDER BY average_rating DESC
LIMIT 10;

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