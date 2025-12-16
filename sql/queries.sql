-- =============================================
-- 1. AGGREGATIONS (COUNT, AVG, ETC.)
-- =============================================

-- Query 1: Count total Movies vs TV Shows
SELECT 
    type, 
    COUNT(*) as total_count
FROM shows
GROUP BY type;

-- Query 2: Find the top 10 most common ratings
SELECT 
    rating, 
    COUNT(*) as count
FROM shows
WHERE rating IS NOT NULL
GROUP BY rating
ORDER BY count DESC
LIMIT 10;

-- Query 3: Content added by Year (Time Series Report)
SELECT 
    EXTRACT(YEAR FROM date_added) as year_added,
    COUNT(*) as total_shows
FROM shows
WHERE date_added IS NOT NULL
GROUP BY year_added
ORDER BY year_added DESC;

-- =============================================
-- 2. JOIN-HEAVY QUERIES (Reports)
-- =============================================

-- Query 4: List Top 5 Countries with the most content
-- Joins: shows -> show_countries -> countries
SELECT 
    c.country_name,
    COUNT(sc.show_id) as total_content
FROM countries c
JOIN show_countries sc ON c.country_id = sc.country_id
GROUP BY c.country_name
ORDER BY total_content DESC
LIMIT 5;

-- Query 5: Find all movies listed as "Horror Movies"
-- Joins: shows -> show_genres -> genres
SELECT 
    s.title,
    s.release_year,
    g.genre_name
FROM shows s
JOIN show_genres sg ON s.show_id = sg.show_id
JOIN genres g ON sg.genre_id = g.genre_id
WHERE g.genre_name = 'Horror Movies'
  AND s.type = 'Movie'
ORDER BY s.release_year DESC
LIMIT 10;

-- Query 6: "The 3 Steps of Kevin Bacon" (Top 5 Actors with most movies)
-- Joins: actors -> show_cast
SELECT 
    a.actor_name,
    COUNT(sc.show_id) as movie_count
FROM actors a
JOIN show_cast sc ON a.actor_id = sc.actor_id
GROUP BY a.actor_name
ORDER BY movie_count DESC
LIMIT 5;

-- =============================================
-- 3. DATA QUALITY CHECKS (Detect Duplicates/Invalid)
-- =============================================

-- Query 7: Check for potential duplicate titles (Remakes or Data Issues)
SELECT 
    title, 
    COUNT(*) as duplicate_count
FROM shows
GROUP BY title
HAVING COUNT(*) > 1;

-- Query 8: Find shows with no Director listed (Missing Data Check)
SELECT COUNT(*) as shows_with_no_director
FROM shows s
LEFT JOIN show_directors sd ON s.show_id = sd.show_id
WHERE sd.director_id IS NULL;