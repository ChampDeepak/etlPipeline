-- =============================================
-- 1. VIEW: Content Summary (Simplifies Complex Joins)
-- =============================================
-- Usage: SELECT * FROM v_content_summary WHERE title LIKE '%Matrix%';

CREATE OR REPLACE VIEW v_content_summary AS
SELECT 
    s.show_id,
    s.title,
    s.type,
    s.release_year,
    s.rating,
    -- Aggregate multiple directors into a single string
    STRING_AGG(DISTINCT d.director_name, ', ') as directors,
    -- Aggregate multiple genres into a single string
    STRING_AGG(DISTINCT g.genre_name, ', ') as genres,
    -- Aggregate multiple countries into a single string
    STRING_AGG(DISTINCT c.country_name, ', ') as countries
FROM shows s
LEFT JOIN show_directors sd ON s.show_id = sd.show_id
LEFT JOIN directors d ON sd.director_id = d.director_id
LEFT JOIN show_genres sg ON s.show_id = sg.show_id
LEFT JOIN genres g ON sg.genre_id = g.genre_id
LEFT JOIN show_countries sc ON s.show_id = sc.show_id
LEFT JOIN countries c ON sc.country_id = c.country_id
GROUP BY s.show_id, s.title, s.type, s.release_year, s.rating;

-- =============================================
-- 2. VIEW: Genre Statistics (Reporting View)
-- =============================================
-- Usage: SELECT * FROM v_genre_stats ORDER BY total_titles DESC;

CREATE OR REPLACE VIEW v_genre_stats AS
SELECT 
    g.genre_name,
    COUNT(s.show_id) as total_titles,
    -- Calculate average release year to see if genre is "modern" or "classic"
    ROUND(AVG(s.release_year)) as avg_release_year
FROM genres g
JOIN show_genres sg ON g.genre_id = sg.genre_id
JOIN shows s ON sg.show_id = s.show_id
GROUP BY g.genre_name;