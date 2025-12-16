-- =============================================
-- QUERY OPTIMIZATION DEMO
-- Objective: Demonstrate performance difference between Sequential Scan and Index Scan.
-- =============================================

-- 1. SETUP: Temporarily drop the index on 'release_year' to simulate a slow database
-- (We created this index in Task 3, so we remove it now to see the "Before" state)
DROP INDEX IF EXISTS idx_shows_year;

-- 2. "BEFORE" ANALYSIS (Slow)
-- Run this and look for "Seq Scan" (Sequential Scan) in the output.
-- This means the DB is reading every single row (8,807 rows) to find the answer.
EXPLAIN ANALYZE 
SELECT * FROM shows WHERE release_year = 2020;

-- =============================================
-- [PAUSE HERE: Take a Screenshot of the "Seq Scan" output]
-- =============================================

-- 3. OPTIMIZATION: Create the Index
-- A B-Tree index sorts the years, allowing the DB to jump directly to "2020".
CREATE INDEX idx_shows_year ON shows(release_year);

-- 4. "AFTER" ANALYSIS (Fast)
-- Run this and look for "Index Scan" or "Bitmap Heap Scan".
-- This means the DB jumped straight to the specific rows.
EXPLAIN ANALYZE 
SELECT * FROM shows WHERE release_year = 2020;

-- =============================================
-- [PAUSE HERE: Take a Screenshot of the "Index Scan" output]
-- =============================================