-- =============================================
-- STORED PROCEDURE: Add New Show
-- =============================================
-- Automates inserting a basic show record.
-- Usage: CALL add_show('s9999', 'Movie', 'My New Movie', 2024);

CREATE OR REPLACE PROCEDURE add_show(
    p_show_id VARCHAR,
    p_type VARCHAR,
    p_title VARCHAR,
    p_year INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Insert the new show with minimal required fields
    INSERT INTO shows (show_id, type, title, release_year, date_added)
    VALUES (p_show_id, p_type, p_title, p_year, CURRENT_DATE)
    ON CONFLICT (show_id) DO NOTHING;
    
    -- Optional: Log the action (if you had a logs table)
    RAISE NOTICE 'Show % added successfully.', p_title;
END;
$$;