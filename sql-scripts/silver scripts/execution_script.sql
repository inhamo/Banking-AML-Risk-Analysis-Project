-- =============================================
-- Create or recreated schema if needed
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
    PRINT 'Created schema: silver';
END
ELSE
    PRINT 'Schema silver already exists.';
GO

-- Union or merge related data
EXEC master_silver_layer_procedures;