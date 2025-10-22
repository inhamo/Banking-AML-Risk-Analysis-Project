USE banking_db; 
GO

-- ===============================================================
--	Master Union Procedure
-- ================================================================
CREATE OR ALTER PROCEDURE master_union_bronze_to_staging
AS 
BEGIN 
	SET NOCOUNT ON;

	EXEC silver.pcd_union_customers_bronze_to_staging;
	EXEC silver.pcd_union_accounts_bronze_to_staging;
	EXEC silver.pcd_union_transactions_bronze_to_staging;

	-- Data Cleaning
	EXEC silver.pcd_cleaning_customers_silver_staging;
	EXEC silver.pcd_cleaning_accounts_bronze_to_staging;

	-- Creating normalization tables 
	EXEC silver.pcd_create_customer_tables_silver;
	EXEC silver.pcd_create_accounts_tables_silver;

	-- Load data into normalized table
	EXEC silver.pcd_insert_data_customers_tables_silver;
	EXEC silver.pcd_insert_data_accounts_tables_silver;

END;
GO

EXEC master_union_bronze_to_staging;