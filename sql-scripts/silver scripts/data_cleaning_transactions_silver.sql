/********************************************************************************************
  Procedure:   silver.pcd_cleaning_transactions_bronze_to_staging
  Purpose:     Cleans and standardizes transactions data from bronze layer into silver layer.
  Author:      Innocent Nhamo
  Created:     2025-10-18
  Version:     1.1
  Change Log:
      - v1.0 Initial creation
      - v1.1 Added data validation
********************************************************************************************/
 
USE banking_db;
GO 

-- Procedure 
CREATE OR ALTER PROCEDURE silver.pcd_cleaning_transactions_bronze_to_staging
AS 
BEGIN
	SET NOCOUNT ON; 

		PRINT '=================================================';
		PRINT 'Starting customer data cleaning process...';
		PRINT '=================================================';

		-- DROP EXISTING CLEANED TABLE IF IT EXISTS
		IF OBJECT_ID('silver.stg_transactions_cleaned', 'U') IS NOT NULL
			DROP TABLE silver.stg_transactions_cleaned;

		-- 
END;
GO