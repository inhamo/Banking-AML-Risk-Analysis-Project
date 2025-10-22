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
        BEGIN
			DROP TABLE silver.stg_transactions_cleaned
        END;

        SELECT DISTINCT
            CAST(UPPER(TRIM(fn.transaction_id)) AS NVARCHAR(50)) AS transaction_id, 
            CAST(UPPER(TRIM(fn.account_id)) AS NVARCHAR(50)) AS account_id,
            CAST(ac.account_number AS NVARCHAR(50)) AS sending_account_number,
            CAST(NULL AS NVARCHAR(50)) AS receiving_account_number, -- Placeholder
            CAST(fn.transaction_date AS DATETIME) AS transaction_date,
            CAST(fn.transaction_time AS DATETIME) AS transaction_time,
            CAST(ABS(fn.amount) AS DECIMAL(15,4)) AS transaction_amount,
            CAST(UPPER(TRIM(fn.debit_credit)) AS NVARCHAR(10)) AS transaction_type,
            CAST(UPPER(TRIM(fn.category)) AS NVARCHAR(50)) AS transaction_category,
            CAST(UPPER(TRIM(fn.status)) AS NVARCHAR(20)) AS transaction_status,
            CAST(TRIM(fn.description) AS NVARCHAR(500)) AS transaction_description,
            CAST(fn.immediate_payment AS BIT) AS is_immediate_payment,
            CAST(
                CASE 
                    WHEN fn.account_id = fn.receiving_account THEN ''
                    ELSE fn.receiving_account
                END AS NVARCHAR(50)
            ) AS new_receiving_account,
            CAST(
                CASE 
                    WHEN fn.account_id = fn.receiving_account AND (UPPER(fn.receiving_bank) = 'SAME BANK' OR  UPPER(fn.receiving_bank) = 'BANK')THEN ''
                    WHEN UPPER(fn.receiving_bank) = 'SAME BANK' OR  UPPER(fn.receiving_bank) = 'BANK'THEN ''
                    ELSE fn.receiving_bank
                END AS NVARCHAR(100)
            ) AS new_receiving_bank, 
            CAST(COALESCE(fn.transaction_cost, 0) AS DECIMAL(5, 2)) AS transaction_cost,
            CAST(UPPER(TRIM(REPLACE(fn.channel, '_', ' '))) AS NVARCHAR(50)) AS transaction_channel, 
            CAST(UPPER(TRIM(fn.merchant_name)) AS NVARCHAR(255)) AS merchant_name,
            CAST(fn.receiving_phone_number AS NVARCHAR(100)) AS receiving_phone_number,
            CAST(fn._source_file_url AS NVARCHAR(1000)) AS _source_file_url,
            CAST(fn._ingestion_timestamp AS DATETIME) AS _ingestion_timestamp,
            CAST(fn._source_hash AS NVARCHAR(32)) AS _source_hash,
            CAST(GETDATE() AS DATETIME) AS _created_at
        INTO silver.stg_transactions_cleaned 
        FROM silver.stg_transactions fn
        JOIN silver.stg_accounts_cleaned ac
            ON ac.account_id = fn.account_id
        -- LEFT JOIN silver.stg_accounts_cleaned rac
        -- ON rac.account_id = fn.new_receiving_account
        WHERE fn.transaction_id IS NOT NULL 
            AND fn.account_id IS NOT NULL
            AND fn.amount IS NOT NULL 
            AND fn.amount != 0;

        -- Now let's update the receiving_account_number separately
        PRINT 'Step 2: Updating receiving_account_number...';
        UPDATE fn
        SET receiving_account_number = CAST(
            CASE 
                WHEN t.receiving_bank IS NOT NULL AND t.receiving_account NOT LIKE 'ACC%' THEN t.receiving_account 
                ELSE rac.account_number 
            END AS NVARCHAR(50)
        )
        FROM silver.stg_transactions_cleaned fn
        JOIN silver.stg_transactions t ON t.transaction_id = fn.transaction_id
        LEFT JOIN silver.stg_accounts_cleaned rac ON rac.account_id = t.receiving_account;

        -- Create indexes after data load
        PRINT 'Creating indexes...';
        CREATE INDEX IX_transactions_account_id ON silver.stg_transactions_cleaned (account_id);
        CREATE INDEX IX_transactions_date ON silver.stg_transactions_cleaned (transaction_date);
        CREATE INDEX IX_transactions_id ON silver.stg_transactions_cleaned (transaction_id);

        PRINT '=================================================';
        PRINT 'Transactions data cleaning process completed!';
        PRINT '=================================================';

END;
GO

EXEC silver.pcd_cleaning_transactions_bronze_to_staging