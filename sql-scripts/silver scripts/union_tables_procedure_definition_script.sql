USE banking_db;
GO

/* ===============================================================
      PROCEDURE: Combine Customer Data from Different Years
   =============================================================== */
CREATE OR ALTER PROCEDURE silver.pcd_union_customers_bronze_to_staging
AS
BEGIN
    SET NOCOUNT ON;

    -- Create a staging table (if it doesn't exist)
    IF NOT EXISTS (
        SELECT * FROM sys.tables 
        WHERE name = 'stg_customers'
          AND schema_id = SCHEMA_ID('silver')
    )
    BEGIN
        SELECT *
        INTO silver.stg_customers
        FROM bronze.crm_customers_2023
        WHERE 1 = 0;  -- Creates empty table with same structure
    END;

    -- Clear staging table
    TRUNCATE TABLE silver.stg_customers;

    -- Union data into staging
    INSERT INTO silver.stg_customers
    SELECT * FROM bronze.crm_customers_2023
    UNION ALL
    SELECT * FROM bronze.crm_customers_2024;

    PRINT 'Customers union completed: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';
END;
GO


/* ===============================================================
      PROCEDURE: Combine Account Data from Different Years
   =============================================================== */
CREATE OR ALTER PROCEDURE silver.pcd_union_accounts_bronze_to_staging
AS
BEGIN
    SET NOCOUNT ON;

    -- Create a staging table (if it doesn�t exist)
    IF NOT EXISTS (
        SELECT * FROM sys.tables 
        WHERE name = 'stg_accounts'
          AND schema_id = SCHEMA_ID('silver')
    )
    BEGIN
        SELECT *
        INTO silver.stg_accounts
        FROM bronze.erp_accounts_2023
        WHERE 1 = 0;
    END;

    -- Clear staging table
    TRUNCATE TABLE silver.stg_accounts;

    -- Union data into staging
    INSERT INTO silver.stg_accounts
    SELECT * FROM bronze.erp_accounts_2023
    UNION ALL
    SELECT * FROM bronze.erp_accounts_2024;

    PRINT 'Accounts union completed: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';
END;
GO


/* ===============================================================
      PROCEDURE: Combine Transaction Data from Different Years
   =============================================================== */
CREATE OR ALTER PROCEDURE silver.pcd_union_transactions_bronze_to_staging
AS
BEGIN
    SET NOCOUNT ON;

    -- Drop table if it exists 
    IF EXISTS (
        SELECT * FROM sys.tables 
        WHERE name = 'stg_transactions'
          AND schema_id = SCHEMA_ID('silver')
    )
    BEGIN
        DROP TABLE silver.stg_transactions
    END;
     
     -- Create staging table with all possible columns
    CREATE TABLE silver.stg_transactions (
        -- Common columns
        transaction_id        NVARCHAR(50),
        account_id            NVARCHAR(50),
        transaction_date      DATETIME,
        transaction_time      DATETIME,
        amount                DECIMAL(15,4),
        debit_credit          NVARCHAR(10),
        category              NVARCHAR(50),
        status                NVARCHAR(20),
        description           NVARCHAR(500),
        immediate_payment     BIT,
        receiving_account     NVARCHAR(50),
        receiving_bank        NVARCHAR(50),
        transaction_cost      DECIMAL(15,4),
        channel               NVARCHAR(100),
        merchant_name         NVARCHAR(255),
        receiving_phone_number NVARCHAR(100),
        ewallet_number         NVARCHAR(100), 
        loan_type              NVARCHAR(100), 

        -- Metadata
        _source_file_url       NVARCHAR(1000),
        _ingestion_timestamp   DATETIME,
        _source_hash           NVARCHAR(32),
        source_file_type       NVARCHAR(10)  
    );

    -- Insert from Transactions_00
    INSERT INTO silver.stg_transactions
    SELECT 
	    transaction_id, account_id, transaction_date, transaction_time, 
	    amount, debit_credit, category, status, description, immediate_payment, 
	    receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	    receiving_phone_number, NULL AS ewallet_number, NULL as loan_type, 
        _source_file_url, _ingestion_timestamp, _source_hash, 
        '2023_00' AS source_file_type
    FROM bronze.erp_transactions_2023_00
    UNION ALL
    SELECT
	    transaction_id, account_id, transaction_date, transaction_time, 
	    amount, debit_credit, category, status, description, immediate_payment, 
	    receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	    receiving_phone_number, NULL AS ewallet_number, NULL as loan_type,
        _source_file_url, _ingestion_timestamp, _source_hash, 
        '2024_00' AS source_file_type
    FROM bronze.erp_transactions_2024_00

    -- Insert from Transactions_01 to 12 (both 2023 & 2024)
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, NULL AS ewallet_number,  NULL AS loan_type,
            _source_file_url,_ingestion_timestamp, _source_hash, 
            '2023_01' AS source_file_type
        FROM bronze.erp_transactions_2023_01
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2023_02' AS source_file_type
        FROM bronze.erp_transactions_2023_02
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2023_03' AS source_file_type
        FROM bronze.erp_transactions_2023_03
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2023_04' AS source_file_type
        FROM bronze.erp_transactions_2023_04
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2023_05' AS source_file_type
        FROM bronze.erp_transactions_2023_05
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2023_06' AS source_file_type
        FROM bronze.erp_transactions_2023_06
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2023_07' AS source_file_type
        FROM bronze.erp_transactions_2023_07
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2023_08' AS source_file_type
        FROM bronze.erp_transactions_2023_08
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2023_09' AS source_file_type
        FROM bronze.erp_transactions_2023_09
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2023_10' AS source_file_type
        FROM bronze.erp_transactions_2023_10
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2023_11' AS source_file_type
        FROM bronze.erp_transactions_2023_11
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2023_12' AS source_file_type
        FROM bronze.erp_transactions_2023_12
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2024_01' AS source_file_type
        FROM bronze.erp_transactions_2024_01
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2024_02' AS source_file_type
        FROM bronze.erp_transactions_2024_02
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2024_03' AS source_file_type
        FROM bronze.erp_transactions_2024_03
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2024_04' AS source_file_type
        FROM bronze.erp_transactions_2024_04
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2024_05' AS source_file_type
        FROM bronze.erp_transactions_2024_05
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2024_06' AS source_file_type
        FROM bronze.erp_transactions_2024_06
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2024_07' AS source_file_type
        FROM bronze.erp_transactions_2024_07
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2024_08' AS source_file_type
        FROM bronze.erp_transactions_2024_08
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2024_09' AS source_file_type
        FROM bronze.erp_transactions_2024_09
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2024_10' AS source_file_type
        FROM bronze.erp_transactions_2024_10
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2024_11' AS source_file_type
        FROM bronze.erp_transactions_2024_11
    UNION ALL 
        SELECT 
	        transaction_id, account_id, transaction_date, transaction_time, 
	        amount, debit_credit, category, status, description, immediate_payment, 
	        receiving_account, receiving_bank, transaction_cost, channel, merchant_name, 
	        receiving_phone_number, ewallet_number, loan_type, _source_file_url, 
            _ingestion_timestamp, _source_hash, 
            '2024_12' AS source_file_type
        FROM bronze.erp_transactions_2024_12
    PRINT 'Transactions union completed: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';
END;
GO

