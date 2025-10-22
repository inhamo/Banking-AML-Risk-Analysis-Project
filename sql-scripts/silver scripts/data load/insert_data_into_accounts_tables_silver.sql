/********************************************************************************************
  Procedure:   silver.pcd_insert_data_accounts_tables_silver
  Purpose:     Loads data from staged cleaned accounts into normalized table structure
  Author:      Innocent Nhamo
  Created:     2025-10-18
  Version:     1.0
********************************************************************************************/
USE banking_db; 
GO

CREATE OR ALTER PROCEDURE silver.pcd_insert_data_accounts_tables_silver
AS 
BEGIN 
    SET NOCOUNT ON; 

        BEGIN TRANSACTION;

        PRINT 'Starting data transfer from staged accounts to normalized tables...';

        -- load the branch table
        TRUNCATE TABLE silver.dim_branch;
        INSERT INTO silver.dim_branch (branch_code)
        SELECT DISTINCT branch_code
        FROM silver.stg_accounts_cleaned
        WHERE branch_code IS NOT NULL;

        -- load the branch relationship table
        TRUNCATE TABLE silver.dim_account_branch;
        INSERT INTO silver.dim_account_branch (account_id, branch_id) 
        SELECT DISTINCT
            a.account_id,
            b.branch_id
        FROM silver.stg_accounts_cleaned a
        LEFT JOIN silver.dim_branch b 
        ON b.branch_code = a.branch_code;

        -- load the facts table 
        TRUNCATE TABLE silver.fact_accounts;
        INSERT INTO silver.fact_accounts (
            account_id, account_number, customer_id, is_primary_account, expected_amount, 
            currency, swift_code, iban, statement_frequency, online_banking_enabled, online_banking_activation_date, 
            days_to_online_banking_activation, cross_border_enabled, minimum_deposit_met, opening_channel, 
            account_value_segment)
        SELECT DISTINCT 
            account_id, 
            account_number, 
            customer_id, 
            is_primary_account, 
            expected_amount, 
            currency, 
            swift_code, 
            iban, 
            statement_frequency, 
            online_banking_enabled, 
            online_banking_activation_date, 
            DATEDIFF(DAY, acc_opening_date,online_banking_activation_date) days_to_online_banking_activation, 
            cross_border_enabled, 
            minimum_deposit_met, 
            opening_channel,
            CASE 
                WHEN expected_amount < 1000 THEN 'LOW VALUE' 
                WHEN expected_amount BETWEEN 1000 AND 5000 THEN 'MEDIUM VALUE'
                WHEN expected_amount BETWEEN 5001 AND 15000 THEN 'HIGH VALUE'
                ELSE 'PREMIUM VALUE'
            END account_value_segment
        FROM silver.stg_accounts_cleaned
        WHERE is_acc_id_null = 0 
            AND is_acc_number_null = 0 
            AND is_cust_id_null = 0
            AND is_invalid_online_banking_activation_date = 0; 

        -- load data into joint account 
        TRUNCATE TABLE silver.dim_joint_account
        INSERT INTO silver.dim_joint_account (account_id, joint_customer_id, added_date, is_active) 
        SELECT DISTINCT 
            account_id, 
            linked_joint_account AS joint_customer_id, 
            acc_opening_date AS added_date, 
            CASE 
                WHEN account_status = 'ACTIVE' THEN 1 
                ELSE 0 
            END is_active
        FROM silver.stg_accounts_cleaned
        WHERE linked_joint_account IS NOT NULL
            AND is_acc_id_null = 0 AND is_acc_number_null = 0 AND is_cust_id_null = 0; 

        -- load data into the documents account
        TRUNCATE TABLE silver.dim_account_documents; 
        INSERT INTO silver.dim_account_documents (
                            account_id, kyc_verified, fica_verified, proof_of_address_provided, bank_statements_provided, 
                            employer_letter_provided, business_registration_provided, tax_certificate_provided, 
                            verification_date, verification_status)
        SELECT DISTINCT 
            account_id, 
            kyc_verified, 
            fica_verified, 
            proof_of_address_provided, 
            bank_statements_provided, 
            employer_letter_provided, 
            business_registration_provided, 
            tax_certificate_provided, 
            acc_approval_date AS verification_date, 
            CASE WHEN acc_approval_date IS NOT NULL THEN 1 ELSE 0 END AS verification_status
        FROM silver.stg_accounts_cleaned;

        -- load data into account type
        TRUNCATE TABLE silver.dim_account_type; 
        INSERT INTO silver.dim_account_type (
                            account_id, account_type, account_purpose, account_tier, interest_rate, monthly_charges, transactions_rate, 
                            negative_balance_rate, overdraft_limit, credit_card_limit
        )
        SELECT DISTINCT 
            account_id, 
            account_type, 
            account_purpose, 
            account_tier, 
            interest_rate, 
            monthly_charges, 
            transactions_rate, 
            negative_balance_rate, 
            overdraft_limit, 
            credit_card_limit
        FROM silver.stg_accounts_cleaned
        WHERE is_acc_type_null = 0;

        -- loan data into account status 
        TRUNCATE TABLE silver.dim_account_status; 
        INSERT INTO silver.dim_account_status (
                    account_id, account_status, acc_opening_date, acc_approval_date, acc_status_change_date, acc_closing_date, 
                    status_description, days_to_approval, days_before_status_change, account_tenure
        )
        SELECT DISTINCT 
            account_id, 
            account_status, 
            acc_opening_date, 
            acc_approval_date, 
            acc_status_change_date, 
            acc_closing_date, 
            acc_status_change_reason AS status_description, 
            DATEDIFF(DAY, acc_opening_date, acc_approval_date) AS days_to_approval, 
            DATEDIFF(DAY, acc_approval_date, acc_status_change_date) AS days_before_status_change, 
            CASE 
                WHEN acc_closing_date IS NULL THEN 
                    DATEDIFF(DAY, acc_approval_date, GETDATE())
                ELSE 
                    DATEDIFF(DAY, acc_approval_date, acc_closing_date)
            END AS account_tenure
        FROM silver.stg_accounts_cleaned
        WHERE is_open_date_null = 0
            AND is_invalid_approval_date = 0 
            AND is_invalid_opening_date = 0 
            AND is_invalid_status_change_date = 0;

        -- load data into account products 
        TRUNCATE TABLE silver.dim_account_products;
        INSERT INTO silver.dim_account_products (account_id, account_product, date_added, is_active)
        SELECT DISTINCT 
            account_id, 
            account_product, 
            acc_opening_date AS date_added, 
            CASE 
                WHEN acc_closing_date IS NULL AND acc_status_change_date IS NULL THEN 1 
                ELSE 0 
            END AS is_active
        FROM silver.stg_accounts_cleaned
        WHERE account_product IS NOT NULL;

        -- load data into account cards 
        TRUNCATE TABLE silver.dim_account_cards;
        INSERT INTO silver.dim_account_cards (
                        account_id, card_number, card_type, card_issue_date, card_expiry_date,
                        card_status, daily_transaction_limit, international_usage_enabled
        )
        SELECT 
            account_id, 
            card_number, 
            card_type, 
            card_issue_date, 
            card_expiry_date, 
            CASE 
                WHEN acc_closing_date IS NULL AND acc_status_change_date IS NULL THEN 'ACTIVE'
                ELSE 'CLOSED ON ACCCOUNT'
            END AS card_status, 
            0.25 * expected_amount AS daily_transaction_limit, 
            CASE
                WHEN cross_border_enabled = 1 THEN 1
                ELSE 0 
            END AS international_usage_enabled
        FROM silver.stg_accounts_cleaned
        WHERE is_invalid_card_expiry_date = 0 
            AND is_invalid_card_issue_date = 0;

        -- load data into account beneficiary
        TRUNCATE TABLE silver.dim_account_beneficiaries 
        INSERT INTO silver.dim_account_beneficiaries (
                            account_id, beneficiary_name, beneficiary_relationship, beneficiary_percentage, 
                            date_added,is_active, verification_status 
        )
        SELECT DISTINCT
            account_id, 
            beneficiary_name, 
            beneficiary_relationship, 
            beneficiary_percentage, 
            acc_opening_date AS date_added,
            CASE 
                WHEN acc_closing_date IS NULL AND acc_status_change_date IS NULL THEN 1
                ELSE 0
            END AS is_active, 
            'VERIFIED' AS verification_status
        FROM silver.stg_accounts_cleaned
        WHERE beneficiary_name IS NOT NULL;

        COMMIT TRANSACTION;
        
        PRINT 'Data transfer completed successfully!';
END;
GO

EXEC silver.pcd_insert_data_accounts_tables_silver;