/********************************************************************************************
  Procedure:   silver.pcd_cleaning_accounts_bronze_to_staging
  Purpose:     Cleans and standardizes account data from bronze layer into silver layer.
  Author:      Innocent Nhamo
  Created:     2025-10-18
  Version:     1.1
  Change Log:
      - v1.0 Initial creation
      - v1.1 Added data validation, deduplication, and beneficiary parsing logic
********************************************************************************************/
 
USE banking_db;
GO 

-- Procedure 
CREATE OR ALTER PROCEDURE silver.pcd_cleaning_accounts_bronze_to_staging
AS 
BEGIN
	SET NOCOUNT ON; 

		PRINT '=================================================';
		PRINT 'Starting customer data cleaning process...';
		PRINT '=================================================';

		-- DROP EXISTING CLEANED TABLE IF IT EXISTS
		IF OBJECT_ID('silver.stg_accounts_cleaned', 'U') IS NOT NULL
			DROP TABLE silver.stg_accounts_cleaned;

		-- CTE account number duplication check 
		WITH CTE_null_check AS (
			SELECT *, 
				CASE WHEN account_id IS NULL THEN 1 ELSE 0 END is_acc_id_null, 
				CASE WHEN account_number IS NULL THEN 1 ELSE 0 END is_acc_number_null, 
				CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END is_cust_id_null, 
				CASE WHEN account_type IS NULL THEN 1 ELSE 0 END is_acc_type_null, 
				CASE WHEN opening_date IS NULL THEN 1 ELSE 0 END is_open_date_null
			FROM silver.stg_accounts
		),
		CTE_duplication_check AS (
			SELECT 
				*, 
				COUNT(*) OVER(PARTITION BY account_number) is_acc_number_duplicated
			FROM CTE_null_check
		), 
		CTE_dates_validations AS (
			SELECT a.*, 
				CASE WHEN a.approval_date < a.opening_date THEN 1 ELSE 0 END is_invalid_approval_date,
				CASE WHEN a.opening_date < c.date_of_entry THEN 1 ELSE 0 END is_invalid_opening_date, 
				CASE WHEN 
					a.status_change_date < a.opening_date 
					OR a.status_change_date < a.approval_date THEN 1 ELSE 0 END is_invalid_status_change_date,
				CASE WHEN 
					a.online_banking_activation_date < a.approval_date THEN 1 ELSE 0 
				END is_invalid_online_banking_activation_date,
				CASE WHEN 
					a.card_expiry_date < a.card_issue_date THEN 1 ELSE 0 END is_invalid_card_expiry_date,
				CASE WHEN 
					a.card_issue_date < a.opening_date THEN 1 ELSE 0 END is_invalid_card_issue_date
			FROM CTE_duplication_check a
			LEFT JOIN silver.stg_customers c 
			ON c.customer_id = a.customer_id
		), 
		CTE_cross_apply_comma_columns AS (
			SELECT dv.*, 
			l.value AS linked_joint_account, 
			bp.value AS account_product, 
			bf.value AS account_beneficiary
			FROM CTE_dates_validations dv
			OUTER APPLY STRING_SPLIT(dv.linked_joint_accounts, ';', 1) l
			OUTER APPLY STRING_SPLIT(dv.bundled_products, ';', 1) bp
			OUTER APPLY STRING_SPLIT(dv.beneficiaries, ';', 1) bf
		),
		CTE_beneficiary_parsing AS (
			SELECT *,
				-- Use JSON or XML parsing for more reliable splitting
				JSON_VALUE('["' + REPLACE(account_beneficiary, '|', '","') + '"]', '$[0]') AS beneficiary_name,
				JSON_VALUE('["' + REPLACE(account_beneficiary, '|', '","') + '"]', '$[1]') AS beneficiary_relationship,
				JSON_VALUE('["' + REPLACE(account_beneficiary, '|', '","') + '"]', '$[2]') AS beneficiary_percentage
			FROM CTE_cross_apply_comma_columns
		)
		SELECT 
			TRIM(account_id) AS account_id, 
			account_number, 
			TRIM(customer_id) AS customer_id, 
			UPPER(TRIM(account_type)) AS account_type, 
			REPLACE(
				CONCAT(
					UPPER(SUBSTRING(COALESCE(account_purpose, ''), 1, 1)),
					LOWER(SUBSTRING(COALESCE(account_purpose, ''), 2, LEN(COALESCE(account_purpose, ''))))
				), '_', ' '
			) AS account_purpose,
			CAST(is_primary_account AS BIT) AS is_primary_account, 
			TRY_CAST(opening_date AS DATE) AS acc_opening_date,
			TRY_CAST(approval_date AS DATE) AS acc_approval_date, 
			UPPER(TRIM(branch_code)) AS branch_code, 
			CAST(kyc_verified AS BIT) AS kyc_verified, 
			CAST(fica_verified AS BIT) AS fica_verified, 
			expected_amount,
			UPPER(TRIM(account_status)) AS account_status,
			TRY_CAST(status_change_date AS DATE) AS acc_status_change_date,
			TRY_CAST(closure_date AS DATE) AS acc_closing_date,
			UPPER(REPLACE(COALESCE(status_reason, ''), '_', ' ')) AS acc_status_change_reason,
			UPPER(TRIM(linked_joint_accounts)) AS linked_joint_accounts,
			UPPER(TRIM(linked_joint_account)) AS linked_joint_account,
			interest_rate,
			monthly_charges,
			transactions_rate,
			negative_balance_rate,
			overdraft_limit,
			credit_card_limit,
			bundled_products,
			UPPER(TRIM(REPLACE(account_product, '_', ' '))) AS account_product,
			UPPER(TRIM(currency)) AS currency,
			UPPER(TRIM(swift_code)) AS swift_code,
			UPPER(TRIM(iban)) AS iban,
			UPPER(TRIM(account_tier)) AS account_tier,
			UPPER(TRIM(statement_frequency)) AS statement_frequency,
			CAST(online_banking_enabled AS BIT) AS online_banking_enabled,
			TRY_CAST(online_banking_activation_date AS DATE) AS online_banking_activation_date,
			card_number,
			UPPER(TRIM(REPLACE(card_type, '_', ' '))) AS card_type,
			TRY_CAST(card_issue_date AS DATE) AS card_issue_date,
			TRY_CAST(card_expiry_date AS DATE) AS card_expiry_date,
			TRIM(beneficiaries) AS beneficiaries,
			UPPER(TRIM(beneficiary_name)) AS beneficiary_name,
			UPPER(TRIM(beneficiary_relationship)) AS beneficiary_relationship,
			beneficiary_percentage,
			CAST(cross_border_enabled AS BIT) AS cross_border_enabled,
			CAST(proof_of_income_provided AS BIT) AS proof_of_income_provided,
			CAST(CAST(proof_of_address_provided AS VARCHAR) AS BIT) AS proof_of_address_provided,
			CAST(bank_statements_provided AS BIT) AS bank_statements_provided,
			CAST(employer_letter_provided AS BIT) AS employer_letter_provided,
			CAST(business_registration_provided AS BIT) AS business_registration_provided,
			CAST(tax_certificate_provided AS BIT) AS tax_certificate_provided,
			CAST(minimum_deposit_met AS BIT) AS minimum_deposit_met,
			UPPER(TRIM(opening_channel)) AS opening_channel,
			is_acc_id_null,
			is_acc_number_null,
			is_cust_id_null,
			is_acc_type_null,
			is_open_date_null,
			is_acc_number_duplicated,
			is_invalid_approval_date,
			is_invalid_opening_date,
			is_invalid_status_change_date,
			is_invalid_online_banking_activation_date,
			is_invalid_card_expiry_date,
			is_invalid_card_issue_date,
			_source_file_url,
			_ingestion_timestamp,
			_source_hash, 
			GETDATE() AS _cleaning_date
		INTO silver.stg_accounts_cleaned
		FROM CTE_beneficiary_parsing;
END;
GO

EXEC silver.pcd_cleaning_accounts_bronze_to_staging;