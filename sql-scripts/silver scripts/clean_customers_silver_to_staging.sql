/*************************************************************************************
Script		: clean_customers_silver_to_staging.sql
Purpose		: ETL procedure to load bronze messy data into a unionized stg_customers
			  standardize text, solve for null values and duplicates 
Author		: Innocent Nhamo
Created On	: 2025-10-29
Version		: 
			  v.1 - Initial creation 
**************************************************************************************/
USE banking_db;
GO

-- Procedure 
CREATE OR ALTER PROCEDURE silver.pcd_clean_customer_to_staging
AS 
BEGIN 
	SET NOCOUNT ON;

	/***********************************************
		Declare variables
	***********************************************/
	DECLARE
		@StartTime DATETIME = SYSDATETIME(), 
		@RowsInserted INT = 0, 
		@ErrorMessage NVARCHAR(4000), 
		@BatchID UNIQUEIDENTIFIER = NEWID();

	BEGIN TRY
        /********************************************
			Begin logging for audit trail
        *********************************************/

		-- Create the error log table if it does not exist 

		IF OBJECT_ID('error_log.etl_log', 'U') IS NULL 
		BEGIN 
			CREATE TABLE error_log.etl_log (
				log_id INT IDENTITY(1,1) PRIMARY KEY,
				batch_id UNIQUEIDENTIFIER NOT NULL,
				procedure_name VARCHAR(200) NOT NULL,
				start_time DATETIME NOT NULL,
				end_time DATETIME NULL,
				status VARCHAR(50) NOT NULL,
				rows_processed INT NOT NULL DEFAULT 0,
				error_message NVARCHAR(MAX) NULL,
				created_at DATETIME DEFAULT GETDATE()
			);
		END;

		INSERT INTO error_log.etl_log(batch_id, procedure_name, start_time, status)
		VALUES (@BatchID, 'pcd_clean_customers_to_staging', @StartTime, 'STARTED')

        /********************************************
			Data cleaning pipeline
        *********************************************/

		-- Drop temp table if it already exists
		IF OBJECT_ID('tempdb..#stg_customers') IS NOT NULL
			DROP TABLE #stg_customers;

		-- Union tables and insert into a temporaty table
		PRINT 'Union all tables...';
		SELECT * 
		INTO #customers_temp
		FROM (
			SELECT 
				customer_id, customer_type, full_name, birth_date, citizenship,
				residential_address, commercial_address, email, phone_number, 
				id_type, id_number, expiry_date, visa_type, visa_expiry_date, 
				is_pep, pep_type, sanctioned_country, risk_score, tax_id_number, occupation, 
				employer_name, source_of_funds, marital_status, nationality, gender, 
				preferred_contact_method, next_of_kin, date_of_entry, annual_income, 
				education_level, ethnicity, is_director, director_of_companies, CAST(trading_name AS NVARCHAR(MAX)) trading_name,  
				principal_place_of_business, registered_office_address, CAST(trading_name AS NVARCHAR(50)) website, has_sanctioned_relationship, 
				CAST(fatca_giin AS NVARCHAR(MAX)) fatca_giin, swift_bic_code, industry, date_of_incorporation, city_of_incorporation, country_of_incorporation, 
				annual_turnover, company_age, number_of_employees, directors_count, shareholders_count, bee_level, vat_registered, 
				industry_risk_rating, is_publicly_listed, external_auditor, legal_representative_name, contact_person_name, 
				contact_person_title, contact_person_phone, contact_person_email,contact_person_is_customer, contact_person_customer_id, 
				num_branches_local, num_branches_foreign, num_subsidiaries_local, num_subsidiaries_foreign, requires_license, 
				CAST(license_info AS NVARCHAR(MAX)) AS license_info, is_supervised_aml_cft, director_customer_ids, major_shareholders, 
				_source_file_url, _ingestion_timestamp, _source_hash
			FROM bronze.crm_customers_2018
			UNION ALL
			SELECT 
				customer_id, customer_type, full_name, birth_date, citizenship,
				residential_address, commercial_address, email, phone_number, 
				id_type, id_number, expiry_date, visa_type, visa_expiry_date, 
				is_pep, pep_type, sanctioned_country, risk_score, tax_id_number, occupation, 
				employer_name, source_of_funds, marital_status, nationality, gender, 
				preferred_contact_method, next_of_kin, date_of_entry, annual_income, 
				education_level, ethnicity, is_director, director_of_companies, CAST(trading_name AS NVARCHAR(MAX)) trading_name,  
				principal_place_of_business, registered_office_address, CAST(trading_name AS NVARCHAR(50)) website, has_sanctioned_relationship, 
				CAST(fatca_giin AS NVARCHAR(MAX)) fatca_giin, swift_bic_code, industry, date_of_incorporation, city_of_incorporation, country_of_incorporation, 
				annual_turnover, company_age, number_of_employees, directors_count, shareholders_count, bee_level, vat_registered, 
				industry_risk_rating, is_publicly_listed, external_auditor, legal_representative_name, contact_person_name, 
				contact_person_title, contact_person_phone, contact_person_email,contact_person_is_customer, contact_person_customer_id, 
				num_branches_local, num_branches_foreign, num_subsidiaries_local, num_subsidiaries_foreign, requires_license, 
				CAST(license_info AS NVARCHAR(MAX)) AS license_info, is_supervised_aml_cft, director_customer_ids, major_shareholders, 
				_source_file_url, _ingestion_timestamp, _source_hash
			FROM bronze.crm_customers_2019 
			UNION ALL
			SELECT  
				customer_id, customer_type, full_name, birth_date, citizenship,
				residential_address, commercial_address, email, phone_number, 
				id_type, id_number, expiry_date, visa_type, visa_expiry_date, 
				is_pep, pep_type, sanctioned_country, risk_score, tax_id_number, occupation, 
				employer_name, source_of_funds, marital_status, nationality, gender, 
				preferred_contact_method, next_of_kin, date_of_entry, annual_income, 
				education_level, ethnicity, is_director, director_of_companies, CAST(trading_name AS NVARCHAR(MAX)) trading_name,  
				principal_place_of_business, registered_office_address, CAST(trading_name AS NVARCHAR(50)) website, has_sanctioned_relationship, 
				CAST(fatca_giin AS NVARCHAR(MAX)) fatca_giin, swift_bic_code, industry, date_of_incorporation, city_of_incorporation, country_of_incorporation, 
				annual_turnover, company_age, number_of_employees, directors_count, shareholders_count, bee_level, vat_registered, 
				industry_risk_rating, is_publicly_listed, external_auditor, legal_representative_name, contact_person_name, 
				contact_person_title, contact_person_phone, contact_person_email,contact_person_is_customer, contact_person_customer_id, 
				num_branches_local, num_branches_foreign, num_subsidiaries_local, num_subsidiaries_foreign, requires_license, 
				CAST(license_info AS NVARCHAR(MAX)) AS license_info, is_supervised_aml_cft, director_customer_ids, major_shareholders, 
				_source_file_url, _ingestion_timestamp, _source_hash
			FROM bronze.crm_customers_2020
			UNION ALL 
			SELECT  
				customer_id, customer_type, full_name, birth_date, citizenship,
				residential_address, commercial_address, email, phone_number, 
				id_type, id_number, expiry_date, visa_type, visa_expiry_date, 
				is_pep, pep_type, sanctioned_country, risk_score, tax_id_number, occupation, 
				employer_name, source_of_funds, marital_status, nationality, gender, 
				preferred_contact_method, next_of_kin, date_of_entry, annual_income, 
				education_level, ethnicity, is_director, director_of_companies, CAST(trading_name AS NVARCHAR(MAX)) trading_name,  
				principal_place_of_business, registered_office_address, CAST(trading_name AS NVARCHAR(50)) website, has_sanctioned_relationship, 
				CAST(fatca_giin AS NVARCHAR(MAX)) fatca_giin, swift_bic_code, industry, date_of_incorporation, city_of_incorporation, country_of_incorporation, 
				annual_turnover, company_age, number_of_employees, directors_count, shareholders_count, bee_level, vat_registered, 
				industry_risk_rating, is_publicly_listed, external_auditor, legal_representative_name, contact_person_name, 
				contact_person_title, contact_person_phone, contact_person_email,contact_person_is_customer, contact_person_customer_id, 
				num_branches_local, num_branches_foreign, num_subsidiaries_local, num_subsidiaries_foreign, requires_license, 
				CAST(license_info AS NVARCHAR(MAX)) AS license_info, is_supervised_aml_cft, director_customer_ids, major_shareholders, 
				_source_file_url, _ingestion_timestamp, _source_hash
			FROM bronze.crm_customers_2021
			UNION ALL
			SELECT  
				customer_id, customer_type, full_name, birth_date, citizenship,
				residential_address, commercial_address, email, phone_number, 
				id_type, id_number, expiry_date, visa_type, visa_expiry_date, 
				is_pep, pep_type, sanctioned_country, risk_score, tax_id_number, occupation, 
				employer_name, source_of_funds, marital_status, nationality, gender, 
				preferred_contact_method, next_of_kin, date_of_entry, annual_income, 
				education_level, ethnicity, is_director, director_of_companies, CAST(trading_name AS NVARCHAR(MAX)) trading_name,  
				principal_place_of_business, registered_office_address, CAST(trading_name AS NVARCHAR(50)) website, has_sanctioned_relationship, 
				CAST(fatca_giin AS NVARCHAR(MAX)) fatca_giin, swift_bic_code, industry, date_of_incorporation, city_of_incorporation, country_of_incorporation, 
				annual_turnover, company_age, number_of_employees, directors_count, shareholders_count, bee_level, vat_registered, 
				industry_risk_rating, is_publicly_listed, external_auditor, legal_representative_name, contact_person_name, 
				contact_person_title, contact_person_phone, contact_person_email,contact_person_is_customer, contact_person_customer_id, 
				num_branches_local, num_branches_foreign, num_subsidiaries_local, num_subsidiaries_foreign, requires_license, 
				CAST(license_info AS NVARCHAR(MAX)) AS license_info, is_supervised_aml_cft, director_customer_ids, major_shareholders, 
				_source_file_url, _ingestion_timestamp, _source_hash 
			FROM bronze.crm_customers_2022 
			UNION ALL
			SELECT  
				customer_id, customer_type, full_name, birth_date, citizenship,
				residential_address, commercial_address, email, phone_number, 
				id_type, id_number, expiry_date, visa_type, visa_expiry_date, 
				is_pep, pep_type, sanctioned_country, risk_score, tax_id_number, occupation, 
				employer_name, source_of_funds, marital_status, nationality, gender, 
				preferred_contact_method, next_of_kin, date_of_entry, annual_income, 
				education_level, ethnicity, is_director, director_of_companies, CAST(trading_name AS NVARCHAR(MAX)) trading_name,  
				principal_place_of_business, registered_office_address, CAST(trading_name AS NVARCHAR(50)) website, has_sanctioned_relationship, 
				CAST(fatca_giin AS NVARCHAR(MAX)) fatca_giin, swift_bic_code, industry, date_of_incorporation, city_of_incorporation, country_of_incorporation, 
				annual_turnover, company_age, number_of_employees, directors_count, shareholders_count, bee_level, vat_registered, 
				industry_risk_rating, is_publicly_listed, external_auditor, legal_representative_name, contact_person_name, 
				contact_person_title, contact_person_phone, contact_person_email,contact_person_is_customer, contact_person_customer_id, 
				num_branches_local, num_branches_foreign, num_subsidiaries_local, num_subsidiaries_foreign, requires_license, 
				CAST(license_info AS NVARCHAR(MAX)) AS license_info, is_supervised_aml_cft, director_customer_ids, major_shareholders, 
				_source_file_url, _ingestion_timestamp, _source_hash
			FROM bronze.crm_customers_2023
			UNION ALL
			SELECT  
				customer_id, customer_type, full_name, birth_date, citizenship,
				residential_address, commercial_address, email, phone_number, 
				id_type, id_number, expiry_date, visa_type, visa_expiry_date, 
				is_pep, pep_type, sanctioned_country, risk_score, tax_id_number, occupation, 
				employer_name, source_of_funds, marital_status, nationality, gender, 
				preferred_contact_method, next_of_kin, date_of_entry, annual_income, 
				education_level, ethnicity, is_director, director_of_companies, CAST(trading_name AS NVARCHAR(MAX)) trading_name,  
				principal_place_of_business, registered_office_address, CAST(trading_name AS NVARCHAR(50)) website, has_sanctioned_relationship, 
				CAST(fatca_giin AS NVARCHAR(MAX)) fatca_giin, swift_bic_code, industry, date_of_incorporation, city_of_incorporation, country_of_incorporation, 
				annual_turnover, company_age, number_of_employees, directors_count, shareholders_count, bee_level, vat_registered, 
				industry_risk_rating, is_publicly_listed, external_auditor, legal_representative_name, contact_person_name, 
				contact_person_title, contact_person_phone, contact_person_email,contact_person_is_customer, contact_person_customer_id, 
				num_branches_local, num_branches_foreign, num_subsidiaries_local, num_subsidiaries_foreign, requires_license, 
				CAST(license_info AS NVARCHAR(MAX)) AS license_info, is_supervised_aml_cft, director_customer_ids, major_shareholders, 
				_source_file_url, _ingestion_timestamp, _source_hash
			FROM bronze.crm_customers_2024
		) AS all_customers;

		PRINT 'Union all tables successful';
		-- Date Validation
		-- customer should be 18 when their information was collected, especially if they don't have a joint account
		-- customer should be less than 120 years

		PRINT 'Data cleaning process started...';
		WITH CTE_date_validation AS (
			SELECT *, 
				CASE 
					WHEN DATEDIFF(YEAR, birth_date, date_of_entry) < 0 THEN 
						DATEADD(YEAR, -100, birth_date)
					WHEN DATEDIFF(YEAR, birth_date, date_of_entry) > 120 THEN
						DATEADD(YEAR, 100, birth_date)
					ELSE birth_date
				END AS new_birth_date
			FROM #customers_temp
		), 

		CTE_age_calculations AS (
			SELECT *, 
				DATEDIFF(YEAR, birth_date, date_of_entry) AS age_at_opening, 
				DATEDIFF(YEAR, new_birth_date, date_of_entry) AS  new_age_at_opening
			FROM CTE_date_validation
		)
		-- Extract address data
		-- Extract street_name, city, province, country in residential
		,CTE_residential_address_data AS (
			SELECT *, 
				CASE 
					WHEN residential_address LIKE '%,%,%,%' THEN 
						PARSENAME(REPLACE(TRY_CAST(residential_address AS VARCHAR(2000)), ',', '.'), 1) 
					ELSE ''
				END AS residential_country,
				CASE 
					WHEN residential_address LIKE '%,%,%,%' THEN 
						PARSENAME(REPLACE(TRY_CAST(residential_address AS VARCHAR(2000)), ',', '.'), 2) 
					ELSE 
						PARSENAME(REPLACE(TRY_CAST(residential_address AS VARCHAR(2000)), ',', '.'), 1) 
				END AS residential_province, 
				CASE 
					WHEN residential_address LIKE '%,%,%,%' THEN 
						PARSENAME(REPLACE(TRY_CAST(residential_address AS VARCHAR(2000)), ',', '.'), 3) 
					ELSE 
						PARSENAME(REPLACE(TRY_CAST(residential_address AS VARCHAR(2000)), ',', '.'), 2) 
				END AS residential_city, 
				CASE 
					WHEN residential_address LIKE '%,%,%,%' THEN 
						PARSENAME(REPLACE(TRY_CAST(residential_address AS VARCHAR(2000)), ',', '.'), 4) 
					ELSE 
						PARSENAME(REPLACE(TRY_CAST(residential_address AS VARCHAR(2000)), ',', '.'), 3) 
				END AS residential_street_name					
			FROM CTE_age_calculations
		), 
		-- Extract commercial address data 
		-- Extract street_name, city, province, country in commercial address
		CTE_commercial_address_data AS (
			SELECT *, 
				CASE 
					WHEN commercial_address LIKE '%,%,%,%' THEN 
						PARSENAME(REPLACE(TRY_CAST(commercial_address AS VARCHAR(2000)), ',', '.'), 1) 
					ELSE ''
				END AS commercial_country,
				CASE 
					WHEN commercial_address LIKE '%,%,%,%' THEN 
						PARSENAME(REPLACE(TRY_CAST(commercial_address AS VARCHAR(2000)), ',', '.'), 2) 
					ELSE 
						PARSENAME(REPLACE(TRY_CAST(commercial_address AS VARCHAR(2000)), ',', '.'), 1) 
				END AS commercial_province, 
				CASE 
					WHEN commercial_address LIKE '%,%,%,%' THEN 
						PARSENAME(REPLACE(TRY_CAST(commercial_address AS VARCHAR(2000)), ',', '.'), 3) 
					ELSE 
						PARSENAME(REPLACE(TRY_CAST(commercial_address AS VARCHAR(2000)), ',', '.'), 2) 
				END AS commercial_city, 
				CASE 
					WHEN commercial_address LIKE '%,%,%,%' THEN 
						PARSENAME(REPLACE(TRY_CAST(commercial_address AS VARCHAR(2000)), ',', '.'), 4) 
					ELSE 
						PARSENAME(REPLACE(TRY_CAST(commercial_address AS VARCHAR(2000)), ',', '.'), 3) 
				END AS commercial_street_name		
			FROM CTE_residential_address_data
		), 
		-- Extract registered office address address data 
		-- Extract street_name, city, province, country in registered office address
		CTE_office_address_data AS (
			SELECT *, 
				CASE 
					WHEN registered_office_address LIKE '%,%,%,%' THEN 
						PARSENAME(REPLACE(TRY_CAST(registered_office_address AS VARCHAR(2000)), ',', '.'), 1) 
					ELSE ''
				END AS office_country,
				CASE 
					WHEN registered_office_address LIKE '%,%,%,%' THEN 
						PARSENAME(REPLACE(TRY_CAST(registered_office_address AS VARCHAR(2000)), ',', '.'), 2) 
					ELSE 
						PARSENAME(REPLACE(TRY_CAST(registered_office_address AS VARCHAR(2000)), ',', '.'), 1) 
				END AS office_province, 
				CASE 
					WHEN registered_office_address LIKE '%,%,%,%' THEN 
						PARSENAME(REPLACE(TRY_CAST(registered_office_address AS VARCHAR(2000)), ',', '.'), 3) 
					ELSE 
						PARSENAME(REPLACE(TRY_CAST(registered_office_address AS VARCHAR(2000)), ',', '.'), 2) 
				END AS office_city, 
				CASE 
					WHEN registered_office_address LIKE '%,%,%,%' THEN 
						PARSENAME(REPLACE(TRY_CAST(registered_office_address AS VARCHAR(2000)), ',', '.'), 4) 
					ELSE 
						PARSENAME(REPLACE(TRY_CAST(registered_office_address AS VARCHAR(2000)), ',', '.'), 3) 
				END AS office_street_name		
			FROM CTE_commercial_address_data
		), 
		-- Extract registered office address address data 
		-- Extract street_name, city, province, country in registered office address
		CTE_principal_address_data AS (
			SELECT *, 
				CASE 
					WHEN principal_place_of_business LIKE '%,%,%,%' THEN 
						PARSENAME(REPLACE(TRY_CAST(principal_place_of_business AS VARCHAR(2000)), ',', '.'), 1) 
					ELSE ''
				END AS principal_country,
				CASE 
					WHEN principal_place_of_business LIKE '%,%,%,%' THEN 
						PARSENAME(REPLACE(TRY_CAST(principal_place_of_business AS VARCHAR(2000)), ',', '.'), 2) 
					ELSE 
						PARSENAME(REPLACE(TRY_CAST(principal_place_of_business AS VARCHAR(2000)), ',', '.'), 1) 
				END AS principal_province, 
				CASE 
					WHEN principal_place_of_business LIKE '%,%,%,%' THEN 
						PARSENAME(REPLACE(TRY_CAST(principal_place_of_business AS VARCHAR(2000)), ',', '.'), 3) 
					ELSE 
						PARSENAME(REPLACE(TRY_CAST(principal_place_of_business AS VARCHAR(2000)), ',', '.'), 2) 
				END AS principal_city, 
				CASE 
					WHEN principal_place_of_business LIKE '%,%,%,%' THEN 
						PARSENAME(REPLACE(TRY_CAST(principal_place_of_business AS VARCHAR(2000)), ',', '.'), 4) 
					ELSE 
						PARSENAME(REPLACE(TRY_CAST(principal_place_of_business AS VARCHAR(2000)), ',', '.'), 3) 
				END AS principal_street_name		
			FROM CTE_office_address_data
		),
		-- Validate contact information
		CTE_validate_contact_information AS (
			SELECT *, 
				CASE WHEN email NOT LIKE '%@%.%' THEN 1 ELSE 0 END AS invalid_email_format, 
				CASE 
					WHEN phone_number NOT LIKE '+%' AND phone_number NOT LIKE '0%' THEN
						CASE 
							WHEN citizenship = 'ZA' THEN 
								CONCAT('+27', phone_number) 
							ELSE phone_number
						END
					WHEN phone_number NOT LIKE '+%' AND phone_number LIKE '0%' THEN
						CASE
							WHEN citizenship = 'ZA' THEN 
								CONCAT('+27', SUBSTRING(phone_number, 2, LEN(phone_number)))
							ELSE phone_number
						END
					ELSE phone_number
				END AS new_phone_number
			FROM CTE_principal_address_data			
		), 
		-- Validate documents expiry dates 
		CTE_validate_documents_expiry AS (
			SELECT *, 
				CASE WHEN expiry_date <= date_of_entry THEN 1 ELSE 0 END AS registered_expired_document, 
				CASE WHEN visa_expiry_date <= date_of_entry THEN 1 ELSE 0 END AS registered_expired_visa
			FROM CTE_validate_contact_information
		),
		-- Validate employment status 
		CTE_validate_employment_status AS (
			SELECT *, 
				CASE WHEN UPPER(occupation) NOT LIKE 'UNEMPLOYED%' AND employer_name IS NOT NULL THEN 1 ELSE 0 END AS check_employment
			FROM CTE_validate_documents_expiry
		), 
		-- Extract JSON Values from license information
		CTE_extract_license_values AS (
			SELECT *, 
				JSON_VALUE(license_info, '$.license_number') AS license_number,
				JSON_VALUE(license_info, '$.issue_date') AS license_issue_date,
				JSON_VALUE(license_info, '$.expiry_date') AS license_expiry_date,
				JSON_VALUE(license_info, '$.regulatory_body') AS license_regulatory_body
			FROM CTE_validate_employment_status
		),
		-- Extract JSON Values from shareholders information
		CTE_extract_shareholders_values AS (
			SELECT *,
				JSON_VALUE(s.value, '$.shareholder_id') AS shareholder_id,
				JSON_VALUE(s.value, '$.shareholder_name') AS shareholder_name,
				JSON_VALUE(s.value, '$.ownership_percentage') AS ownership_percentage
			FROM CTE_extract_license_values t
			OUTER APPLY OPENJSON(REPLACE(t.major_shareholders, '''', '"')) AS s
		), 
		-- Extract JSON Values from directors ids information
		CTE_outer_apply_director_customer_ids AS (
			SELECT t.*,
				s.value AS director_customer_id
			FROM CTE_extract_shareholders_values t
			OUTER APPLY OPENJSON(
				REPLACE(REPLACE(t.director_customer_ids, '''', '"'), 'None', 'null')
			) AS s
		)
		, 
		-- Extract JSON Values from company directed information
		CTE_outer_apply_companies_directed AS (
			SELECT t.*,
				s.value AS directed_company
			FROM CTE_extract_shareholders_values t
			OUTER APPLY OPENJSON(
				REPLACE(REPLACE(t.director_of_companies, '''', '"'), 'None', 'null')
			) AS s
		) 		
		SELECT 
			UPPER(TRIM(customer_id))                      AS customer_id,
			UPPER(TRIM(customer_type))                    AS customer_type,
			UPPER(TRIM(full_name))                        AS full_name,
			birth_date                                    AS date_of_birth,
			new_birth_date                                AS new_date_of_birth, 
			age_at_opening,
			new_age_at_opening,
			UPPER(TRIM(citizenship))                      AS citizenship,
			UPPER(TRIM(CAST(residential_address AS NVARCHAR(MAX))))              AS residential_address,
			UPPER(TRIM(residential_street_name)) AS residential_street_name,
			UPPER(TRIM(residential_city)) AS residential_city,
			UPPER(TRIM(residential_province)) AS residential_province,
			CASE
				WHEN (residential_country IS NULL OR LTRIM(RTRIM(residential_country)) = '')
					 AND UPPER(TRIM(residential_province)) IN (
						 'MPUMALANGA',
						 'GAUTENG',
						 'EASTERN CAPE',
						 'NORTH WEST',
						 'KWAZULU-NATAL',
						 'LIMPOPO',
						 'FREE STATE',
						 'NORTHERN CAPE',
						 'WESTERN CAPE'
					 ) THEN 'SOUTH AFRICA'
				ELSE UPPER(TRIM(residential_country))
			END AS residential_country,
			UPPER(TRIM(CAST(commercial_address AS NVARCHAR(MAX))))               AS commercial_address,
			UPPER(TRIM(commercial_street_name)) AS commercial_street_name,
			UPPER(TRIM(commercial_city)) AS commercial_city,
			UPPER(TRIM(commercial_province)) AS commercial_province,
			UPPER(TRIM(commercial_country)) AS commercial_country,
			UPPER(TRIM(email))                            AS email,
			invalid_email_format                          AS is_invalid_email_format,
			UPPER(TRIM(phone_number))                     AS phone_number,
			new_phone_number,
			UPPER(TRIM(id_type))                          AS document_type,
			UPPER(TRIM(id_number))                        AS document_number,
			registered_expired_document,
			expiry_date                                   AS document_expiry_date,
			UPPER(TRIM(visa_type))                        AS visa_type,
			visa_expiry_date,
			registered_expired_visa,
			is_pep,
			UPPER(TRIM(pep_type))                         AS pep_type,
			sanctioned_country                            AS is_sanctioned_country,
			risk_score,
			UPPER(TRIM(tax_id_number))                    AS tax_id_number,
			UPPER(TRIM(occupation))                       AS occupation,
			UPPER(TRIM(employer_name))                    AS employer_name,
			UPPER(TRIM(source_of_funds))                  AS source_of_funds,
			UPPER(TRIM(marital_status))                   AS marital_status,
			UPPER(TRIM(nationality))                      AS nationality,
			UPPER(TRIM(gender))                           AS gender,
			UPPER(TRIM(preferred_contact_method))         AS preferred_contact_method,
			UPPER(TRIM(next_of_kin))                      AS next_of_kin,
			date_of_entry                                 AS customer_registration_date,
			annual_income,
			UPPER(TRIM(education_level))                  AS education_level,
			UPPER(TRIM(ethnicity))                        AS ethnicity,
			is_director,
			UPPER(TRIM(director_of_companies))            AS director_of_companies,
			directed_company,
			UPPER(TRIM(trading_name))                     AS trading_name,
			UPPER(TRIM(CAST(principal_place_of_business AS NVARCHAR(MAX))))      AS principal_place_of_business,
			UPPER(TRIM(principal_street_name)) AS principal_street_name,
			UPPER(TRIM(principal_city)) AS principal_city,
			UPPER(TRIM(principal_province)) AS principal_province,
			UPPER(TRIM(principal_country)) AS principal_country,
			UPPER(TRIM(CAST(registered_office_address AS NVARCHAR(MAX))))        AS registered_office_address,
			UPPER(TRIM(office_street_name)) AS office_street_name,
			UPPER(TRIM(office_city)) AS office_city,
			UPPER(TRIM(office_province)) AS office_province,
			UPPER(TRIM(office_country)) AS office_country,
			UPPER(TRIM(website))                          AS website,
			has_sanctioned_relationship,
			UPPER(TRIM(fatca_giin))                       AS fatca_giin,
			UPPER(TRIM(swift_bic_code))                   AS swift_bic_code,
			UPPER(TRIM(industry))                         AS industry,
			date_of_incorporation,
			UPPER(TRIM(city_of_incorporation))            AS city_of_incorporation,
			UPPER(TRIM(country_of_incorporation))         AS country_of_incorporation,
			annual_turnover,
			company_age,
			number_of_employees,
			directors_count,
			shareholders_count,
			bee_level,
			vat_registered,
			UPPER(TRIM(industry_risk_rating))             AS industry_risk_rating,
			is_publicly_listed,
			UPPER(TRIM(external_auditor))                 AS external_auditor,
			UPPER(TRIM(legal_representative_name))        AS legal_representative_name,
			UPPER(TRIM(contact_person_name))              AS contact_person_name,
			UPPER(TRIM(contact_person_title))             AS contact_person_title,
			UPPER(TRIM(contact_person_phone))             AS contact_person_phone,
			UPPER(TRIM(contact_person_email))             AS contact_person_email,
			contact_person_is_customer,
			UPPER(TRIM(contact_person_customer_id))       AS contact_person_customer_id,
			num_branches_local,
			num_branches_foreign,
			num_subsidiaries_local,
			num_subsidiaries_foreign,
			requires_license,
			license_info,
			is_supervised_aml_cft,
			director_customer_ids,
			major_shareholders,
			_source_file_url,
			_ingestion_timestamp,
			_source_hash
		INTO #stg_customers
		FROM CTE_outer_apply_companies_directed;

		SELECT * 
		FROM #stg_customers;

		-- Get the number of rows inserted
		SET @RowsInserted = @@ROWCOUNT;

        /********************************************
			Log successful completion
        *********************************************/

		UPDATE error_log.etl_log
		SET end_time = SYSDATETIME(),
			status = 'SUCCESS',
			rows_processed = @RowsInserted
		WHERE batch_id = @BatchID;
		
	END TRY
	BEGIN CATCH 
        /********************************************
			Handle and log errors
        *********************************************/

		SET @ErrorMessage = ERROR_MESSAGE();
		UPDATE error_log.etl_log
		SET end_time = SYSDATETIME(),
			status = 'FAILED', 
			error_message = @ErrorMessage
		WHERE batch_id = @BatchID;

		RAISERROR('ETL failed in pcd_clean_customers_to_staging: %s', 16, 1, @ErrorMessage);

	END CATCH
END
GO
