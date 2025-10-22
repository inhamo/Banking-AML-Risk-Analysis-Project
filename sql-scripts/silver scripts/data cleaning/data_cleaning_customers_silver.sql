/********************************************************************************************
  Procedure:   silver.pcd_cleaning_customers_bronze_to_staging
  Purpose:     Cleans and standardizes customer data from bronze layer into silver layer.
  Author:      Innocent Nhamo
  Created:     2025-10-18
  Version:     1.1
  Change Log:
      - v1.0 Initial creation
********************************************************************************************/

USE banking_db;
GO

CREATE OR ALTER PROCEDURE silver.pcd_cleaning_customers_silver_staging
AS 
BEGIN 
	SET NOCOUNT ON;

	PRINT '=';
	PRINT 'Starting customer data cleaning process...';
	PRINT '=';
	
	--  DROP EXISTING CLEANED TABLE IF IT EXISTS 
	--  Ensures fresh start and prevents schema conflicts 
	IF OBJECT_ID('silver.stg_customers_cleaned', 'U') IS NOT NULL
		DROP TABLE silver.stg_customers_cleaned;

	--  DATE VALIDATION : Validates birth dates: ensures customer is between 18 and 100 years old at time of entry 
	--  Flags impossible dates (future births, entries before birth, or unrealistic ages) 
	WITH CTE_date_validation AS (
		SELECT *, 
			CASE 
				WHEN 
					birth_date >= DATEADD(YEAR, -18, date_of_entry) 
					OR birth_date <= DATEADD(YEAR, -100, date_of_entry)
				THEN 1
				ELSE 0
			END AS invalid_birth_date_flag 
		FROM silver.stg_customers
	), 
	
	--  PASSPORT AND VISA EXPIRY VALIDATION 
	--  Validates identification documents and visa information 
	--  Flags: missing passport expiry, expired documents, missing visa info for passport holders 
	CTE_passport_and_visa_expiry_validation AS (
		SELECT *, 
			CASE 
				WHEN id_type = 'Passport' AND expiry_date IS NULL THEN 1
				ELSE 0
			END AS missing_passport_expiry_flag, 
			CASE 
				WHEN expiry_date < date_of_entry THEN 1
				ELSE 0 
			END AS expired_passport_flag, 
			CASE
				WHEN id_type = 'Passport' AND visa_type IS NULL THEN 1 
				ELSE 0 
			END AS missing_visa_for_passport_flag, 
			CASE 
				WHEN visa_type IS NOT NULL AND visa_expiry_date IS NULL THEN 1 
				ELSE 0 
			END AS missing_visa_expiry_flag, 
			CASE 
				WHEN visa_type IS NOT NULL AND visa_expiry_date < date_of_entry THEN 1 
				ELSE 0 
			END AS expired_visa_flag
		FROM CTE_date_validation
	), 
	
	-- EMPLOYMENT STATUS VALIDATION 
	--  Validates employment data consistency 
	--  Flags: Unemployed individuals who should not have employer_name populated 
	CTE_invalid_employment_status AS(
		SELECT *, 
		CASE 
			WHEN occupation = 'Unemployed Unskilled' AND employer_name IS NOT NULL THEN 1 
			ELSE 0
		END AS invalid_employment_status_flag
		FROM CTE_passport_and_visa_expiry_validation
	),
	
	-- 	ADDRESS DATA TYPE CONVERSION AND CLEANING 
	--  Converts address fields to VARCHAR for manipulation 
	--  Removes leading/trailing spaces, dots, and trailing commas from addresses 
	--  Prepares addresses for parsing into components (street, city, province, country) 
	CTE_change_address_data_type AS (
		SELECT 
			*, 
			--  RESIDENTIAL ADDRESS CLEANING 
			--  Remove trailing commas and dots from residential address 
			CASE 
				WHEN RIGHT(TRIM(CAST(residential_address AS VARCHAR(255))), 1) = ',' 
				THEN REPLACE(LEFT(TRIM(CAST(residential_address AS VARCHAR(255))), LEN(TRIM(CAST(residential_address AS VARCHAR(255)))) - 1), '.', '')
				ELSE REPLACE(TRIM(CAST(residential_address AS VARCHAR(255))), '.', '')
			END AS residential_address_cleaned,
        
			--  COMMERCIAL ADDRESS CLEANING 
			--  Remove trailing commas and dots from commercial address 
			CASE 
				WHEN RIGHT(TRIM(CAST(commercial_address AS VARCHAR(255))), 1) = ',' 
				THEN REPLACE(LEFT(TRIM(CAST(commercial_address AS VARCHAR(255))), LEN(TRIM(CAST(commercial_address AS VARCHAR(255)))) - 1), '.', ' ')
				ELSE REPLACE(TRIM(CAST(commercial_address AS VARCHAR(255))), '.', ' ')
			END AS commercial_address_cleaned
		FROM CTE_invalid_employment_status
	), 
	
	--  PARSE COMMERCIAL ADDRESS INTO COMPONENTS 
	--  Extracts country, province, city, and street from comma-separated address format 
	--  Supports both 4-part (street,city,province,country) and 3-part (city,province,country) formats 
	--  Uses PARSENAME function with comma-to-dot replacement trick 
	CTE_extract_address_information_commercial AS (
		SELECT *, 
		--  Extract Country (last component) 
		CASE 
			WHEN commercial_address_cleaned LIKE '%,%,%,%' THEN 
				PARSENAME(REPLACE(commercial_address_cleaned, ',', '.'), 1)
			ELSE ''
		END comm_country, 
		--  Extract Province (3rd component for 4-part, last for 3-part) 
		CASE 
			WHEN commercial_address_cleaned LIKE '%,%,%,%' THEN 
				PARSENAME(REPLACE(commercial_address_cleaned, ',', '.'), 2)
			WHEN commercial_address_cleaned LIKE '%,%,%' THEN 
				PARSENAME(REPLACE(commercial_address_cleaned, ',', '.'), 1)
			ELSE ''
		END comm_province,
		--  Extract City (2nd component for 4-part, middle for 3-part) 
		CASE 
			WHEN commercial_address_cleaned LIKE '%,%,%,%' THEN 
				PARSENAME(REPLACE(commercial_address_cleaned, ',', '.'), 3)
			WHEN commercial_address_cleaned LIKE '%,%,%' THEN 
				PARSENAME(REPLACE(commercial_address_cleaned, ',', '.'), 2)
			ELSE ''
		END comm_city, 
		--  Extract Street/Building (first component for 4-part) 
		CASE 
			WHEN commercial_address_cleaned LIKE '%,%,%,%' THEN 
				PARSENAME(REPLACE(commercial_address_cleaned, ',', '.'), 4)
			WHEN commercial_address_cleaned LIKE '%,%,%' THEN 
				PARSENAME(REPLACE(commercial_address_cleaned, ',', '.'), 3)
			ELSE ''
		END comm_street_name
		FROM CTE_change_address_data_type
	), 
	
	--  PARSE RESIDENTIAL ADDRESS INTO COMPONENTS 
	--  Extracts country, province, city, and street from comma-separated address format 
	--  Same logic as commercial addresses for consistency 
	CTE_extract_address_information_residential AS (
		SELECT *, 
		--  Extract Country (last component) 
		CASE 
			WHEN residential_address_cleaned LIKE '%,%,%,%' THEN 
				PARSENAME(REPLACE(residential_address_cleaned, ',', '.'), 1)
			ELSE ''
		END res_country, 
		--  Extract Province (3rd component for 4-part, last for 3-part) 
		CASE 
			WHEN residential_address_cleaned LIKE '%,%,%,%' THEN 
				PARSENAME(REPLACE(residential_address_cleaned, ',', '.'), 2)
			WHEN residential_address_cleaned LIKE '%,%,%' THEN 
				PARSENAME(REPLACE(residential_address_cleaned, ',', '.'), 1)
			ELSE ''
		END res_province,
		--  Extract City (2nd component for 4-part, middle for 3-part) 
		CASE 
			WHEN residential_address_cleaned LIKE '%,%,%,%' THEN 
				PARSENAME(REPLACE(residential_address_cleaned, ',', '.'), 3)
			WHEN residential_address_cleaned LIKE '%,%,%' THEN 
				PARSENAME(REPLACE(residential_address_cleaned, ',', '.'), 2)
			ELSE ''
		END res_city, 
		--  Extract Street/Building (first component for 4-part) 
		CASE 
			WHEN residential_address_cleaned LIKE '%,%,%,%' THEN 
				PARSENAME(REPLACE(residential_address_cleaned, ',', '.'), 4)
			WHEN residential_address_cleaned LIKE '%,%,%' THEN 
				PARSENAME(REPLACE(residential_address_cleaned, ',', '.'), 3)
			ELSE ''
		END res_street_name
		FROM CTE_extract_address_information_commercial
	),
	
	--  GENDER VALIDATION 
	--  NEW: Validates gender field and standardizes values 
	--  Ensures gender is one of valid categories: M, F, OTHER, or PREFER NOT TO SAY 
	--  Flags invalid gender values for data quality review 
	CTE_gender_validation AS (
		SELECT *, 
			CASE 
				WHEN UPPER(TRIM(ISNULL(gender, ''))) NOT IN ('M', 'F', 'OTHER', 'PREFER NOT TO SAY') 
					AND gender IS NOT NULL 
				THEN 1
				ELSE 0
			END AS invalid_gender_flag
		FROM CTE_extract_address_information_residential
	),
	
	--  EMAIL VALIDATION 
	--  Attempts to fix common email errors (missing @ symbol) 
	--  Validates minimum length and proper domain format (@ and extension) 
	--  Flags emails that don't match preferred contact method 
	CTE_email_enhanced_validation AS (
		SELECT *, 
			--  ATTEMPT TO FIX MALFORMED EMAILS 
			--  If email has 'example' but no @, insert @ before 'example' 
			CASE 
                WHEN email LIKE '%example.%' AND email NOT LIKE '%@%' THEN 
                    TRIM(STUFF(email, CHARINDEX('example', email), LEN('example'), '@example'))
                ELSE TRIM(ISNULL(email, ''))
            END AS email_cleaned,
			
			--  FLAG: EMAIL FORMAT VALIDATION 
			--  Must have @ symbol and at least one dot after @ (domain extension) 
			--  Minimum length of 5 characters (a@b.c) 
			CASE 
				WHEN email IS NULL THEN 0
				WHEN email NOT LIKE '%@%.%' THEN 1
				WHEN LEN(TRIM(email)) < 5 THEN 1
				ELSE 0
			END AS invalid_email_format_flag,
			
			--  FLAG: CONTACT METHOD CONSISTENCY 
			--  If preferred method is Email, email must be valid 
			CASE 
				WHEN preferred_contact_method = 'Email' AND (email IS NULL OR email NOT LIKE '%@%.%') THEN 1
				ELSE 0
			END AS missing_preferred_contact_email_flag
		FROM CTE_gender_validation
	),
	
	--  PHONE NUMBER VALIDATION 
	--  Validates phone numbers have required length (12 characters including country code) 
	--  Flags missing phones when they're listed as preferred contact method 
	CTE_phone_validation AS (
		SELECT *, 
			CASE
				WHEN email_cleaned NOT LIKE '%@%' AND email_cleaned IS NOT NULL AND email_cleaned != '' THEN 1 
				ELSE 0
			END AS invalid_email_flag_final,
			
			CASE 
				WHEN LEN(TRIM(ISNULL(phone_number, ''))) != 12 AND phone_number IS NOT NULL THEN 1 
				ELSE 0
			END AS invalid_phone_number_flag,
			
			--  FLAG: CONTACT METHOD CONSISTENCY 
			--  If preferred method is Phone, phone must exist and be valid 
			CASE 
				WHEN preferred_contact_method = 'Phone' AND (phone_number IS NULL OR LEN(TRIM(ISNULL(phone_number, ''))) = 0) THEN 1
				ELSE 0
			END AS missing_preferred_contact_phone_flag
		FROM CTE_email_enhanced_validation
	),
	
	-- 	COMPREHENSIVE VISA VALIDATION 
	--  NEW: Enhanced visa validation beyond passport holders 
	--  Flags: Visa data for non-travel document ID types, missing visa expiry dates 
	--  Ensures visa data only exists when appropriate for ID type 
	CTE_visa_comprehensive_validation AS (
		SELECT *, 
			--  FLAG: VISA WITHOUT APPROPRIATE ID TYPE 
			--  Only Passport and Travel Documents should have visa info 
			CASE 
				WHEN visa_type IS NOT NULL AND id_type NOT IN ('Passport', 'Travel Document') THEN 1
				ELSE 0
			END AS invalid_visa_for_id_type_flag,
			
			--  FLAG: MISSING VISA EXPIRY WHEN VISA EXISTS 
			--  If visa_type is populated, visa_expiry_date must also be populated 
			CASE 
				WHEN visa_type IS NOT NULL AND visa_expiry_date IS NULL THEN 1
				ELSE 0
			END AS missing_visa_expiry_when_visa_exists_flag
		FROM CTE_phone_validation
	),
	
	-- 	NEXT OF KIN AND MARITAL STATUS VALIDATION 
	--  NEW: Validates next of kin information against marital status 
	--  Flags missing emergency contact for dependent statuses 
	--  Notes: Single individuals can still have emergency contacts 
	CTE_kin_marital_validation AS (
		SELECT *, 
			--  FLAG: MISSING NEXT OF KIN FOR DEPENDENT STATUSES 
			--  Married, Divorced, Widowed individuals should have emergency contact 
			CASE 
				WHEN marital_status IN ('Married', 'Partnered', 'Divorced', 'Widowed') 
					AND (next_of_kin IS NULL OR TRIM(next_of_kin) = '') 
				THEN 1
				ELSE 0
			END AS missing_next_of_kin_flag
		FROM CTE_visa_comprehensive_validation
	),
	
	--  CONTACT METHOD CONSISTENCY CHECK 
	--  NEW: Validates preferred contact method has supporting data 
	--  Ensures Email, Phone, or SMS can actually be used 
	CTE_contact_method_validation AS (
		SELECT *, 
			--  FLAG: INCONSISTENT CONTACT METHOD 
			--  Phone method requires valid phone, Email requires valid email, SMS requires valid phone 
			CASE 
				WHEN preferred_contact_method = 'Phone' AND invalid_phone_number_flag = 1 THEN 1
				WHEN preferred_contact_method = 'Email' AND invalid_email_format_flag = 1 THEN 1
				WHEN preferred_contact_method = 'SMS' AND invalid_phone_number_flag = 1 THEN 1
				ELSE 0
			END AS inconsistent_contact_method_flag
		FROM CTE_kin_marital_validation
	),
	
	--  CREATE ALL VALIDATION FLAGS FOR OUTPUT 
	CTE_final_validation AS (
		SELECT *,
			--  Add risk score validation here to ensure it's in scope 
			CASE 
				WHEN risk_score NOT BETWEEN 0 AND 1 THEN 1 
				ELSE 0
			END AS invalid_risk_score_flag
		FROM CTE_contact_method_validation
	)
	
	--  CREATE CLEANED TABLE WITH ALL TRANSFORMATIONS 
	--  Applies standardization, type conversions, and includes all validation flags 
	SELECT 
		--  IDENTIFIERS AND BASIC INFO 
		--  Standardize to uppercase and trim whitespace for consistency 
		UPPER(TRIM(customer_id)) AS customer_id, 
		UPPER(TRIM(customer_type)) AS customer_type,
		TRIM(full_name) AS customer_fullname, 
		
		--  BIRTH DATE HANDLING 
		--  Corrects common year transcription errors (e.g., 1951 entered as 2051) 
		--  Validates age is between 18 and 100 at time of account opening 
		CAST(birth_date AS DATE) AS birth_date,
		CASE 
			WHEN birth_date > date_of_entry THEN CAST(DATEADD(YEAR, -100, birth_date) AS DATE)
			WHEN birth_date <= DATEADD(YEAR, -100, date_of_entry) THEN CAST(DATEADD(YEAR, 100, birth_date) AS DATE)
			ELSE birth_date
		END AS corrected_birth_date,
		CAST(birth_date AS DATE) AS original_birth_date,
		invalid_birth_date_flag,
		
		--  CITIZENSHIP AND LOCATION 
		--  Corrects known data entry errors (ZI -> ZW for Zimbabwe) 
		CASE 
			WHEN citizenship = 'ZI' THEN 'ZW'
			ELSE UPPER(TRIM(citizenship))
		END AS citizenship,
		
		--  RESIDENTIAL ADDRESS PARSING 
		--  Breaks down full address into individual components for filtering/analysis 
		TRIM(residential_address_cleaned) AS original_residential_address, 
		TRIM(res_street_name) AS residential_street_name, 
		TRIM(res_city) AS residential_city, 
		TRIM(res_province) AS residential_province, 
		TRIM(res_country) AS residential_country,
		
		--  COMMERCIAL ADDRESS PARSING 
		--  Breaks down business address into components for business customers 
		TRIM(commercial_address_cleaned) AS original_commercial_address,
		TRIM(comm_street_name) AS commercial_street_name, 
		TRIM(comm_city) AS commercial_city, 
		TRIM(comm_province) AS commercial_province, 
		TRIM(comm_country) AS commercial_country,
		
		--  CONTACT INFORMATION 
		--  Email: Attempts to fix common errors and validates format 
		email_cleaned AS email, 
		invalid_email_format_flag, 
		missing_preferred_contact_email_flag,
		invalid_email_flag_final,
		
		--  Phone: Validates length requirement for international format 
		TRIM(ISNULL(phone_number, '')) AS phone_number, 
		invalid_phone_number_flag,
		missing_preferred_contact_phone_flag,
		
		--  IDENTIFICATION DOCUMENTS 
		--  Standardizes ID type and stores expiry information 
		UPPER(TRIM(id_type)) AS id_type, 
		TRIM(id_number) AS id_number,
		CAST(expiry_date AS DATE) AS id_expiry_date,
		missing_passport_expiry_flag,
		expired_passport_flag,
		missing_visa_for_passport_flag,
		missing_visa_expiry_flag, 
		expired_visa_flag,
		
		--  VISA INFORMATION 
		--  Stores visa details for international customers 
		UPPER(TRIM(ISNULL(visa_type, ''))) AS visa_type,
		CAST(visa_expiry_date AS DATE) AS visa_expiry_date,
		invalid_visa_for_id_type_flag,
		missing_visa_expiry_when_visa_exists_flag,
		
		--  RISK AND COMPLIANCE 
		--  PEP: Politically Exposed Person flag from source 
		--  Sanctioned Country: Flag indicating customer from restricted jurisdictions 
		--  Risk Score: Normalized score between 0 and 1 
		CAST(is_pep AS BIT) AS is_pep, 
		CAST(sanctioned_country AS BIT) AS sanctioned_country, 
		risk_score, 
		invalid_risk_score_flag, 
		
		--  EMPLOYMENT AND FINANCIAL 
		--  Captures occupation, employer, and source of funds for AML/KYC purposes 
		TRIM(tax_id_number) AS tax_id_number,
		TRIM(occupation) AS occupation, 
		TRIM(employer_name) AS employer_name, 
		invalid_employment_status_flag,
		TRIM(source_of_funds) AS source_of_funds, 
		
		--  PERSONAL DETAILS 
		--  Demographic information for customer profiling 
		UPPER(TRIM(marital_status)) AS marital_status, 
		TRIM(nationality) AS nationality,
		
		--  GENDER (NEW) 
		--  Standardizes gender field to uppercase with fallback for NULL values 
		UPPER(TRIM(ISNULL(gender, 'Unknown'))) AS gender,
		invalid_gender_flag,
		
		--  CONTACT PREFERENCES 
		--  Preferred method and emergency contact information 
		CASE 
			WHEN preferred_contact_method IS NULL THEN 'Unknown'
			ELSE TRIM(preferred_contact_method)
		END AS preferred_contact_method,
		inconsistent_contact_method_flag,
		TRIM(ISNULL(next_of_kin, '')) AS next_of_kin,
		missing_next_of_kin_flag,
		
		--  DATES AND DEMOGRAPHICS 
		--  Account opening date, income, education, and ethnicity 
		CAST(date_of_entry AS DATE) AS date_of_entry, 
		CAST(annual_income AS DECIMAL(15, 2)) AS annual_income,
		TRIM(education_level) AS education_level,
		UPPER(TRIM(ethnicity)) AS ethnicity,
		CAST(is_affidavit AS BIT) AS is_affidavit, 
		
		--  BUSINESS CUSTOMER INFORMATION 
		--  Company details for B2B/corporate accounts 
		CAST(CAST(company_age AS DECIMAL(10,2)) AS INT) AS company_age , 
		CAST(CAST(number_of_employees AS DECIMAL(10,2)) AS INT) AS number_of_employees,
		ABS(CAST(annual_turnover AS DECIMAL(15, 2))) AS annual_turnover, 
		CAST(CAST(directors_count AS DECIMAL(10,2)) AS INT) AS directors_count, 
		CAST(CAST(shareholders_count AS DECIMAL(10,2)) AS INT) AS shareholders_count,
		CAST(CAST(bee_level AS DECIMAL(10,2)) AS INT) AS bee_level,
		CAST(vat_registered AS BIT) AS vat_registered,
		UPPER(TRIM(industry_risk_rating)) AS industry_risk_rating,
		
		--  METADATA 
		--  Data lineage and processing information 
		_source_file_url,
		_ingestion_timestamp,
		_source_hash,
		GETDATE() AS _cleaning_timestamp
		
	INTO silver.stg_customers_cleaned
	FROM CTE_final_validation;
	
	--  SET PRIMARY KEY CONSTRAINT 
	--  Ensures customer_id is unique and not null 
	ALTER TABLE silver.stg_customers_cleaned
	ALTER COLUMN customer_id NVARCHAR(50) NOT NULL;

	ALTER TABLE silver.stg_customers_cleaned 
	ADD CONSTRAINT pk_stg_customers_cleaned PRIMARY KEY (customer_id);

	--  CHECK FOR DUPLICATES IN CLEANED TABLE 
	--  Primary key prevents duplicates, but check for completeness 
	IF EXISTS (SELECT customer_id FROM silver.stg_customers_cleaned GROUP BY customer_id HAVING COUNT(*) > 1)
	BEGIN 
		PRINT 'WARNING: Duplicates found in cleaned table!';
	END 
	ELSE
	BEGIN 
		PRINT 'OK: No duplicates found in cleaned table';
	END 
END;
GO

