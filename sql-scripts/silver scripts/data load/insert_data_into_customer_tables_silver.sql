-- ==============================================
--  PROCEDURE TO INSERT DATA INTO CUSTOMER TABLES 
-- ==============================================
USE banking_db; 
GO 

-- Procedure (fixed spelling)
CREATE OR ALTER PROCEDURE silver.pcd_insert_data_customers_tables_silver  -- Added PROCEDURE keyword
AS 
BEGIN
	SET NOCOUNT ON;
	
	PRINT 'Inserting data into customers tables.....';

		BEGIN TRANSACTION;
		
		-- Clear existing data first (optional - depends on your needs)
		PRINT 'Clearing existing data from customer tables...';
		DELETE FROM silver.dim_customer_business_profiles;
		DELETE FROM silver.dim_customer_contacts;
		DELETE FROM silver.dim_customer_employment_and_education;
		DELETE FROM silver.dim_customer_risks;
		DELETE FROM silver.dim_customer_addresses;
		DELETE FROM silver.dim_customer_identifications;
		DELETE FROM silver.fact_customers;

		-- INSERT INTO FACT CUSTOMERS (All customers in one query - more efficient)
		PRINT 'Inserting into fact_customers...';
		TRUNCATE TABLE silver.fact_customers;
		INSERT INTO silver.fact_customers (
			customer_id, 
			customer_type, 
			fullname, 
			birth_date, 
			marital_status, 
			nationality, 
			gender, 
			date_of_entry, 
			ethnicity, 
			preferred_contact_method,
			is_invalid_birth_date
		)
		SELECT
			customer_id, 
			CASE 
				WHEN UPPER(customer_type) = 'COMPANY' THEN 'COMPANY'
				ELSE 'INDIVIDUAL' 
			END AS customer_type, 
			customer_fullname AS fullname, 
			corrected_birth_date AS birth_date, 
			marital_status, 
			nationality, 
			gender, 
			date_of_entry, 
			ethnicity, 
			UPPER(preferred_contact_method), 
			is_invalid_birth_date
		FROM silver.stg_customers_cleaned;

		PRINT 'Successfully inserted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records into fact_customers';

		-- INSERT INTO IDENTIFICATIONS TABLE
		PRINT 'Inserting into dim_customer_identifications...';
		TRUNCATE TABLE silver.dim_customer_identifications;
		INSERT INTO silver.dim_customer_identifications (
			customer_id, 
			citizenship, 
			id_type, 
			id_number, 
			id_expiry_date, 
			visa_type, 
			visa_expiry_date, 
			tax_id_number, 
			is_primary
		)
		SELECT 
			customer_id,
			citizenship,
			id_type,
			id_number,
			id_expiry_date,
			visa_type,
			visa_expiry_date,
			tax_id_number,
			1 AS is_primary  -- Assuming one primary ID per customer for now
		FROM silver.stg_customers_cleaned
		WHERE id_type IS NOT NULL AND id_number IS NOT NULL;

		PRINT 'Successfully inserted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records into dim_customer_identifications';

		-- INSERT INTO ADDRESSES TABLE (Residential)
		PRINT 'Inserting residential addresses...';
		TRUNCATE TABLE silver.dim_customer_addresses;
		INSERT INTO silver.dim_customer_addresses (
			customer_id,
			address_type,
			street_name,
			city,
			province,
			country,
			full_address,
			is_primary
		)
		SELECT 
			customer_id,
			'RESIDENTIAL' AS address_type,
			residential_street_name,
			residential_city,
			residential_province,
			CASE 
				WHEN UPPER(residential_province) IN ('GAUTENG','WESTERN CAPE', 'KWAZULU-NATAL','EASTERN CAPE','LIMPOPO',
				'MPUMALANGA','NORTH WEST','FREE STATE','NORTHERN CAPE') THEN 'South Africa'
				ELSE ''
			END AS residential_country,
			original_residential_address,
			1 AS is_primary
		FROM silver.stg_customers_cleaned
		WHERE original_residential_address IS NOT NULL;

		PRINT 'Successfully inserted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' residential addresses';

		-- INSERT INTO ADDRESSES TABLE (Commercial - for companies)
		PRINT 'Inserting commercial addresses...';
		INSERT INTO silver.dim_customer_addresses (
			customer_id,
			address_type,
			street_name,
			city,
			province,
			country,
			full_address,
			is_primary
		)
		SELECT 
			customer_id,
			'COMMERCIAL' AS address_type,
			commercial_street_name,
			commercial_city,
			commercial_province,
			CASE 
				WHEN UPPER(commercial_province) IN ('GAUTENG','WESTERN CAPE', 'KWAZULU-NATAL','EASTERN CAPE','LIMPOPO',
				'MPUMALANGA','NORTH WEST','FREE STATE','NORTHERN CAPE') THEN 'South Africa'
				ELSE ''
			END AS commercial_country,
			original_commercial_address,
			CASE 
				WHEN customer_type = 'INDIVIDUAL' THEN 0 
				ELSE 1 
			END AS is_primary  -- Not primary address for individuals
		FROM silver.stg_customers_cleaned
		WHERE original_commercial_address IS NOT NULL;

		PRINT 'Successfully inserted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' commercial addresses';

		-- INSERT INTO RISKS TABLE
		PRINT 'Inserting into dim_customer_risks...';
		TRUNCATE TABLE silver.dim_customer_risks;
		INSERT INTO silver.dim_customer_risks (
			customer_id,
			is_pep,
			sanctioned_country,
			is_affidavit,
			risk_score,
			risk_category
		)
		SELECT 
			customer_id,
			is_pep,
			sanctioned_country,
			is_affidavit,
			risk_score,
			CASE 
				WHEN risk_score >= 0.5 THEN 'HIGH'
				WHEN risk_score >= 0.3 THEN 'MEDIUM'
				ELSE 'LOW'
			END AS risk_category
		FROM silver.stg_customers_cleaned;

		PRINT 'Successfully inserted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records into dim_customer_risks';

		-- INSERT INTO EMPLOYMENT AND EDUCATION TABLE
		PRINT 'Inserting into dim_customer_employment_and_education...';
		TRUNCATE TABLE silver.dim_customer_employment_and_education;
		INSERT INTO silver.dim_customer_employment_and_education (
			customer_id,
			occupation,
			employer_name,
			source_of_funds,
			annual_income,
			education_level,
			employment_status
		)
		SELECT 
			customer_id,
			occupation,
			employer_name,
			source_of_funds,
			annual_income,
			education_level,
			CASE 
				WHEN occupation = 'Unemployed Unskilled' THEN 'UNEMPLOYED'
				WHEN employer_name IS NOT NULL AND occupation != 'Unemployed Unskilled' THEN 'EMPLOYED'
				ELSE 'UNKNOWN'
			END AS employment_status
		FROM silver.stg_customers_cleaned;

		PRINT 'Successfully inserted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' records into employment table';

		-- INSERT INTO CONTACTS TABLE (Email)
		PRINT 'Inserting email contacts...';
		TRUNCATE TABLE silver.dim_customer_contacts;
		INSERT INTO silver.dim_customer_contacts (
			customer_id,
			contact_category,
			contact_type,
			contact_value,
			is_primary
		)
		SELECT 
			customer_id,
			'COMMUNICATION' AS contact_category,
			'EMAIL' AS contact_type,
			email,
			1 AS is_primary
		FROM silver.stg_customers_cleaned
		WHERE invalid_email_flag_final = 0;

		PRINT 'Successfully inserted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' email contacts';

		-- INSERT INTO CONTACTS TABLE (Phone)
		PRINT 'Inserting phone contacts...';
		INSERT INTO silver.dim_customer_contacts (
			customer_id,
			contact_category,
			contact_type,
			contact_value,
			is_primary
		)
		SELECT 
			customer_id,
			'COMMUNICATION' AS contact_category,
			'PHONE' AS contact_type,
			phone_number,
			1 AS is_primary
		FROM silver.stg_customers_cleaned
		WHERE invalid_phone_number_flag = 0;

		PRINT 'Successfully inserted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' phone contacts';

		-- INSERT INTO CONTACTS TABLE (Next of Kin)
		PRINT 'Inserting next of kin contacts...';
		INSERT INTO silver.dim_customer_contacts (
			customer_id,
			contact_category,
			contact_type,
			contact_value,
			is_primary
		)
		SELECT 
			customer_id,
			'EMERGENCY' AS contact_category,
			'NEXT_OF_KIN' AS contact_type,
			next_of_kin,
			1 AS is_primary
		FROM silver.stg_customers_cleaned
		WHERE next_of_kin IS NOT NULL AND next_of_kin != '';

		PRINT 'Successfully inserted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' next of kin contacts';

		-- INSERT INTO BUSINESS PROFILES (for companies)
		PRINT 'Inserting business profiles...';
		TRUNCATE TABLE silver.dim_customer_business_profiles;
		INSERT INTO silver.dim_customer_business_profiles (
			customer_id,
			company_age,
			number_of_employees,
			annual_turnover,
			directors_count,
			shareholders_count,
			bee_level,
			vat_registered,
			industry_risk_rating,
			business_size_category
		)
		SELECT 
			customer_id,
			company_age,
			number_of_employees,
			annual_turnover,
			directors_count,
			shareholders_count,
			bee_level,
			vat_registered,
			industry_risk_rating,
			CASE 
				WHEN number_of_employees < 50 THEN 'SMALL'
				WHEN number_of_employees < 200 THEN 'MEDIUM'
				ELSE 'LARGE'
			END AS business_size_category
		FROM silver.stg_customers_cleaned
		WHERE UPPER(customer_type) = 'COMPANY';

		PRINT 'Successfully inserted ' + CAST(@@ROWCOUNT AS VARCHAR) + ' business profiles';

		COMMIT TRANSACTION;
		PRINT 'All customer data inserted successfully';
END;
GO

