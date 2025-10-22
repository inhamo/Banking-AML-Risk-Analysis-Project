-- ======================================
-- CREATE NORMALIZATION TABLES
-- ======================================
USE banking_db;
GO 


-- Procedure to create the tables
CREATE OR ALTER PROCEDURE silver.pcd_create_customer_tables_silver
AS
BEGIN 
	SET NOCOUNT ON; 

	-- Drop tables if they exist 
	DROP TABLE IF EXISTS silver.dim_customers_business_profile;
	DROP TABLE IF EXISTS silver.dim_customer_contacts; 
	DROP TABLE IF EXISTS silver.dim_customer_employment_and_education;
	DROP TABLE IF EXISTS silver.dim_customer_risks;  
	DROP TABLE IF EXISTS silver.dim_customer_addresses; 
	DROP TABLE IF EXISTS silver.dim_customer_identifications; 
	DROP TABLE IF EXISTS silver.fact_customers;

	PRINT 'Creating customer dimension and fact tables for normalization.....';
	
	-- CUSTOMER FACT TABLE (Core customer information)
	CREATE TABLE silver.fact_customers ( 
		customer_id VARCHAR(20) NOT NULL PRIMARY KEY, 
		customer_type VARCHAR(20), 
		fullname VARCHAR(50) NOT NULL,  
		birth_date DATE, 
		marital_status VARCHAR(20), 
		nationality VARCHAR(50), 
		gender VARCHAR(20),
		date_of_entry DATE, 
		ethnicity VARCHAR(20), 
		preferred_contact_method VARCHAR(10),
		created_date DATETIME2 DEFAULT GETDATE(),
		updated_date DATETIME2 DEFAULT GETDATE()
	);

	-- CUSTOMER IDENTIFICATIONS
	CREATE TABLE silver.dim_customer_identifications (
		customer_id VARCHAR(20) NOT NULL, 
		citizenship CHAR(3), 
		id_type VARCHAR(20), 
		id_number VARCHAR(30), 
		id_expiry_date DATE, 
		visa_type VARCHAR(20), 
		visa_expiry_date DATE, 
		tax_id_number VARCHAR(30), 
		is_primary BIT DEFAULT 1,
		created_date DATETIME2 DEFAULT GETDATE(),  

		CONSTRAINT pk_dim_customer_identifications PRIMARY KEY (customer_id, id_type),
		CONSTRAINT fk_identifications_customer FOREIGN KEY (customer_id) 
			REFERENCES silver.fact_customers(customer_id)
	);

	-- CUSTOMER ADDRESSES
	CREATE TABLE silver.dim_customer_addresses (
		customer_id VARCHAR(20) NOT NULL, 
		address_type VARCHAR(20) CHECK (address_type IN ('RESIDENTIAL', 'COMMERCIAL', 'MAILING')),  
		street_name VARCHAR(100),
		city VARCHAR(50),  
		province VARCHAR(50), 
		country VARCHAR(50),  
		full_address VARCHAR(500), 
		is_primary BIT DEFAULT 1, 
		created_date DATETIME2 DEFAULT GETDATE(),

		CONSTRAINT pk_dim_customer_addresses PRIMARY KEY (customer_id, address_type),
		CONSTRAINT fk_addresses_customer FOREIGN KEY (customer_id) 
			REFERENCES silver.fact_customers(customer_id)
	);

	-- CUSTOMER RISKS 
	CREATE TABLE silver.dim_customer_risks (
		customer_id VARCHAR(20) NOT NULL PRIMARY KEY,  
		is_pep BIT, 
		sanctioned_country BIT, 
		is_affidavit BIT,
		risk_score DECIMAL(3, 2),  
		risk_category VARCHAR(20), 
		created_date DATETIME2 DEFAULT GETDATE(),
		updated_date DATETIME2 DEFAULT GETDATE(),

		CONSTRAINT fk_risks_customer FOREIGN KEY (customer_id) 
			REFERENCES silver.fact_customers(customer_id)
	); 

	-- CUSTOMER EMPLOYMENT AND EDUCATION 
	CREATE TABLE silver.dim_customer_employment_and_education (
		customer_id VARCHAR(20) NOT NULL PRIMARY KEY, 
		occupation VARCHAR(50), 
		employer_name VARCHAR(100), 
		source_of_funds VARCHAR(50), 
		annual_income DECIMAL(15, 2),
		education_level VARCHAR(50),
		employment_status VARCHAR(20),  
		created_date DATETIME2 DEFAULT GETDATE(),
		updated_date DATETIME2 DEFAULT GETDATE(),

		CONSTRAINT fk_employment_customer FOREIGN KEY (customer_id) 
			REFERENCES silver.fact_customers(customer_id)
	);

	-- CUSTOMER CONTACTS 
	CREATE TABLE silver.dim_customer_contacts (
		customer_id VARCHAR(20) NOT NULL,
		contact_category VARCHAR(20) NOT NULL CHECK (contact_category IN ('COMMUNICATION', 'EMERGENCY', 'BUSINESS')), 
		contact_type VARCHAR(20) NOT NULL CHECK (contact_type IN ('EMAIL', 'PHONE', 'MOBILE', 'NEXT_OF_KIN', 'ALTERNATE_EMAIL')), 
		contact_value VARCHAR(255) NOT NULL, 
		is_primary BIT DEFAULT 1, 
		created_date DATETIME2 DEFAULT GETDATE(),

		CONSTRAINT pk_dim_customer_contacts PRIMARY KEY (customer_id, contact_category, contact_type),
		CONSTRAINT fk_contacts_customer FOREIGN KEY (customer_id) 
			REFERENCES silver.fact_customers(customer_id)
	);

	-- CUSTOMER BUSINESS PROFILE 
	CREATE TABLE silver.dim_customer_business_profiles ( 
		customer_id VARCHAR(20) NOT NULL PRIMARY KEY, 
		company_age INT, 
		number_of_employees INT, 
		annual_turnover DECIMAL(15, 2), 
		directors_count INT, 
		shareholders_count INT, 
		bee_level INT, 
		vat_registered BIT,
		industry_risk_rating VARCHAR(20),  
		business_size_category VARCHAR(20), 
		created_date DATETIME2 DEFAULT GETDATE(),
		updated_date DATETIME2 DEFAULT GETDATE(),

		CONSTRAINT fk_business_customer FOREIGN KEY (customer_id) 
			REFERENCES silver.fact_customers(customer_id)
	);

	PRINT 'All customer dimension and fact tables created successfully!';
	PRINT 'Tables created:';
	PRINT '- silver.fact_customers (core customer information)';
	PRINT '- silver.dim_customer_identifications (IDs, passports, visas)';
	PRINT '- silver.dim_customer_addresses (residential/commercial addresses)';
	PRINT '- silver.dim_customer_risks (PEP, sanctions, risk scores)';
	PRINT '- silver.dim_customer_employment_and_education (occupation, income, education)';
	PRINT '- silver.dim_customer_contacts (emails, phones, emergency contacts)';
	PRINT '- silver.dim_customer_business_profiles (business customer details)';
END;
GO

-- Execute the procedure

GO

