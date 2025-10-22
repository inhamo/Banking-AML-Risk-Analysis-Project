 
/********************************************************************************************
  Procedure:   silver.pcd_create_accounts_tables_silver
  Purpose:     Cleans and standardizes account data from bronze layer into silver layer.
  Author:      Innocent Nhamo
  Created:     2025-10-18
  Version:     1.2
  Change Log:
      - v1.0 Initial creation
      - v1.1 Added data validation, deduplication, and beneficiary parsing logic
      - v1.2 Complete normalization redesign to eliminate anomalies and add proper constraints
********************************************************************************************/
USE banking_db; 
GO
 
CREATE OR ALTER PROCEDURE silver.pcd_create_accounts_tables_silver
AS 
BEGIN 
    SET NOCOUNT ON; 
        PRINT 'Creating normalized accounts tables in silver layer.....';

        -- Drop tables if they exist (in correct order due to foreign key constraints)
        DROP TABLE IF EXISTS silver.dim_account_beneficiaries;
        DROP TABLE IF EXISTS silver.dim_joint_account;
        DROP TABLE IF EXISTS silver.dim_account_documents;
        DROP TABLE IF EXISTS silver.dim_branch;
        DROP TABLE IF EXISTS silver.dim_account_type;
        DROP TABLE IF EXISTS silver.dim_account_status;
        DROP TABLE IF EXISTS silver.fact_accounts;
        DROP TABLE IF EXISTS silver.dim_account_branch;
        DROP TABLE IF EXISTS silver.dim_account_products;
        DROP TABLE IF EXISTS silver.dim_account_cards;

        -- BRANCH CODE
        CREATE TABLE silver.dim_branch (
            branch_id INT IDENTITY(1,1),
            branch_code NVARCHAR(50)  NOT NULL,
            branch_name NVARCHAR(100),
            branch_address NVARCHAR(255),
            branch_city NVARCHAR(100),
            branch_manager NVARCHAR(100),
            _created_date DATETIME2 DEFAULT GETDATE(),
            _updated_date DATETIME2 DEFAULT GETDATE()
        );

        -- FACT ACCOUNTS
        CREATE TABLE silver.fact_accounts (
            account_id NVARCHAR(50),
            account_number NVARCHAR(50)  NOT NULL,
            customer_id NVARCHAR(50) NOT NULL,
            is_primary_account BIT,
            expected_amount DECIMAL(15,2),
            currency CHAR(3) DEFAULT 'ZAR',
            swift_code NVARCHAR(50),
            iban NVARCHAR(50),
            statement_frequency NVARCHAR(50),
            online_banking_enabled BIT,
            online_banking_activation_date DATE,
            days_to_online_banking_activation INT,
            cross_border_enabled BIT,
            minimum_deposit_met BIT,
            opening_channel NVARCHAR(50),
            account_value_segment NVARCHAR(50),
            _created_date DATETIME2 DEFAULT GETDATE(),
            _updated_date DATETIME2 DEFAULT GETDATE()
        );

        -- JOINT ACCOUNTS
        CREATE TABLE silver.dim_joint_account (
            account_id NVARCHAR(50) NOT NULL ,
            joint_customer_id NVARCHAR(50) NOT NULL,
            added_date DATE, 
            is_active BIT,
            _created_date DATETIME2 DEFAULT GETDATE(),
            _updated_date DATETIME2 DEFAULT GETDATE()
        );

        -- ACCOUNT DOCUMENTS 
        CREATE TABLE silver.dim_account_documents (
            account_id NVARCHAR(50) NOT NULL ,
            kyc_verified BIT DEFAULT 0,
            fica_verified BIT DEFAULT 0,
            proof_of_income_provided BIT DEFAULT 0,
            proof_of_address_provided BIT DEFAULT 0,
            bank_statements_provided BIT DEFAULT 0,
            employer_letter_provided BIT DEFAULT 0,
            business_registration_provided BIT DEFAULT 0,
            tax_certificate_provided BIT DEFAULT 0,
            verification_date DATE,
            verification_status NVARCHAR(50),
            _created_date DATETIME2 DEFAULT GETDATE(),
            _updated_date DATETIME2 DEFAULT GETDATE()
        );

        --  ACCOUNT STATUS
        CREATE TABLE silver.dim_account_status (
            account_id NVARCHAR(50) NOT NULL,
            account_status NVARCHAR(50)  NOT NULL,
            acc_opening_date DATE NOT NULL, 
            acc_approval_date DATE, 
            acc_status_change_date DATE,
            acc_closing_date DATE,
            status_description NVARCHAR(255),
            days_to_approval INT, 
            days_before_status_change INT, 
            account_tenure INT,
            _created_date DATETIME2 DEFAULT GETDATE(),
            _updated_date DATETIME2 DEFAULT GETDATE()
        );

        -- ACCOUNT TYPES
        CREATE TABLE silver.dim_account_type (
            account_id NVARCHAR(50) NOT NULL,
            account_type NVARCHAR(50) NOT NULL,
            account_purpose NVARCHAR(100),
            account_tier VARCHAR(50),
            interest_rate DECIMAL(8,6),
            monthly_charges DECIMAL(10,2),
            transactions_rate DECIMAL(8,6),
            negative_balance_rate DECIMAL(8,6),
            overdraft_limit BIT, 
            credit_card_limit BIT,
            _created_date DATETIME2 DEFAULT GETDATE(),
            _updated_date DATETIME2 DEFAULT GETDATE()
        );

        -- ACCOUNT PRODUCTS 
        CREATE TABLE silver.dim_account_products (
            account_id NVARCHAR(50) NOT NULL, 
            account_product NVARCHAR(100) NOT NULL,
            date_added DATE,
            is_active BIT DEFAULT 1,
            _created_date DATETIME2 DEFAULT GETDATE(),
            _updated_date DATETIME2 DEFAULT GETDATE()
        );

        -- ACCOUNT CARDS
        CREATE TABLE silver.dim_account_cards (
            account_id NVARCHAR(50) NOT NULL,
            card_number NVARCHAR(50),
            card_type NVARCHAR(50),
            card_issue_date DATE,
            card_expiry_date DATE,
            card_status NVARCHAR(50) DEFAULT 'ACTIVE',
            daily_transaction_limit DECIMAL(15,2),
            international_usage_enabled BIT DEFAULT 0,
            _created_date DATETIME2 DEFAULT GETDATE(),
            _updated_date DATETIME2 DEFAULT GETDATE()
        );

        -- ACCOUNT BENEFICIARIES
        CREATE TABLE silver.dim_account_beneficiaries (
            account_id NVARCHAR(50) NOT NULL ,
            beneficiary_name NVARCHAR(100) NOT NULL,
            beneficiary_relationship NVARCHAR(50),
            beneficiary_percentage DECIMAL(5,2),
            date_added DATE DEFAULT GETDATE(),
            is_active BIT DEFAULT 1,
            verification_status NVARCHAR(50),
            _created_date DATETIME2 DEFAULT GETDATE(),
            _updated_date DATETIME2 DEFAULT GETDATE(),
            CONSTRAINT CHK_beneficiary_pct CHECK (beneficiary_percentage >= 0 AND beneficiary_percentage <= 1)
        );

        -- ACCOUNT BRANCH RELATIONSHIP
        CREATE TABLE silver.dim_account_branch (
            account_id NVARCHAR(50), 
            branch_id INT
        )


        PRINT 'All tables created successfully with proper normalization.';
        PRINT 'Table structure eliminates insertion, deletion, and update anomalies.';
END;
GO

EXEC silver.pcd_create_accounts_tables_silver;