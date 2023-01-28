USE ROLE ACCOUNTADMIN;

-- Create the `transform` role
CREATE ROLE IF NOT EXISTS transform;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Create the `dbt` user and assign to role
CREATE USER IF NOT EXISTS dbt_felix
  PASSWORD='dbtPassword'
  LOGIN_NAME='dbt_user'
  MUST_CHANGE_PASSWORD=TRUE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE='transform'
  DEFAULT_NAMESPACE='GROCERY_STORE.RAW'
  COMMENT='DBT user used for data transformation';


  
GRANT ROLE transform to USER dbt_user;

-- Create our database and schemas
CREATE DATABASE IF NOT EXISTS GROCERY_STORE;
CREATE SCHEMA IF NOT EXISTS GROCERY_STORE.RAW;

-- Set up permissions to role `transform`
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE transform; 
GRANT ALL ON DATABASE GROCERY_STORE to ROLE transform;

GRANT ALL ON ALL SCHEMAS IN DATABASE GROCERY_STORE to ROLE transform;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE GROCERY_STORE to ROLE transform;
GRANT ALL ON ALL TABLES IN SCHEMA GROCERY_STORE.RAW to ROLE transform;
GRANT ALL ON FUTURE TABLES IN SCHEMA GROCERY_STORE.RAW to ROLE transform;


USE WAREHOUSE COMPUTE_WH;
USE DATABASE grocery_store;
USE SCHEMA RAW;


CREATE  OR REPLACE  FILE FORMAT  upload_csv
                      TYPE = CSV 
                     field_delimiter = ','
                     skip_header = 1,
                      empty_field_as_null = true,
                      compression = gzip;


CREATE OR REPLACE TABLE sales (
TRANSACTION_ID number,
TRANSACTIONAL_DATE DATETIME,
PRODUCT_ID character varying,
CUSTOMER_ID number,
PAYMENT character varying,
CREDIT_CARD number,
LOYALTY_CARD character(1),
COST number(12,2),
QUANTITY number,
PRICE number(12,2)
)



create or replace stage my_csv_stage
  file_format = upload_csv;


grant ALL on stage my_csv_stage to role accountadmin;
grant ALL on stage my_csv_stage to role transform;


select t.$1, t.$2  from @my_csv_stage  (file_format => 'upload_csv', pattern=>'.*Sales.*[.]csv.gz') t;


copy into sales
  from @%my_csv_stage
  file_format = (format_name = 'upload_csv' , error_on_column_count_mismatch=false)
  pattern = '.*Sales.csv.gz'
  on_error = 'skip_file';

DROP TABLE IF EXISTS products;
CREATE TABLE products (
    product_id character(6),
    product_name character varying,
    category character varying,
    subcategory character varying
   );
   
copy into products
  from @my_csv_stage
  file_format = (format_name = 'upload_csv' , error_on_column_count_mismatch=false)
  pattern = '.*Products.csv.gz'
  on_error = 'skip_file';