/*
1. Open pgAdmin
2. In Object Explorer (left-hand pane), navigate to `sql_course` database
3. Right-click `sql_course` and select `PSQL Tool`
    - This opens a terminal window to write the following code
4. Get the absolute file path of your csv files
    1. Find path by right-clicking a CSV file in VS Code and selecting “Copy Path”
5. Paste the following into `PSQL Tool`, (with the CORRECT file path)

\copy bronze.crm_cust_info FROM 'C:\Users\dings\OneDrive\Desktop\Baraa_DataWarehouse\datasets\source_crm\cust_info.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

\copy bronze.crm_prd_id FROM 'C:\Users\dings\OneDrive\Desktop\Baraa_DataWarehouse\datasets\source_crm\prd_info.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

\copy bronze.crm_sales_details FROM 'C:\Users\dings\OneDrive\Desktop\Baraa_DataWarehouse\datasets\source_crm\sales_details.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

\copy bronze.erp_cust_az12 FROM 'C:\Users\dings\OneDrive\Desktop\Baraa_DataWarehouse\datasets\soure_erp\CUST_AZ12.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

\copy bronze.erp_loc_a101 FROM 'C:\Users\dings\OneDrive\Desktop\Baraa_DataWarehouse\datasets\soure_erp\LOC_A101.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

\copy bronze.erp_px_cat_g1v2 FROM 'C:\Users\dings\OneDrive\Desktop\Baraa_DataWarehouse\datasets\soure_erp\PX_CAT_G1V2.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
*/
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
-- TO CREATE STORED PROCEDURE & TIMESTAMP --

Procedure is used while : 
1. If CSV file need to be reload frequently 
2. Avoid forgetting to TRUNCATE, load a table or running steps in wrong order
3. Automate the ETL process via CALL bronze.load_bronze()
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE 
    start_time          TIMESTAMP; 
    end_time            TIMESTAMP;
    batch_start_time    TIMESTAMP;
    batch_end_time      TIMESTAMP;

BEGIN
    batch_start_time := NOW();
    RAISE NOTICE '============================';
    RAISE NOTICE 'Starting Bronze Load';
    RAISE NOTICE '============================';

    ----------------------------------------------------------------------------------------------------------
    -- CRM: crm_cust_info
    ----------------------------------------------------------------------------------------------------------
    RAISE NOTICE '>> Loading bronze.crm_cust_info';

    -- Check start time to bulk load bronze.crm_cust_info
    start_time := NOW();

    -- Step 1: Remove existing data
    TRUNCATE TABLE bronze.crm_cust_info;
    -- Step 2: Load CSV file
    COPY bronze.crm_cust_info 
        FROM 'C:\Users\dings\OneDrive\Desktop\Baraa_DataWarehouse\datasets\source_crm\cust_info.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

    -- Check end time to bulk load bronze.crm_cust_info
    end_time := NOW();

    RAISE NOTICE 'Duration: % seconds', 
        EXTRACT(epoch FROM (end_time - start_time));


    ----------------------------------------------------------------------------------------------------------
    -- CRM: crm_prd_id
    ----------------------------------------------------------------------------------------------------------
    RAISE NOTICE '>> Loading bronze.crm_prd_id';
    start_time := NOW();


    TRUNCATE TABLE bronze.crm_prd_id;
    COPY bronze.crm_prd_id
        FROM 'C:\Users\dings\OneDrive\Desktop\Baraa_DataWarehouse\datasets\source_crm\prd_info.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

    end_time := NOW();

    RAISE NOTICE 'Duration: % seconds', 
        EXTRACT(epoch FROM (end_time - start_time));


    ----------------------------------------------------------------------------------------------------------
    -- CRM: crm_sales_details
    ----------------------------------------------------------------------------------------------------------
    RAISE NOTICE '>> Loading bronze.crm_sales_details';
    start_time := NOW();


    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
        FROM 'C:\Users\dings\OneDrive\Desktop\Baraa_DataWarehouse\datasets\source_crm\sales_details.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
    
    end_time := NOW();

    RAISE NOTICE 'Duration: % seconds', 
        EXTRACT(epoch FROM (end_time - start_time));


    ----------------------------------------------------------------------------------------------------------
    -- CRM: erp_cust_az12
    ----------------------------------------------------------------------------------------------------------
    RAISE NOTICE '>> Loading bronze.erp_cust_az12';
    start_time := NOW();


    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
        FROM 'C:\Users\dings\OneDrive\Desktop\Baraa_DataWarehouse\datasets\soure_erp\CUST_AZ12.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
    
    end_time := NOW();

    RAISE NOTICE 'Duration: % seconds', 
        EXTRACT(epoch FROM (end_time - start_time));


    ----------------------------------------------------------------------------------------------------------
    -- CRM: erp_loc_a101
    ----------------------------------------------------------------------------------------------------------
    RAISE NOTICE '>> Loading bronze.erp_loc_a101';
    start_time := NOW();


    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
        FROM 'C:\Users\dings\OneDrive\Desktop\Baraa_DataWarehouse\datasets\soure_erp\LOC_A101.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

    end_time := NOW();

    RAISE NOTICE 'Duration: % seconds', 
        EXTRACT(epoch FROM (end_time - start_time));


    ----------------------------------------------------------------------------------------------------------
    -- CRM: erp_px_cat_g1v2
    ----------------------------------------------------------------------------------------------------------

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
        FROM 'C:\Users\dings\OneDrive\Desktop\Baraa_DataWarehouse\datasets\soure_erp\PX_CAT_G1V2.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
    
    end_time := NOW();

    RAISE NOTICE 'Duration: % seconds', 
        EXTRACT(epoch FROM (end_time - start_time));


    ----------------------------------------------------------------------------------------------------------
    -- Capture End Time & Error Message
    ----------------------------------------------------------------------------------------------------------

    batch_end_time := NOW();
    RAISE NOTICE '============================';
    RAISE NOTICE ' Bronze Load Complete';
    RAISE NOTICE ' Total Duration: % seconds',
        EXTRACT(epoch FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '============================';

    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '============================';
        RAISE NOTICE ' ERROR: %', SQLERRM;
        RAISE NOTICE '============================';

END;
$$;

SELECT *
FROM bronze.erp_cust_az12

CALL bronze.load_bronze();