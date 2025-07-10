--Procedure to perform ETL process to transfer data from bronze schema to silver schema
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @table_start_time DATETIME,
			@table_end_time DATETIME,
			@proc_start_time DATETIME,
			@proc_end_time DATETIME
	BEGIN TRY
		PRINT ('STARTING BATCH')
		SET @proc_start_time = GETDATE()
		/* 
			Purpose: cleaning and adding data from the bronze.crm_cust_info to silver.crm_cust_info
	
			Data cleaning: 
				Find duplicates based on cst_create_date that is not null
				Standardize marital status to Married, Single or N/A
				Standardize customer gender to Male, Female or N/A
		*/
		PRINT ('BEGIN INSERTING DATA INTO silver.crm_cust_info')
		SET @table_start_time = GETDATE()
		TRUNCATE TABLE silver.crm_cust_info;
		with dup_order_date_cte as (
		select *,
			row_number() over (partition by cst_id order by cst_create_date desc) as date_flag
		from bronze.crm_cust_info
		where cst_id IS NOT NULL
		)
		insert into silver.crm_cust_info (
			cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date) 
		select cst_id, 
				cst_key,
				TRIM(cst_firstname) as cst_firstname,
				TRIM(cst_lastname) as cst_lastname,
				CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
					 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
					 ELSE 'N/A'
				END AS cst_marital_status,
				CASE UPPER(TRIM (cst_gndr))
					WHEN 'M' THEN 'Male'
					WHEN 'F' THEN 'Female'
					ELSE 'N/A'
				END AS cst_gndr,
				cst_create_date
		from dup_order_date_cte
		where date_flag = 1
		SET @table_end_time = GETDATE()
		PRINT ('FINISH LOADING DATA INTO silver.crm_cust_info');
		PRINT ('>> LOAD DURATION: ' + CAST(DATEDIFF(second,@table_start_time,@table_end_time) AS NVARCHAR) + 'seconds.');
		/* 
			Purpose: cleaning and adding data from the bronze.crm_prd_info to silver.crm_prd_info
	
			Data cleaning: 
				Extract the category id from the first 5 characters of the original prd_key 
					category id is needed to connect to the erp_px_cat_g1v2 
				Extract the product key from the the original prd_key starting at the 7th character
				Standardize product line to Moutain, Road, Other Sales, Touring or N/A
				Set start and end date to DATE data type
		*/
		PRINT ('BEGIN INSERTING DATA INTO silver.crm_prd_info')
		SET @table_start_time = GETDATE()
		TRUNCATE TABLE silver.crm_prd_info;
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT 
			prd_id, 
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL (prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line)) 
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'N/A'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) as prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE ) AS prd_end_dt
		FROM bronze.crm_prd_info
		SET @table_end_time = GETDATE()
		PRINT ('FINISH LOADING DATA INTO silver.crm_prd_info');
		PRINT ('>> LOAD DURATION: ' + CAST(DATEDIFF(second,@table_start_time,@table_end_time) AS NVARCHAR) + 'seconds.');
		/*
			Purpose: cleaning and adding data from the bronze.crm_sales_details to silver.crm_sales_details 

			Data cleaning: 
				For date: 
					IF order any date is negative or out of format, then make them NULL
				For quantity, sale and price: 
					If sales is negative, 0 or NULL, then re-calculate using quantity and price
					If price is negative, 0 or NULL, then re-calculate using sales and price
		*/
		PRINT ('BEGIN INSERTING DATA INTO silver.crm_sales_details')
		SET @table_start_time = GETDATE()
		TRUNCATE TABLE silver.crm_sales_details
		INSERT INTO silver.crm_sales_details (
			sls_ord_num  ,
			sls_prd_key  ,
			sls_cust_id  ,
			sls_order_dt ,
			sls_ship_dt  ,
			sls_due_dt   ,
			sls_sales    ,
			sls_quantity ,
			sls_price    
		)
		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		--check for negative date and correct accordingly
		CASE WHEN sls_order_dt < 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE )
			END AS sls_order_dt,
		CASE WHEN sls_ship_dt < 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE )
			END AS sls_ship_dt,
		CASE WHEN sls_due_dt < 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE )
			END AS sls_due_dt,
		--check for invalid sales, quantity and price and correct accordingly
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0 
				THEN sls_sales / NULLIF(sls_quantity,0)
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details
		PRINT ('FINISH LOADING DATA INTO silver.crm_sales_details');
		PRINT ('>> LOAD DURATION: ' + CAST(DATEDIFF(second,@table_start_time,@table_end_time) AS NVARCHAR) + 'seconds.');
		/* 
			Purpose: cleaning and adding data from the bronze.erp_cust_az12 to silver.erp_cust_az12
	
			Data cleaning: 
				Remove the 'NAS' from the cid column 
				IF bdate (birthdate) is greater than current date, then set to NULL
				Standardize gender to either N/A, Male or Female  
		*/
		PRINT ('BEGIN INSERTING DATA INTO silver.erp_cust_az12')
		SET @table_start_time = GETDATE()
		TRUNCATE TABLE silver.erp_cust_az12
		INSERT INTO silver.erp_cust_az12 (
			cid, bdate, gen
		)
		SELECT 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
				ELSE cid
			END AS cid,
			CASE WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate,
			CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				ELSE 'N/A'
			END AS gen
		FROM bronze.erp_cust_az12
		PRINT ('FINISH LOADING DATA INTO silver.erp_cust_az12');
		PRINT ('>> LOAD DURATION: ' + CAST(DATEDIFF(second,@table_start_time,@table_end_time) AS NVARCHAR) + 'seconds.');
		/* 
			Purpose: cleaning and adding data from the bronze.erp_loc_a101 to silver.erp_loc_a101
	
			Data cleaning: 
				Remove the '-' in cid
				For country:
					Replace 'DE' in country with Germany 
					Replace 'US' and 'USA' in country with United States 
					If null or no entry, set to 'N/A'
		*/
		PRINT ('BEGIN INSERTING DATA INTO silver.erp_loc_a101')
		SET @table_start_time = GETDATE()
		TRUNCATE TABLE silver.erp_loc_a101
		INSERT INTO silver.erp_loc_a101 (cid,cntry)
		SELECT
			REPLACE(cid,'-','') as cid,
			CASE WHEN cntry = 'DE' THEN 'Germany'
				WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
				WHEN cntry = '' OR cntry IS NULL THEN 'N/A'
				ELSE cntry
			END AS cnty
		FROM bronze.erp_loc_a101
		PRINT ('FINISH LOADING DATA INTO silver.erp_loc_a101');
		PRINT ('>> LOAD DURATION: ' + CAST(DATEDIFF(second,@table_start_time,@table_end_time) AS NVARCHAR) + 'seconds.');
		-- Purpose: adding data from the bronze.erp_px_cat_g1v2 to silver.erp_px_cat_g1v2
		PRINT ('BEGIN INSERTING DATA INTO silver.erp_px_cat_g1v2')
		SET @table_start_time = GETDATE()
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,cat,subcat,maintenance
		)
		SELECT id,cat,subcat,maintenance FROM bronze.erp_px_cat_g1v2
		PRINT ('FINISH LOADING DATA INTO silver.erp_px_cat_g1v2');
		PRINT ('>> LOAD DURATION: ' + CAST(DATEDIFF(second,@table_start_time,@table_end_time) AS NVARCHAR) + 'seconds.');
		SET @proc_end_time = GETDATE()
		PRINT('ENDING BATCH')
		PRINT('>> TOTAL TIME RUNNING BATCH: ' + CAST(DATEDIFF(second,@proc_start_time,@proc_end_time) AS VARCHAR) + ' seconds');
	END TRY
	BEGIN CATCH
		PRINT ('ERROR MESSAGE: ' + ERROR.MESSAGE() );
		PRINT ('ERROR NUMBER: ' + CAST(ERROR.NUMBER() AS NVARCHAR));
		PRINT ('ERROR STATE: ' + CAST(ERROR.STATE() AS NVARCHAR));
	END CATCH
END

EXEC silver.load_silver
