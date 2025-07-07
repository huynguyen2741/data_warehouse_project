/* 
  Purpose: Truncate the tables and Load the data from 3 CRM files and 3 ERP files into 6 tables using BULK INSERT. 
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @table_start_time DATETIME,
			@table_end_time DATETIME,
			@proc_start_time DATETIME,
			@proc_end_time DATETIME
	BEGIN TRY
		SET @proc_start_time = GETDATE();
		PRINT ('LOADING DATA INTO crm_cust_info');
		SET @table_start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info 
		FROM 'C:\Users\huyng\Desktop\pySpark\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH ( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @table_end_time = GETDATE();
		PRINT ('FINISH LOADING DATA INTO crm_cust_info');
		PRINT ('>> LOAD DURATION: ' + CAST(DATEDIFF(second,@table_start_time,@table_end_time) AS NVARCHAR) + 'seconds.');

		PRINT ('LOADING DATA INTO crm_prd_info');
		SET @table_start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\huyng\Desktop\pySpark\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH ( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @table_end_time = GETDATE();
		PRINT ('FINISH LOADING DATA INTO crm_prd_info');
		PRINT ('>> LOAD DURATION: ' + CAST(DATEDIFF(second,@table_start_time,@table_end_time) AS NVARCHAR) + 'seconds.');

		PRINT ('LOADING DATA INTO crm_sales_details');
		SET @table_start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\huyng\Desktop\pySpark\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH ( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @table_end_time = GETDATE();
		PRINT ('FINISH LOADING DATA INTO crm_sales_details');
		PRINT ('>> LOAD DURATION: ' + CAST(DATEDIFF(second,@table_start_time,@table_end_time) AS NVARCHAR) + 'seconds.');

		PRINT ('LOADING DATA INTO erp_cust_az12');
		SET @table_start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\huyng\Desktop\pySpark\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH ( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @table_end_time = GETDATE();
		PRINT ('FINISH LOADING DATA INTO erp_cust_az12');
		PRINT ('>> LOAD DURATION: ' + CAST(DATEDIFF(second,@table_start_time,@table_end_time) AS NVARCHAR) + 'seconds.');

		PRINT ('LOADING DATA INTO erp_loc_a101');
		SET @table_start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\huyng\Desktop\pySpark\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH ( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @table_end_time = GETDATE();
		PRINT ('FINISH LOADING DATA INTO erp_loc_a101');
		PRINT ('>> LOAD DURATION: ' + CAST(DATEDIFF(second,@table_start_time,@table_end_time) AS NVARCHAR) + 'seconds.');

		PRINT ('LOADING DATA INTO erp_px_cat_g1v2');
		SET @table_start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\huyng\Desktop\pySpark\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH ( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @table_end_time = GETDATE();
		PRINT ('FINISH LOADING DATA INTO erp_px_cat_g1v2');
		PRINT ('>> LOAD DURATION: ' + CAST(DATEDIFF(second,@table_start_time,@table_end_time) AS NVARCHAR) + 'seconds.');
		SET @proc_end_time = GETDATE();
		PRINT ('TOTAL TIME LOADING BATCH: ' + CAST(DATEDIFF(second,@proc_start_time,@proc_end_time) AS NVARCHAR) + 'seconds.');
	END TRY
	BEGIN CATCH
		PRINT ('ERROR MESSAGE: ' + ERROR.MESSAGE() );
		PRINT ('ERROR NUMBER: ' + CAST(ERROR.NUMBER() AS NVARCHAR));
		PRINT ('ERROR STATE: ' + CAST(ERROR.STATE() AS NVARCHAR));
	END CATCH
END;

EXEC bronze.load_bronze;
