/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source → Bronze)
===============================================================================

Procedure Overview:
    This stored procedure is responsible for loading data into the 'bronze'
    schema from external CSV files.

    The procedure performs the following steps:
    - Clears existing data from bronze tables before loading new records.
    - Loads data into bronze tables using the BULK INSERT operation from CSV files.

Parameters:
    None.
    This procedure does not accept input parameters and does not return any output.

Execution Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROC bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
			set @batch_start_time = GETDATE();
			print '==================================='
			print 'Loading the data to Bronze layer'
			print '==================================='

			print '***********************************'
			print 'Loading CRM Tables'
			print '***********************************'

			set @start_time = GETDATE();
			print '>> Truncating Table: bronze.crm_cust_info';
			truncate table bronze.crm_cust_info;

			print '>> Inserting Data into: bronze.crm_cust_info';
			Bulk insert bronze.crm_cust_info
			from 'D:\My Files\source\datasets\source_crm\cust_info.csv'
			with(
				firstrow = 2,
				fieldterminator = ',',
				tablock
			)
			set @end_time = GETDATE();
			print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) As Nvarchar) + 'seconds';
			print '>>----------------------------------';

			set @start_time = GETDATE();
			print '>> Truncating Table: bronze.crm_prd_info';
			truncate table bronze.crm_prd_info;

			print '>> Inserting Data into: bronze.crm_prd_info';
			Bulk insert bronze.crm_prd_info
			from 'D:\My Files\source\datasets\source_crm\prd_info.csv'
			with(
				firstrow = 2,
				fieldterminator = ',',
				tablock
			)
			set @end_time = GETDATE();
			print '>> Loading Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar ) + 'seconds';
			print '>>----------------------------------';

			set @end_time = GETDATE();
			print '>> Truncating Table: bronze.crm_sales_details';
			truncate table bronze.crm_sales_details;

			print '>> Inserting Data into: bronze.crm_sales_details';
			Bulk insert bronze.crm_sales_details
			from 'D:\My Files\source\datasets\source_crm\sales_details.csv'
			with(
				firstrow = 2,
				fieldterminator = ',',
				tablock
			)
			set @end_time = GETDATE();
			print '>> Loading Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar ) + 'seconds';
			print '>>----------------------------------';



			print '***********************************';
			print 'Loading ERP Tables';
			print '***********************************';

			set @start_time = GETDATE();
			print '>> Truncating Table: bronze.erp_cust_az12';
			truncate table bronze.erp_cust_az12;

			print '>> Inserting Data into: bronze.erp_cust_az12';
			Bulk insert bronze.erp_cust_az12
			from 'D:\My Files\source\datasets\source_erp\CUST_AZ12.csv'
			with(
				firstrow = 2,
				fieldterminator = ',',
				tablock
			)
			set @end_time = GETDATE();
			print '>> Loading Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar ) + 'seconds';
			print '>>----------------------------------';

			set @start_time = GETDATE();
			print '>> Truncating Table: bronze.erp_loc_a101';
			truncate table bronze.erp_loc_a101;
			print '>> Inserting Data into: bronze.erp_loc_a101';
			Bulk insert bronze.erp_loc_a101
			from 'D:\My Files\source\datasets\source_erp\LOC_A101.csv'
			with(
				firstrow = 2,
				fieldterminator = ',',
				tablock
			)
			set @end_time = GETDATE();
			print '>> Loading Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar ) + 'seconds';
			print '>>----------------------------------';

			set @start_time = GETDATE();
			print '>> Truncating Table: bronze.erp_px_cat_g1v2';
			truncate table bronze.erp_px_cat_g1v2;

			print '>> Inserting Data into:bronze.erp_px_cat_g1v2';
			Bulk insert bronze.erp_px_cat_g1v2
			from 'D:\My Files\source\datasets\source_erp\PX_CAT_G1V2.csv'
			with(
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
			set @end_time = GETDATE();
			print '>> Loading Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar ) + 'seconds';
			print '>>----------------------------------';

			set @batch_end_time = GETDATE();
			print '=============================================='
			print 'Loading the data to Bronze layer is completed'
			print ' -- Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
			print '=============================================='

	end try
begin catch
	print '````````````````````````````````````````````'
	print 'ERROR OCCURED DURING LOADING BRONZE LAYER';
	print 'Error Message' + ERROR_Message();
	print 'Error Message' + cast(ERROR_Number() as nvarchar);
	print 'Error Message' + cast(ERROR_Number() as nvarchar);
	print 'Error Message' + cast(ERROR_state() as nvarchar);
	print '````````````````````````````````````````````'
end catch
end
