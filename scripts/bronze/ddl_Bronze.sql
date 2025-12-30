/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================

Script Purpose:
    This script creates tables within the bronze schema and removes any existing tables before creation.
    It should be executed to redefine the DDL structure of the bronze tables.
    
    Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

if OBJECT_ID('bronze.crm_cust_info','U') is not null
drop table bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_Date Date
);

if OBJECT_ID('bronze.crm_prd_info','U') is not null
drop table bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);

if OBJECT_ID('bronze.crm_sales_details','U') is not null
drop table bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

if OBJECT_ID('bronze.erp_loc_a101','U') is not null
drop table bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);

if OBJECT_ID('bronze.erp_cust_az12','U') is not null
drop table bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);

if OBJECT_ID('bronze.erp_px_cat_g1v2','U') is not null
drop table bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);

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


exec bronze.load_bronze

