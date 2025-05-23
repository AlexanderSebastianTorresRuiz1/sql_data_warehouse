/*
================================================================
Stored Procedure - Carga de datos en la capa bronze
================================================================
Este store procedure nos permite cargar nuestros seis archivos en formato csv a nuestra capa bronze, incluyendo el 
tipo de carga de cada tabla y todo el script, permitiendonos sacar un mejor analisis para seguir obtimizando el
script.

Nota: Tener en cuenta que los archivo lo tengo en mi local y al usar el bulk insert, deben poner donde se encuentre 
la ruta donde lo tienen almacenado sus archivos.
*/


create or alter procedure bronze.carga_bronze as
	begin 
		declare @star_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime

			set @batch_start_time=getdate()

			print '=================================='
			print 'Carga de datos a la capa bronze.'
			print '=================================='

			set @star_time=getdate();

			-- Ingesta de datos(crm).
			print('----------------------------------')
			print '-------- Carga del CRM -----------'
			print('----------------------------------')

			set @star_time=getdate()

			truncate table bronze.crm_cust_info;

			bulk insert [bronze].[crm_cust_info]
			from '...\source_crm\cust_info.csv'
			with(
				firstrow=2,
				fieldterminator=',',
				tablock
			)
			set @end_time=getdate()

			print('=======================================')
			print 'Duracion: ' + cast(datediff(second,@star_time,@end_time) as nvarchar(255)) + 'segundos';
			print('=======================================')


			set @star_time=getdate()
			truncate table [bronze].[crm_prd_info];
			bulk insert [bronze].[crm_prd_info]
			from '...\source_crm\prd_info.csv'
			with(
				firstrow=2,
				fieldterminator=',',
				tablock
			)
			set @end_time=getdate()

			print('=======================================')
			print 'Duracion: '+ cast(datediff(second,@star_time,@end_time) as nvarchar(255)) + 'segundos.';
			print('=======================================')


			set @star_time=getdate()
			truncate table [bronze].[crm_sales_details];
			bulk insert [bronze].[crm_sales_details]
			from '...\source_crm\sales_details.csv'
			with(
				firstrow=2,
				fieldterminator=',',
				tablock
			)
			set @end_time=getdate()

			print('=======================================')
			print 'Duracion: '+ cast(datediff(second,@star_time,@end_time) as nvarchar(255)) + 'segundos.'
			print('=======================================')


			print('----------------------------------')
			print '-------- Carga del ERP -----------'
			print('----------------------------------')

			set @star_time=getdate()

			truncate table [bronze].[erp_cust_az12];
			-- Ingesta de datos(erp).
			bulk insert [bronze].[erp_cust_az12]
			from '...\source_erp\CUST_AZ12.csv'
			with(
				firstrow=2,
				fieldterminator=',',
				tablock
			)

			set @end_time=getdate()

			print('=======================================')
			print 'Duracion: '+ cast(datediff(second,@star_time,@end_time) as nvarchar(255)) + 'segundos.'
			print('=======================================')

			set @star_time=getdate()

			truncate table [bronze].[erp_loc_a101];
			bulk insert [bronze].[erp_loc_a101]
			from '...\source_erp\LOC_A101.csv'
			with(
				firstrow=2,
				fieldterminator=',',
				tablock
			)

			set @end_time=getdate()
			print('=======================================')
			print 'Duracion: '+ cast(datediff(second,@star_time,@end_time) as nvarchar(255)) + 'segundos.'
			print('=======================================')

			set @star_time=getdate()
			truncate table [bronze].[erp_px_cat_g1v2];
			bulk insert [bronze].[erp_px_cat_g1v2]
			from 'C:\Users\teale\OneDrive\Documentos\DATA WAREHOUSE\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			with(
				firstrow=2,
				fieldterminator=',',
				tablock
			)
			set @end_time=getdate()
			print('=======================================')
			print 'Duracion: '+ cast(datediff(second,@star_time,@end_time) as nvarchar(255)) + 'segundos.'
			print('=======================================')

			set @batch_end_time=getdate()

			print('=======================================')
			print 'Duracion: '+ cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar(255)) + 'segundos.'
			print('=======================================')

end


print('----------------------------------')
print '---- Ejecución del SP bronze -----'
print('----------------------------------')

EXEC bronze.cara_bronze
