use master;
-- Creamos la base de datos.
create database Datawarehouse;
-- Nos posicionamos en el base de datos.
use Datawarehouse;
GO
-- Creamos schemas (bronze, silver, gold)
create schema bronze;
GO
create schema silver;
GO
create schema gold;
GO
