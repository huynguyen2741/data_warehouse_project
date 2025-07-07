/*
  This script is used to create the DataWarehouse database and the three schemas: bronze, silver and gold 
*/

USE master;
GO
  
-- Drop existing DataWarehouse database 
IF EXISTS ( SELECT 1 from sys.databases WHERE name = "DataWarehouse")
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

-- Create the database
CREATE DATABASE DateWarehouse;
GO

USE DataWahouse;
GO

-- Create bronze, silver and gold schema
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
