/*
===============================================================================
Create Database and Schemas
Project: Pharmacy Enterprise Data Warehouse
===============================================================================
*/

-- Create Database
USE master;
GO

CREATE DATABASE PharmacyEnterpriseDW;
GO

-- Use Database
USE PharmacyEnterpriseDW;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
