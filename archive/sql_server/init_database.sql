/*
===============================================================================
Create Database and Schemas
===============================================================================
Script Purpose:
    This script creates a new database named 'PharmacyEnterpriseDW' after
    checking if it already exists.

    If the database exists, it is dropped and recreated.

    Additionally, the script creates the three schemas used in the
    Medallion Architecture:

        - bronze
        - silver
        - gold

WARNING:
    Running this script will drop the entire 'PharmacyEnterpriseDW'
    database if it already exists.

    All data inside the database will be permanently deleted.

    Proceed with caution and make sure you have proper backups before
    running this script in any environment containing important data.
===============================================================================
*/

USE master;
GO


-- Drop and recreate the 'PharmacyEnterpriseDW' database
IF EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE name = 'PharmacyEnterpriseDW'
)
BEGIN
    ALTER DATABASE PharmacyEnterpriseDW
        SET SINGLE_USER
        WITH ROLLBACK IMMEDIATE;

    DROP DATABASE PharmacyEnterpriseDW;
END;
GO


-- Create the 'PharmacyEnterpriseDW' database
CREATE DATABASE PharmacyEnterpriseDW;
GO


-- Use the newly created database
USE PharmacyEnterpriseDW;
GO


-- Create Bronze Schema
CREATE SCHEMA bronze;
GO


-- Create Silver Schema
CREATE SCHEMA silver;
GO


-- Create Gold Schema
CREATE SCHEMA gold;
GO
