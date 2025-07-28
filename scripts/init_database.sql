-- WARNING:
-- This will drop the 'data_warehouse' database if it exists.
-- All data will be permanently deleted.

-- Run this part while connected to another database (e.g., 'postgres')
DROP DATABASE IF EXISTS data_warehouse;
CREATE DATABASE data_warehouse;

-- After creation, connect to the new database:
-- In psql: \c data_warehouse

-- Once connected to 'data_warehouse', run:
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
