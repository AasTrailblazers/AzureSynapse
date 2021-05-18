/*
	Sample Contoso database can be installed from here:
	https://docs.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-load-from-azure-blob-storage-with-polybase
	https://github.com/microsoft/sql-server-samples/blob/master/samples/databases/contoso-data-warehouse/load-contoso-data-warehouse-to-sql-data-warehouse.sql
*/

/*
	-- A: Create a master key.
	-- Only necessary if one does not already exist.
	-- Required to encrypt the credential secret in the next step.

	CREATE MASTER KEY;

	-- Create an external data source
	-- TYPE: HADOOP - PolyBase uses Hadoop APIs to access data in Azure blob storage.
	-- LOCATION: Provide Azure storage account name and blob container name.
	-- CREDENTIAL: Provide the credential created in the previous step.

	CREATE EXTERNAL DATA SOURCE AzureStorage_west_public
	WITH 
	(  
		TYPE = Hadoop 
	,   LOCATION = 'wasbs://contosoretaildw-tables@contosoretaildw.blob.core.windows.net/'
	); 
	GO

	-- The data is stored in text files in Azure blob storage, and each field is separated with a delimiter. 
	-- Run this [CREATE EXTERNAL FILE FORMAT][] command to specify the format of the data in the text files. 
	-- he Contoso data is uncompressed and pipe delimited.

	CREATE EXTERNAL FILE FORMAT TextFileFormatContoso 
	WITH 
	(   FORMAT_TYPE = DELIMITEDTEXT
	,	FORMAT_OPTIONS	(   FIELD_TERMINATOR = '|'
						,	STRING_DELIMITER = ''
						,	DATE_FORMAT		 = 'yyyy-MM-dd HH:mm:ss.fff'
						,	USE_TYPE_DEFAULT = FALSE 
						)
	);
	GO
*/
/*
	-- To create a place to store the Contoso data in your database, create a schema
	-- for external tables and a schema for internal tables.
	CREATE SCHEMA [asb]
	GO
	CREATE SCHEMA [cso]
	GO
*/
/*
	--Clean up objects created in this example
	DROP EXTERNAL TABLE [asb].FactOnlineSales
	DROP TABLE [cso].[FactOnlineSalesRR]
	DROP TABLE [cso].[FactOnlineSalesHash]
	DROP TABLE [cso].[FactOnlineSalesHashStoreKey]
	DROP EXTERNAL TABLE [asb].DimProduct
	DROP TABLE [cso].[DimProductRR]
	DROP TABLE [cso].[DimProductProductKey]            
	DROP TABLE [cso].[DimProductProductLabel]
	DROP TABLE [cso].[DimProductReplicate]
	GO
	--DROP EXTERNAL FILE FORMAT TextFileFormatContoso
	--DROP EXTERNAL DATA SOURCE AzureStorage_west_public
	--GO
	--DROP SCHEMA [asb]
	--GO
	--DROP SCHEMA [cso]
	--GO
*/

--Part 1
SELECT  [pdw_node_id]   AS node_id
,       [type]          AS node_type
,       [name]          AS node_name
FROM    sys.[dm_pdw_nodes];

SELECT  [distribution_id]   AS dist_id
,       [pdw_node_id]       AS node_id
,       [name]              AS dist_name
,       [position]          AS dist_position
FROM    sys.[pdw_distributions];

--Part 2
CREATE EXTERNAL TABLE [asb].FactOnlineSales
(
    [OnlineSalesKey] [int]  NOT NULL,
    [DateKey] [datetime] NOT NULL,
    [StoreKey] [int] NOT NULL,
    [ProductKey] [int] NOT NULL,
    [PromotionKey] [int] NOT NULL,
    [CurrencyKey] [int] NOT NULL,
    [CustomerKey] [int] NOT NULL,
    [SalesOrderNumber] [nvarchar](20) NOT NULL,
    [SalesOrderLineNumber] [int] NULL,
    [SalesQuantity] [int] NOT NULL,
    [SalesAmount] [money] NOT NULL,
    [ReturnQuantity] [int] NOT NULL,
    [ReturnAmount] [money] NULL,
    [DiscountQuantity] [int] NULL,
    [DiscountAmount] [money] NULL,
    [TotalCost] [money] NOT NULL,
    [UnitCost] [money] NULL,
    [UnitPrice] [money] NULL,
    [ETLLoadID] [int] NULL,
    [LoadDate] [datetime] NULL,
    [UpdateDate] [datetime] NULL
)
WITH
(
    LOCATION='/FactOnlineSales/'
,   DATA_SOURCE = AzureStorage_west_public
,   FILE_FORMAT = TextFileFormatContoso
,   REJECT_TYPE = VALUE
,   REJECT_VALUE = 0
);

--Create Tables - takes time based on data volume, performance level and resource class
CREATE TABLE [cso].[FactOnlineSalesRR]       
WITH (DISTRIBUTION = ROUND_ROBIN) --Round Robin
AS 
SELECT * FROM [asb].[FactOnlineSales]        
OPTION (LABEL = 'CTAS : Load [cso].[FactOnlineSalesRR]');

CREATE TABLE [cso].[FactOnlineSalesHash]       
WITH (DISTRIBUTION = HASH([ProductKey])) --Hash Distributed
AS 
SELECT * FROM [asb].[FactOnlineSales]        
OPTION (LABEL = 'CTAS : Load [cso].[FactOnlineSalesHash]');

--Create necessary statistics

/*
	Create the dbo.vTableSizes with the source code available here:
	https://docs.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-overview#table-size-queries
*/
SELECT pdw_node_id, schema_name, table_name, node_table_name, distribution_id, 
	row_count, distribution_policy_name, distribution_column, index_type_desc,
	reserved_space_GB, unused_space_GB, data_space_GB, index_space_GB
FROM dbo.vTableSizes
WHERE table_name = 'FactOnlineSalesRR'
ORDER BY distribution_id

SELECT pdw_node_id, schema_name, table_name, node_table_name, distribution_id, 
	row_count, distribution_policy_name, distribution_column, index_type_desc,
	reserved_space_GB, unused_space_GB, data_space_GB, index_space_GB
FROM dbo.vTableSizes
WHERE table_name = 'FactOnlineSalesHash'
ORDER BY distribution_id

--Query Tables and look at estimated execution plan
SELECT [ProductKey], Sum([SalesAmount]) 
FROM [cso].[FactOnlineSalesRR] --Round Robin
GROUP BY ProductKey
ORDER BY ProductKey
GO
SELECT [ProductKey], Sum([SalesAmount]) 
FROM [cso].[FactOnlineSalesHash] --Hash Distributed
GROUP BY ProductKey
ORDER BY ProductKey
GO

--Analyze Plans
EXPLAIN SELECT [ProductKey], Sum([SalesAmount]) 
FROM [cso].[FactOnlineSalesRR] --Round Robin
GROUP BY ProductKey
ORDER BY ProductKey
GO
EXPLAIN SELECT [ProductKey], Sum([SalesAmount]) 
FROM [cso].[FactOnlineSalesHash] --Hash Distributed
GROUP BY ProductKey
ORDER BY ProductKey
GO

--Joining Hash Tables
CREATE EXTERNAL TABLE [asb].DimProduct (
    [ProductKey] [int] NOT NULL,
    [ProductLabel] [nvarchar](255) NULL,
    [ProductName] [nvarchar](500) NULL,
    [ProductDescription] [nvarchar](400) NULL,
    [ProductSubcategoryKey] [int] NULL,
    [Manufacturer] [nvarchar](50) NULL,
    [BrandName] [nvarchar](50) NULL,
    [ClassID] [nvarchar](10) NULL,
    [ClassName] [nvarchar](20) NULL,
    [StyleID] [nvarchar](10) NULL,
    [StyleName] [nvarchar](20) NULL,
    [ColorID] [nvarchar](10) NULL,
    [ColorName] [nvarchar](20) NOT NULL,
    [Size] [nvarchar](50) NULL,
    [SizeRange] [nvarchar](50) NULL,
    [SizeUnitMeasureID] [nvarchar](20) NULL,
    [Weight] [float] NULL,
    [WeightUnitMeasureID] [nvarchar](20) NULL,
    [UnitOfMeasureID] [nvarchar](10) NULL,
    [UnitOfMeasureName] [nvarchar](40) NULL,
    [StockTypeID] [nvarchar](10) NULL,
    [StockTypeName] [nvarchar](40) NULL,
    [UnitCost] [money] NULL,
    [UnitPrice] [money] NULL,
    [AvailableForSaleDate] [datetime] NULL,
    [StopSaleDate] [datetime] NULL,
    [Status] [nvarchar](7) NULL,
    [ImageURL] [nvarchar](150) NULL,
    [ProductURL] [nvarchar](150) NULL,
    [ETLLoadID] [int] NULL,
    [LoadDate] [datetime] NULL,
    [UpdateDate] [datetime] NULL
)
WITH
(
    LOCATION='/DimProduct/'
,   DATA_SOURCE = AzureStorage_west_public
,   FILE_FORMAT = TextFileFormatContoso
,   REJECT_TYPE = VALUE
,   REJECT_VALUE = 0
)
;

CREATE TABLE [cso].[DimProductProductKey]            
WITH (DISTRIBUTION = HASH([ProductKey])) 
AS 
SELECT * FROM [asb].[DimProduct] 
OPTION (LABEL = 'CTAS : Load [cso].[DimProductProductKey]');
GO

CREATE TABLE [cso].[DimProductProductLabel]            
WITH (DISTRIBUTION = HASH([ProductLabel])) 
AS 
SELECT * FROM [asb].[DimProduct] 
OPTION (LABEL = 'CTAS : Load [cso].[DimProductProductLabel]');
GO

CREATE TABLE [cso].[DimProductRR]            
WITH (DISTRIBUTION = ROUND_ROBIN) 
AS 
SELECT * FROM [asb].[DimProduct] 
OPTION (LABEL = 'CTAS : Load [cso].[DimProductRR]');
GO

--Join with compatible joinining key
SELECT   p.[ProductKey], SUM(f.[SalesAmount]) 
FROM    [cso].[FactOnlineSalesHash] AS f
JOIN    [cso].[DimProductProductKey] AS p ON f.[ProductKey] = p.[ProductKey]
GROUP BY p.[ProductKey]
GO

--Join with incompatible joinining key
SELECT  SUM(f.[SalesAmount]) 
,       p.[ProductKey]
FROM    [cso].[FactOnlineSalesHash] AS f
JOIN    [cso].[DimProductProductLabel] AS p ON f.[ProductKey] = p.[ProductKey]
GROUP BY p.[ProductKey]
GO

--Data Skewness Example
--The rows per distribution can vary up to 10% without a noticeable impact on performance.
CREATE TABLE [cso].[FactOnlineSalesHashStoreKey]       
WITH (DISTRIBUTION = HASH([StoreKey])) --Hash Distributed
AS 
SELECT * FROM [asb].[FactOnlineSales]        
OPTION (LABEL = 'CTAS : Load [cso].[FactOnlineSalesHashStoreKey]');

SELECT pdw_node_id, schema_name, table_name, node_table_name, distribution_id, 
	row_count, distribution_policy_name, distribution_column, index_type_desc,
	reserved_space_GB, unused_space_GB, data_space_GB, index_space_GB
FROM dbo.vTableSizes
WHERE table_name = 'FactOnlineSalesHashStoreKey'
ORDER BY pdw_node_id

--Replicated Table
--Join with RR tables
SELECT   p.[ProductKey], SUM(f.[SalesAmount]) 
FROM    [cso].[FactOnlineSalesRR] AS f
JOIN    [cso].[DimProductRR] AS p ON f.[ProductKey] = p.[ProductKey]
GROUP BY p.[ProductKey]
GO

CREATE TABLE [cso].[DimProductReplicate]            
WITH (DISTRIBUTION = Replicate) 
AS 
SELECT * FROM [asb].[DimProduct] 
OPTION (LABEL = 'CTAS : Load [cso].[DimProductReplicate]')
GO

--To trigger a rebuild, run the following statement on each table in the preceding output
--rebuild reads immediately from the master version of the table while the data is 
--asynchronously copied to each Compute node. Until the data copy is complete, 
--subsequent queries will continue to use the master version of the table.
SELECT TOP 1 * FROM [cso].[DimProductReplicate] ;

SELECT   p.[ProductKey], SUM(f.[SalesAmount]) 
FROM    [cso].[FactOnlineSalesRR] AS f
JOIN    [cso].[DimProductReplicate] AS p ON f.[ProductKey] = p.[ProductKey]
GROUP BY p.[ProductKey]
GO

SELECT   p.[ProductKey], SUM(f.[SalesAmount]) 
FROM    [cso].[FactOnlineSalesHash] AS f
JOIN    [cso].[DimProductProductKey] AS p ON f.[ProductKey] = p.[ProductKey]
GROUP BY p.[ProductKey]
GO
SELECT   p.[ProductKey], SUM(f.[SalesAmount]) 
FROM    [cso].[FactOnlineSalesHash] AS f
JOIN    [cso].[DimProductReplicate] AS p ON f.[ProductKey] = p.[ProductKey]
GROUP BY p.[ProductKey]
GO
