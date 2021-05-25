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
	DROP EXTERNAL TABLE [asb].[FactOnlineSales];
	DROP TABLE [cso].[FactOnlineSalesRR];
	DROP EXTERNAL TABLE [asb].[DimProduct];
	DROP TABLE [cso].[DimProductRR];
	GO
	--DROP EXTERNAL FILE FORMAT TextFileFormatContoso
	--DROP EXTERNAL DATA SOURCE AzureStorage_west_public
	--GO
	--DROP SCHEMA [asb]
	--GO
	--DROP SCHEMA [cso]
	--GO
*/
--Part 1 - Columnstore index (default), Heap, Clustered and non-clustered Indexes
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

----Default is clustered Columstore index
IF OBJECT_ID('cso.DimProductRR', 'U') IS NOT NULL
    DROP TABLE [cso].[DimProductRR]
GO
CREATE TABLE [cso].[DimProductRR]            
WITH (DISTRIBUTION = ROUND_ROBIN) 
AS 
SELECT * FROM [asb].[DimProduct] 
OPTION (LABEL = 'CTAS : Load [cso].[DimProductRR]');
GO

/*
	Create the dbo.vTableSizes with the source code available here:
	https://docs.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-overview#table-size-queries
*/
SELECT pdw_node_id, schema_name, table_name, index_type_desc, node_table_name, distribution_id, 
	row_count, distribution_policy_name, distribution_column, 
	reserved_space_GB, unused_space_GB, data_space_GB, index_space_GB
FROM dbo.vTableSizes
WHERE table_name = 'DimProductRR'
ORDER BY distribution_id

----Heap
IF OBJECT_ID('cso.DimProductRR', 'U') IS NOT NULL
    DROP TABLE [cso].[DimProductRR]
GO
CREATE TABLE [cso].[DimProductRR]            
WITH (DISTRIBUTION = ROUND_ROBIN, HEAP) 
AS 
SELECT * FROM [asb].[DimProduct] 
OPTION (LABEL = 'CTAS : Load [cso].[DimProductRR]');
GO

SELECT pdw_node_id, schema_name, table_name, index_type_desc, node_table_name, distribution_id, 
	row_count, distribution_policy_name, distribution_column, 
	reserved_space_GB, unused_space_GB, data_space_GB, index_space_GB
FROM dbo.vTableSizes
WHERE table_name = 'DimProductRR'
ORDER BY distribution_id

----Clustered Index
IF OBJECT_ID('cso.DimProductRR', 'U') IS NOT NULL
    DROP TABLE [cso].[DimProductRR]
GO
CREATE TABLE [cso].[DimProductRR]            
WITH (DISTRIBUTION = ROUND_ROBIN, CLUSTERED INDEX ([ProductKey])) 
AS 
SELECT * FROM [asb].[DimProduct] 
OPTION (LABEL = 'CTAS : Load [cso].[DimProductRR]');
GO

SELECT pdw_node_id, schema_name, table_name, index_type_desc, node_table_name, distribution_id, 
	row_count, distribution_policy_name, distribution_column, 
	reserved_space_GB, unused_space_GB, data_space_GB, index_space_GB
FROM dbo.vTableSizes
WHERE table_name = 'DimProductRR'
ORDER BY distribution_id

----Non-Clustered Index
CREATE NONCLUSTERED INDEX IX_DimProduct_ProductSubcategoryKey   
    ON [cso].[DimProductRR] (ProductSubcategoryKey);  

SELECT 
	s.name, t.name, i.name, i.type_desc
FROM 
    sys.schemas s
INNER JOIN sys.tables t
    ON s.[schema_id] = t.[schema_id]
INNER JOIN sys.indexes i
    ON  t.[object_id] = i.[object_id]
WHERE s.name = 'cso' AND t.name = 'DimProductRR'

--Part 2 - Clustered Columnstore Index - rowgroup, column segments
CREATE EXTERNAL TABLE [asb].[FactOnlineSales]
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

----Drop existing table
IF OBJECT_ID('cso.FactOnlineSalesRR', 'U') IS NOT NULL
    DROP TABLE [cso].[FactOnlineSalesRR]
----Create Tables - takes time based on data volume, performance level and resource class
CREATE TABLE [cso].[FactOnlineSalesRR]       
WITH (DISTRIBUTION = ROUND_ROBIN, CLUSTERED COLUMNSTORE INDEX) -- Round Robin
AS 
SELECT * FROM [asb].[FactOnlineSales]        
OPTION (LABEL = 'CTAS : Load [cso].[FactOnlineSalesRR]');

----Create necessary statistics

----Analyze - Total rows, row in open, closed and compressed row groups
SELECT * FROM
(
	SELECT
		s.name AS [Schema Name]
		,t.name AS [Table Name]
		,rg.partition_number AS [Partition Number]
		,SUM(rg.total_rows) AS [Total Rows]
		,SUM(CASE WHEN rg.State = 1 THEN rg.Total_rows Else 0 END) AS [Rows in OPEN Row Groups]
		,SUM(CASE WHEN rg.State = 2 THEN rg.Total_Rows ELSE 0 END) AS [Rows in Closed Row Groups]
		,SUM(CASE WHEN rg.State = 3 THEN rg.Total_Rows ELSE 0 END) AS [Rows in COMPRESSED Row Groups]
	FROM sys.pdw_nodes_column_store_row_groups rg
	  JOIN sys.pdw_nodes_tables pt
		ON rg.object_id = pt.object_id
		AND rg.pdw_node_id = pt.pdw_node_id
		AND pt.distribution_id = rg.distribution_id
	  JOIN sys.pdw_table_mappings tm
		ON pt.name = tm.physical_name
	  INNER JOIN sys.tables t
		ON tm.object_id = t.object_id
	  INNER JOIN sys.schemas s
		ON t.schema_id = s.schema_id
	GROUP BY s.name, t.name, rg.partition_number
) Temp
WHERE [Table Name] = 'FactOnlineSalesRR'
GO

----Analyze - Total rows, deleted rows, percent deleted rows per distribution
SELECT IndexMap.object_id,   
  object_name(IndexMap.object_id) AS LogicalTableName,   
  CSRowGroups.*,  
  100*(ISNULL(deleted_rows,0))/total_rows AS PercentDeletedRows,
  i.name AS LogicalIndexName, IndexMap.index_id, NI.type_desc,   
  IndexMap.physical_name AS PhyIndexNameFromIMap
FROM sys.tables AS t  
JOIN sys.indexes AS i  
    ON t.object_id = i.object_id  
JOIN sys.pdw_index_mappings AS IndexMap  
    ON i.object_id = IndexMap.object_id  
    AND i.index_id = IndexMap.index_id  
JOIN sys.pdw_nodes_indexes AS NI  
    ON IndexMap.physical_name = NI.name  
    AND IndexMap.index_id = NI.index_id  
JOIN sys.pdw_nodes_column_store_row_groups AS CSRowGroups  
    ON CSRowGroups.object_id = NI.object_id   
    AND CSRowGroups.pdw_node_id = NI.pdw_node_id  
    AND CSRowGroups.distribution_id = NI.distribution_id
    AND CSRowGroups.index_id = NI.index_id      
WHERE t.name = 'FactOnlineSalesRR'   
ORDER BY object_name(i.object_id), i.name, IndexMap.physical_name, pdw_node_id;  

----Analyze - Number of column segment for each column of the table
SELECT * FROM
(
	SELECT  sm.name           as schema_nm
	,       tb.name           as table_nm
	,       nc.name           as col_nm
	,       nc.column_id
	,       COUNT(*)          as segment_count
	FROM    sys.[schemas] sm
	JOIN    sys.[tables] tb                   ON  sm.[schema_id]          = tb.[schema_id]
	JOIN    sys.[pdw_table_mappings] mp       ON  tb.[object_id]          = mp.[object_id]
	JOIN    sys.[pdw_nodes_tables] nt         ON  nt.[name]               = mp.[physical_name]
	JOIN    sys.[pdw_nodes_partitions] np     ON  np.[object_id]          = nt.[object_id]
											  AND np.[pdw_node_id]        = nt.[pdw_node_id]
											  AND np.[distribution_id]    = nt.[distribution_id]
	JOIN    sys.[pdw_nodes_columns] nc        ON  np.[object_id]          = nc.[object_id]
											  AND np.[pdw_node_id]        = nc.[pdw_node_id]
											  AND np.[distribution_id]    = nc.[distribution_id]
	JOIN    sys.[pdw_nodes_column_store_segments] rg  ON  rg.[partition_id]         = np.[partition_id]
														  AND rg.[pdw_node_id]      = np.[pdw_node_id]
														  AND rg.[distribution_id]  = np.[distribution_id]
														  AND rg.[column_id]        = nc.[column_id]
	GROUP BY    sm.name
	,           tb.name
	,           nc.name
	,           nc.column_id  
) Temp
WHERE table_nm = 'FactOnlineSalesRR' ORDER BY column_id
GO

----Analyze - Number of column segment and rows each column segments of the table
SELECT distinct o.name, css.hobt_id, css.pdw_node_id, css.distribution_id, 
css.column_id, css.segment_id, css.row_count, css.on_disk_size
FROM sys.pdw_nodes_column_store_segments AS css
JOIN sys.pdw_nodes_partitions AS pnp
    ON css.partition_id = pnp.partition_id
JOIN sys.pdw_nodes_tables AS part
    ON pnp.object_id = part.object_id 
    AND pnp.pdw_node_id = part.pdw_node_id
JOIN sys.pdw_table_mappings AS TMap
    ON part.name = TMap.physical_name
JOIN sys.objects AS o
    ON TMap.object_id = o.object_id
WHERE o.name = 'FactOnlineSalesRR' AND css. column_id = 1 --AND  css.distribution_id = 31
ORDER BY css.pdw_node_id, css.distribution_id, css.column_id, css.segment_id

--Part 3 - Updatind rows to columnstore index
SELECT
	s.name AS [Schema Name]
	,t.name AS [Table Name]
	,rg.partition_number AS [Partition Number]
	,rg.total_rows AS [Total Rows]
	,CASE WHEN rg.State = 1 THEN rg.Total_rows Else 0 END AS [Rows in OPEN Row Groups]
	,CASE WHEN rg.State = 2 THEN rg.Total_Rows ELSE 0 END AS [Rows in Closed Row Groups]
	,CASE WHEN rg.State = 3 THEN rg.Total_Rows ELSE 0 END AS [Rows in COMPRESSED Row Groups]
FROM sys.pdw_nodes_column_store_row_groups rg
	JOIN sys.pdw_nodes_tables pt
	ON rg.object_id = pt.object_id
	AND rg.pdw_node_id = pt.pdw_node_id
	AND pt.distribution_id = rg.distribution_id
	JOIN sys.pdw_table_mappings tm
	ON pt.name = tm.physical_name
	INNER JOIN sys.tables t
	ON tm.object_id = t.object_id
	INNER JOIN sys.schemas s
	ON t.schema_id = s.schema_id
WHERE t.name = 'FactOnlineSalesRR'
GO
UPDATE [cso].[FactOnlineSalesRR]
SET [UpdateDate] = [UpdateDate] + 1
GO
SELECT
	s.name AS [Schema Name]
	,t.name AS [Table Name]
	,rg.partition_number AS [Partition Number]
	,rg.total_rows AS [Total Rows]
	,CASE WHEN rg.State = 1 THEN rg.Total_rows Else 0 END AS [Rows in OPEN Row Groups]
	,CASE WHEN rg.State = 2 THEN rg.Total_Rows ELSE 0 END AS [Rows in Closed Row Groups]
	,CASE WHEN rg.State = 3 THEN rg.Total_Rows ELSE 0 END AS [Rows in COMPRESSED Row Groups]
FROM sys.pdw_nodes_column_store_row_groups rg
	JOIN sys.pdw_nodes_tables pt
	ON rg.object_id = pt.object_id
	AND rg.pdw_node_id = pt.pdw_node_id
	AND pt.distribution_id = rg.distribution_id
	JOIN sys.pdw_table_mappings tm
	ON pt.name = tm.physical_name
	INNER JOIN sys.tables t
	ON tm.object_id = t.object_id
	INNER JOIN sys.schemas s
	ON t.schema_id = s.schema_id
WHERE t.name = 'FactOnlineSalesRR'
GO
----Analyze - Total rows, deleted rows, percent deleted rows per distribution
SELECT IndexMap.object_id,   
  object_name(IndexMap.object_id) AS LogicalTableName,   
  CSRowGroups.*,  
  100*(ISNULL(deleted_rows,0))/total_rows AS PercentDeletedRows,
  i.name AS LogicalIndexName, IndexMap.index_id, NI.type_desc,   
  IndexMap.physical_name AS PhyIndexNameFromIMap
FROM sys.tables AS t  
JOIN sys.indexes AS i  
    ON t.object_id = i.object_id  
JOIN sys.pdw_index_mappings AS IndexMap  
    ON i.object_id = IndexMap.object_id  
    AND i.index_id = IndexMap.index_id  
JOIN sys.pdw_nodes_indexes AS NI  
    ON IndexMap.physical_name = NI.name  
    AND IndexMap.index_id = NI.index_id  
JOIN sys.pdw_nodes_column_store_row_groups AS CSRowGroups  
    ON CSRowGroups.object_id = NI.object_id   
    AND CSRowGroups.pdw_node_id = NI.pdw_node_id  
    AND CSRowGroups.distribution_id = NI.distribution_id
    AND CSRowGroups.index_id = NI.index_id      
WHERE t.name = 'FactOnlineSalesRR'   
ORDER BY object_name(i.object_id), i.name, IndexMap.physical_name, pdw_node_id;  
GO

ALTER INDEX ALL ON cso.FactOnlineSalesRR REBUILD
GO

SELECT IndexMap.object_id,   
  object_name(IndexMap.object_id) AS LogicalTableName,   
  CSRowGroups.*,  
  100*(ISNULL(deleted_rows,0))/total_rows AS PercentDeletedRows,
  i.name AS LogicalIndexName, IndexMap.index_id, NI.type_desc,   
  IndexMap.physical_name AS PhyIndexNameFromIMap
FROM sys.tables AS t  
JOIN sys.indexes AS i  
    ON t.object_id = i.object_id  
JOIN sys.pdw_index_mappings AS IndexMap  
    ON i.object_id = IndexMap.object_id  
    AND i.index_id = IndexMap.index_id  
JOIN sys.pdw_nodes_indexes AS NI  
    ON IndexMap.physical_name = NI.name  
    AND IndexMap.index_id = NI.index_id  
JOIN sys.pdw_nodes_column_store_row_groups AS CSRowGroups  
    ON CSRowGroups.object_id = NI.object_id   
    AND CSRowGroups.pdw_node_id = NI.pdw_node_id  
    AND CSRowGroups.distribution_id = NI.distribution_id
    AND CSRowGroups.index_id = NI.index_id      
WHERE t.name = 'FactOnlineSalesRR'   
ORDER BY object_name(i.object_id), i.name, IndexMap.physical_name, pdw_node_id;  
GO