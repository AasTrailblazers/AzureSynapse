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

--DROP EXTERNAL TABLE [asb].[FactOnlineSales];
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

--DROP TABLE [cso].[FactOnlineSales];
CREATE TABLE [cso].[FactOnlineSales]       
WITH (DISTRIBUTION = HASH([ProductKey])) --Hash Distributed
AS 
SELECT * FROM [asb].[FactOnlineSales]        
OPTION (LABEL = 'CTAS : Load [cso].[FactOnlineSales]');

*/

SELECT min(datekey), max(datekey) 
FROM [cso].[FactOnlineSales];

SELECT DISTINCT year(datekey)
FROM [cso].[FactOnlineSales];

SELECT year(datekey), count(*)
FROM [cso].[FactOnlineSales]
GROUP BY year(datekey);

--DROP TABLE [cso].[FactOnlineSales_PExample]
CREATE TABLE [cso].[FactOnlineSales_PExample] 
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
(   CLUSTERED COLUMNSTORE INDEX
,   DISTRIBUTION = HASH([ProductKey])
,   PARTITION
    (
        [DateKey] RANGE RIGHT FOR VALUES
        (
            '2007-01-01 00:00:00.000','2008-01-01 00:00:00.000'
        ,   '2009-01-01 00:00:00.000','2010-01-01 00:00:00.000'
        )
    )
);

--DROP TABLE [cso].[FactOnlineSales_Partitioned]
CREATE TABLE [cso].[FactOnlineSales_Partitioned]
WITH
(   CLUSTERED COLUMNSTORE INDEX
,   DISTRIBUTION = HASH([ProductKey])
,   PARTITION
    (
        [DateKey] RANGE RIGHT FOR VALUES
        (
        	'2007-01-01 00:00:00.000','2008-01-01 00:00:00.000'
		,	'2009-01-01 00:00:00.000','2010-01-01 00:00:00.000'
        )
    )
)
AS
SELECT * FROM [cso].[FactOnlineSales];

--UPDATE statistics [cso].[FactOnlineSales_Partitioned]

SELECT year(datekey), count(*)
FROM [cso].[FactOnlineSales_Partitioned]
GROUP BY year(datekey)
ORDER BY year(datekey);

SELECT  pnp.partition_number, sum(nps.[row_count]) AS Row_Count
FROM
   sys.tables t
INNER JOIN sys.indexes i
    ON  t.[object_id] = i.[object_id]
    AND i.[index_id] <= 1 /* HEAP = 0, CLUSTERED or CLUSTERED_COLUMNSTORE =1 */
INNER JOIN sys.pdw_table_mappings tm
    ON t.[object_id] = tm.[object_id]
INNER JOIN sys.pdw_nodes_tables nt
    ON tm.[physical_name] = nt.[name]
INNER JOIN sys.pdw_nodes_partitions pnp 
    ON nt.[object_id]=pnp.[object_id] 
    AND nt.[pdw_node_id]=pnp.[pdw_node_id] 
    AND nt.[distribution_id] = pnp.[distribution_id]
INNER JOIN sys.dm_pdw_nodes_db_partition_stats nps
    ON nt.[object_id] = nps.[object_id]
    AND nt.[pdw_node_id] = nps.[pdw_node_id]
    AND nt.[distribution_id] = nps.[distribution_id]
    AND pnp.[partition_id]=nps.[partition_id]
WHERE t.name='FactOnlineSales_Partitioned'
GROUP BY pnp.partition_number;

--Data deletion or archival - Partition Switch Out
--DROP TABLE [cso].[FactOnlineSales_out]
CREATE TABLE [cso].[FactOnlineSales_out]
WITH 
(   DISTRIBUTION=HASH ([ProductKey])
,   CLUSTERED COLUMNSTORE INDEX
,   PARTITION ([DateKey] 
    RANGE RIGHT FOR VALUES 	
	(
		'2007-01-01 00:00:00.000'
    ))
)
AS 
SELECT * FROM [cso].[FactOnlineSales_Partitioned] WHERE 1=2;

--Data deletion or archival - Partition Switch Out
SELECT  pnp.partition_number, sum(nps.[row_count]) AS Row_Count
FROM
   sys.tables t
INNER JOIN sys.indexes i
    ON  t.[object_id] = i.[object_id]
    AND i.[index_id] <= 1 /* HEAP = 0, CLUSTERED or CLUSTERED_COLUMNSTORE =1 */
INNER JOIN sys.pdw_table_mappings tm
    ON t.[object_id] = tm.[object_id]
INNER JOIN sys.pdw_nodes_tables nt
    ON tm.[physical_name] = nt.[name]
INNER JOIN sys.pdw_nodes_partitions pnp 
    ON nt.[object_id]=pnp.[object_id] 
    AND nt.[pdw_node_id]=pnp.[pdw_node_id] 
    AND nt.[distribution_id] = pnp.[distribution_id]
INNER JOIN sys.dm_pdw_nodes_db_partition_stats nps
    ON nt.[object_id] = nps.[object_id]
    AND nt.[pdw_node_id] = nps.[pdw_node_id]
    AND nt.[distribution_id] = nps.[distribution_id]
    AND pnp.[partition_id]=nps.[partition_id]
WHERE t.name='FactOnlineSales_out'
GROUP BY pnp.partition_number;

ALTER TABLE [cso].[FactOnlineSales_Partitioned] 
SWITCH PARTITION 2 
TO [cso].[FactOnlineSales_out] PARTITION 2;

SELECT min(datekey), max(datekey) 
FROM [cso].[FactOnlineSales_Partitioned];

SELECT  min(datekey), max(datekey) 
FROM [cso].[FactOnlineSales_out];

--Validate row count for both main and archive tables

DROP TABLE [cso].[FactOnlineSales_out];

--Partition Split - create new partition
/*
https://docs.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-partition#how-to-split-a-partition-that-contains-data
A clustered columnstore table, the table partition must be 
empty before it can be split. 
1. The most efficient method to split a partition that already contains data is to use a CTAS statement. 
2. Consider disabling the columnstore index before issuing the ALTER PARTITION statement, 
   then rebuilding the columnstore index after ALTER PARTITION is complete.
3. Use CTAS to create a new table to hold the data (to empty the partition) and the split 
   and then finally switch data back in.
*/
ALTER TABLE [cso].[FactOnlineSales_Partitioned] 
SPLIT RANGE ('2011-01-01 00:00:00.000');

SELECT  pnp.partition_number, sum(nps.[row_count]) AS Row_Count
FROM
   sys.tables t
INNER JOIN sys.indexes i
    ON  t.[object_id] = i.[object_id]
    AND i.[index_id] <= 1 /* HEAP = 0, CLUSTERED or CLUSTERED_COLUMNSTORE =1 */
INNER JOIN sys.pdw_table_mappings tm
    ON t.[object_id] = tm.[object_id]
INNER JOIN sys.pdw_nodes_tables nt
    ON tm.[physical_name] = nt.[name]
INNER JOIN sys.pdw_nodes_partitions pnp 
    ON nt.[object_id]=pnp.[object_id] 
    AND nt.[pdw_node_id]=pnp.[pdw_node_id] 
    AND nt.[distribution_id] = pnp.[distribution_id]
INNER JOIN sys.dm_pdw_nodes_db_partition_stats nps
    ON nt.[object_id] = nps.[object_id]
    AND nt.[pdw_node_id] = nps.[pdw_node_id]
    AND nt.[distribution_id] = nps.[distribution_id]
    AND pnp.[partition_id]=nps.[partition_id]
WHERE t.name='FactOnlineSales_Partitioned'
GROUP BY pnp.partition_number;


--Incremental data load - Partition Switch In
--ensure the table definitions match and that the partitions align 
--on their respective boundaries, i.e. the source table must contain 
--the same partition boundaries as the target table 
--Scenario 1
--DROP TABLE [cso].[FactOnlineSales_in]
CREATE TABLE [cso].[FactOnlineSales_in]
WITH
(   CLUSTERED COLUMNSTORE INDEX
,   DISTRIBUTION = HASH([ProductKey])
,   PARTITION
    (
        [DateKey] RANGE RIGHT FOR VALUES
        (
        	'2007-01-01 00:00:00.000','2008-01-01 00:00:00.000'
		,	'2009-01-01 00:00:00.000','2010-01-01 00:00:00.000'
		,	'2011-01-01 00:00:00.000'
        )
    )
)
AS
SELECT [OnlineSalesKey]
	  ,ISNULL(DATEADD(year, 1, [DateKey]), GETDATE()) AS [DateKey]
      ,[StoreKey],[ProductKey],[PromotionKey],[CurrencyKey],[CustomerKey]
      ,[SalesOrderNumber],[SalesOrderLineNumber],[SalesQuantity]
      ,[SalesAmount],[ReturnQuantity],[ReturnAmount],[DiscountQuantity]
      ,[DiscountAmount],[TotalCost],[UnitCost],[UnitPrice]
      ,[ETLLoadID],[LoadDate],[UpdateDate]
FROM   [cso].[FactOnlineSales] stg
WHERE stg.[DateKey] >= '2009-01-01 00:00:00.000'
AND   stg.[DateKey] <  '2010-01-01 00:00:00.000'

SELECT  min(datekey), max(datekey) 
FROM [cso].[FactOnlineSales_in];

SELECT  pnp.partition_number, sum(nps.[row_count]) AS Row_Count
FROM
   sys.tables t
INNER JOIN sys.indexes i
    ON  t.[object_id] = i.[object_id]
    AND i.[index_id] <= 1 /* HEAP = 0, CLUSTERED or CLUSTERED_COLUMNSTORE =1 */
INNER JOIN sys.pdw_table_mappings tm
    ON t.[object_id] = tm.[object_id]
INNER JOIN sys.pdw_nodes_tables nt
    ON tm.[physical_name] = nt.[name]
INNER JOIN sys.pdw_nodes_partitions pnp 
    ON nt.[object_id]=pnp.[object_id] 
    AND nt.[pdw_node_id]=pnp.[pdw_node_id] 
    AND nt.[distribution_id] = pnp.[distribution_id]
INNER JOIN sys.dm_pdw_nodes_db_partition_stats nps
    ON nt.[object_id] = nps.[object_id]
    AND nt.[pdw_node_id] = nps.[pdw_node_id]
    AND nt.[distribution_id] = nps.[distribution_id]
    AND pnp.[partition_id]=nps.[partition_id]
WHERE t.name='FactOnlineSales_in'
GROUP BY pnp.partition_number;

ALTER TABLE [cso].[FactOnlineSales_in] 
SWITCH PARTITION 5 
TO [cso].[FactOnlineSales_Partitioned] PARTITION 5;

SELECT  pnp.partition_number, sum(nps.[row_count]) AS Row_Count
FROM
   sys.tables t
INNER JOIN sys.indexes i
    ON  t.[object_id] = i.[object_id]
    AND i.[index_id] <= 1 /* HEAP = 0, CLUSTERED or CLUSTERED_COLUMNSTORE =1 */
INNER JOIN sys.pdw_table_mappings tm
    ON t.[object_id] = tm.[object_id]
INNER JOIN sys.pdw_nodes_tables nt
    ON tm.[physical_name] = nt.[name]
INNER JOIN sys.pdw_nodes_partitions pnp 
    ON nt.[object_id]=pnp.[object_id] 
    AND nt.[pdw_node_id]=pnp.[pdw_node_id] 
    AND nt.[distribution_id] = pnp.[distribution_id]
INNER JOIN sys.dm_pdw_nodes_db_partition_stats nps
    ON nt.[object_id] = nps.[object_id]
    AND nt.[pdw_node_id] = nps.[pdw_node_id]
    AND nt.[distribution_id] = nps.[distribution_id]
    AND pnp.[partition_id]=nps.[partition_id]
WHERE t.name='FactOnlineSales_Partitioned'
GROUP BY pnp.partition_number;

--Scenario 2
CREATE TABLE [cso].[FactOnlineSales_in2]
WITH 
(   DISTRIBUTION=HASH ([ProductKey])
,   CLUSTERED COLUMNSTORE INDEX
PARTITION
    (
        [DateKey] RANGE RIGHT FOR VALUES
        (
        	'2007-01-01 00:00:00.000','2008-01-01 00:00:00.000'
		,	'2009-01-01 00:00:00.000','2010-01-01 00:00:00.000'
		,	'2011-01-01 00:00:00.000'
        )
    )
)
AS
SELECT [OnlineSalesKey]
	  ,[DateKey] AS [DateKey]
      ,[StoreKey],[ProductKey],[PromotionKey],[CurrencyKey],[CustomerKey]
      ,[SalesOrderNumber],[SalesOrderLineNumber],[SalesQuantity]
      ,[SalesAmount],[ReturnQuantity],[ReturnAmount],[DiscountQuantity]
      ,[DiscountAmount],[TotalCost],[UnitCost],[UnitPrice]
      ,[ETLLoadID],[LoadDate],[UpdateDate]
FROM   [cso].[FactOnlineSales] stg
WHERE stg.[DateKey] >= '2010-05-01 00:00:00.000'
AND   stg.[DateKey] <  '2010-05-31 00:00:00.000'
UNION ALL
SELECT *
FROM   [cso].[FactOnlineSales_Partitioned] tgt
WHERE tgt.[DateKey] >= '2010-01-01 00:00:00.000'
AND   tgt.[DateKey] <  '2010-04-30 00:00:00.000'


--Before partitions are created, dedicated SQL pool already divides 
--each table into 60 distributed databases. For optimal compression and 
--performance of clustered columnstore tables, 1 million rows 
--per distribution and partition is recommended. 
--Distribution - partitions - rowcounts
SELECT  t.name, nt.distribution_id, 
	pnp.partition_number, nps.[row_count]
FROM
   sys.tables t
INNER JOIN sys.indexes i
    ON  t.[object_id] = i.[object_id]
    AND i.[index_id] <= 1 /* HEAP = 0, CLUSTERED or CLUSTERED_COLUMNSTORE =1 */
INNER JOIN sys.pdw_table_mappings tm
    ON t.[object_id] = tm.[object_id]
INNER JOIN sys.pdw_nodes_tables nt
    ON tm.[physical_name] = nt.[name]
INNER JOIN sys.pdw_nodes_partitions pnp 
    ON nt.[object_id]=pnp.[object_id] 
    AND nt.[pdw_node_id]=pnp.[pdw_node_id] 
    AND nt.[distribution_id] = pnp.[distribution_id]
INNER JOIN sys.dm_pdw_nodes_db_partition_stats nps
    ON nt.[object_id] = nps.[object_id]
    AND nt.[pdw_node_id] = nps.[pdw_node_id]
    AND nt.[distribution_id] = nps.[distribution_id]
    AND pnp.[partition_id]=nps.[partition_id]
WHERE t.name='FactOnlineSales_Partitioned'
ORDER BY nt.distribution_id;