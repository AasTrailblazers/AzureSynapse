-------------------bcp example-----------------------
--DROP TABLE [cso].[DimDate]
CREATE TABLE [cso].[DimDate](
	DateKey int NOT NULL,
	FullDateAlternateKey date NOT NULL, 
	DayNumberOfWeek tinyint NOT NULL,
	EnglishDayNameOfWeek nvarchar(10) NOT NULL,
	SpanishDayNameOfWeek nvarchar(10) NOT NULL,
	FrenchDayNameOfWeek nvarchar(10) NOT NULL,
	DayNumberOfMonth tinyint NOT NULL,
	DayNumberOfYear smallint NOT NULL,
	WeekNumberOfYear tinyint NOT NULL,
	EnglishMonthName nvarchar(10) NOT NULL,
	SpanishMonthName nvarchar(10) NOT NULL,
	FrenchMonthName nvarchar(10) NOT NULL,
	MonthNumberOfYear tinyint NOT NULL,
	CalendarQuarter tinyint NOT NULL,
	CalendarYear smallint NOT NULL,
	CalendarSemester tinyint NOT NULL,
	FiscalQuarter tinyint NOT NULL,
	FiscalYear smallint NOT NULL,
	FiscalSemester tinyint NOT NULL)
WITH (CLUSTERED INDEX(DateKey), DISTRIBUTION = REPLICATE);

SELECT * FROM [cso].[DimDate]

-------------------Polybase-----------------------

--https://docs.microsoft.com/en-us/sql/t-sql/statements/create-external-file-format-transact-sql?view=azure-sqldw-latest&tabs=delimited
--IF (EXISTS(SELECT * FROM sys.external_file_formats WHERE name = 'TextFileFormatContoso')) BEGIN
--    DROP EXTERNAL FILE FORMAT TextFileFormatContoso
--END
CREATE EXTERNAL FILE FORMAT TextFileFormatContoso 
WITH 
(   FORMAT_TYPE = DELIMITEDTEXT  -- PARQUET | ORC | RCFILE
,	FORMAT_OPTIONS	(   FIELD_TERMINATOR = '|'
					,	STRING_DELIMITER = ''
					,	DATE_FORMAT		 = 'yyyy-MM-dd HH:mm:ss.fff'
					,	FIRST_ROW = 1
					--,	DATA COMPRESSION = 'org.apache.hadoop.io.compress.DefaultCodec'
					-- tradeoff between transferring less data and increased CPU cycle needed to compress and decompress the data.
					-- The ideal number of compressed files is the maximum number of data reader processes per compute node.
					)
 )
GO

-- Create an external data source
-- TYPE: HADOOP - PolyBase uses Hadoop APIs to access data in Azure blob storage.
-- LOCATION: Provide Azure storage account name and blob container name.
--IF (EXISTS(SELECT * FROM sys.external_data_sources WHERE name = 'AzureStorage_west_public')) BEGIN
--    DROP EXTERNAL DATA SOURCE AzureStorage_west_public
--END;
CREATE EXTERNAL DATA SOURCE AzureStorage_west_public
WITH 
(  
	TYPE = Hadoop 
,   LOCATION = 'wasbs://contosoretaildw-tables@contosoretaildw.blob.core.windows.net/'
); 

-- Create a master key. Only necessary if one does not already exist in the database.
-- Required to encrypt the credential secret 
CREATE MASTER KEY; -- BY PASSWORD='<EnterStrongPasswordHere>';

-- Create a database scoped credential
-- IDENTITY: Provide any string, it is not used for authentication to Azure storage.
-- SECRET: Provide your Azure storage account key.
--IF EXISTS(SELECT * FROM sys.database_scoped_credentials WHERE name = 'ASCredentialKey')
--   DROP DATABASE SCOPED CREDENTIAL ASCredentialKey
--END;
CREATE DATABASE SCOPED CREDENTIAL ASCredentialKey
WITH
    IDENTITY = 'storagename',
    SECRET = '************************************************************************'
;

/* 
	https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/develop-tables-external-tables?tabs=hadoop
*/
--DROP DATABASE SCOPED CREDENTIAL ASCredentialSAS
--CREATE DATABASE SCOPED CREDENTIAL ASCredentialSAS
--WITH
--    IDENTITY = 'SHARED ACCESS SIGNATURE',
--    SECRET = '************************************************************************'
--;

-- Create credential that will allow user to impersonate using Managed Identity assigned to workspace
--DROP DATABASE SCOPED CREDENTIAL ASCredentialWorkspaceIdentity 
CREATE DATABASE SCOPED CREDENTIAL ASCredentialWorkspaceIdentity 
	WITH IDENTITY = 'Managed Identity'
GO

--DROP EXTERNAL DATA SOURCE AzureDataLake_secured
CREATE EXTERNAL DATA SOURCE AzureDataLake_secured
WITH 
(  
	TYPE = Hadoop 
--,   LOCATION = 'wasbs://containername@storagename.blob.core.windows.net/'
,   LOCATION = 'abfss://containername@storagename.dfs.core.windows.net'
-- absence of credential will work with public or use the caller's Azure AD identity to access files on storage
,	CREDENTIAL = ASCredentialWorkspaceIdentity 
); 

--CREATE SCHEMA [asb]
--GO
--CREATE SCHEMA [cso]
--GO
--CREATE SCHEMA [stg]
--GO

--DROP EXTERNAL TABLE [asb].[DimProduct]
CREATE EXTERNAL TABLE [asb].[DimProduct] (
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
    -- retrieve files from the folder and all of its subfolders
	-- doesn't include hidden files/folders and file name begins with an underline (_) or a period (.)
	LOCATION='/DimProduct/' 
,   DATA_SOURCE = AzureDataLake_secured
,   FILE_FORMAT = TextFileFormatContoso
-- 'dirty' record - if it actual data types or the number of columns don't match the column definitions
-- writes both data file and reason file
,   REJECT_TYPE = VALUE -- VALUE (int) or PERCENTAGE (float between 0 and 100)
,   REJECT_VALUE = 10
,	REJECTED_ROW_LOCATION = '/REJECT_Directory/DimProduct/'
);

--SELECT * FROM [asb].[DimProduct]

select * from sys.tables where is_external = 1
select * from sys.external_tables 

--DROP TABLE [stg].[DimProduct]  
CREATE TABLE [stg].[DimProduct]            
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN) 
AS 
SELECT * FROM [asb].[DimProduct] 
OPTION (LABEL = 'CTAS : Load [stg].[DimProduct]');
GO

CREATE TABLE [cso].[DimProduct]
WITH (CLUSTERED COLUMNSTORE INDEX,
		DISTRIBUTION = ROUND_ROBIN)
AS 
SELECT * FROM [stg].[DimProduct] 
GO

--DROP EXTERNAL TABLE [asb].[DimProduct_2]  
CREATE EXTERNAL TABLE [asb].[DimProduct_2]            
WITH (
        LOCATION = '/DimProduct_2/',  
        DATA_SOURCE = AzureDataLake_secured,  
        FILE_FORMAT = TextFileFormatContoso  
)
AS 
SELECT * FROM [stg].[DimProduct] 
OPTION (LABEL = 'CETAS : Export [stg].[DimProduct]');
GO

-------------------Copy Command-----------------------
-- Without needing strict CONTROL permissions
-- Without having to create any additional database objects
-- Without exposing storage account keys using Share Access Signatures (SAS)
-- Use a different storage account for the ERRORFILE location (REJECTED_ROW_LOCATION)
-- Customize default values for each target column
-- Specify source data fields to load into specific target columns
-- Supports multiple locations from the same storage account, separated by comma

--DROP TABLE [stg].[DimProduct]
CREATE TABLE [stg].[DimProduct]
(
	[ProductKey] [int] NULL,
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
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO

--Authenticating with Storage account key
COPY INTO [stg].[DimProduct]
FROM 'https://storagename.blob.core.windows.net/containername/DimProduct/*.txt'
WITH (
    FILE_TYPE = 'CSV',
	FIELDTERMINATOR = '|',
	FIELDQUOTE = '',
	FIRSTROW = 1,
    CREDENTIAL = (IDENTITY= 'Storage Account Key', SECRET='************************************'),
	ERRORFILE = 'https://storagename.blob.core.windows.net/containername/REJECT_Directory/DimProduct/' 
)
--Authenticating with Shared Access Signature
COPY INTO [stg].[DimProduct]
FROM 'https://storagename.blob.core.windows.net/containername/DimProduct/*.txt'
WITH (
    FILE_TYPE = 'CSV',
	FIELDTERMINATOR = '|',
	FIELDQUOTE = '',
	FIRSTROW = 1,
    CREDENTIAL = (IDENTITY= 'Shared Access Signature', SECRET='********************'),
	ERRORFILE = 'https://storagename.blob.core.windows.net/containername/REJECT_Directory/DimProduct/' 
)
--Authenticating with Managed Identity
COPY INTO [stg].[DimProduct]
FROM 'https://storagename.blob.core.windows.net/containername/DimProduct/*.txt'
WITH (
    FILE_TYPE = 'CSV',
	FIELDTERMINATOR = '|',
	FIELDQUOTE = '',
	FIRSTROW = 1,
    CREDENTIAL = (IDENTITY = 'Managed Identity'),
	ERRORFILE = 'https://storagename.blob.core.windows.net/containername/REJECT_Directory/DimProduct/' 
)
--Authenticating with an AAD user
COPY INTO [stg].[DimProduct]
FROM 'https://storagename.blob.core.windows.net/containername/DimProduct/*.txt' 
WITH (
    FILE_TYPE = 'CSV',
	FIELDTERMINATOR = '|',
	FIELDQUOTE = '',
	FIRSTROW = 1,
	ERRORFILE = 'https://storagename.blob.core.windows.net/containername/REJECT_Directory/DimProduct/' 
)

SELECT * FROM [stg].[DimProduct] WHERE [WeightUnitMeasureID] IS NULL

--When a column list is not specified, COPY will map columns based on the source 
--and target ordinality: Input field 1 will go to target column 1, field 2 will go to column 2, etc.
--Note when specifying the column list, input field numbers start from 1
COPY INTO [stg].[DimProduct]
(
	   [ProductKey] 1
      ,[ProductLabel] 2
      ,[ProductName] 3
      ,[ProductDescription] 4
      ,[ProductSubcategoryKey] 5
      ,[Manufacturer] 6
      ,[BrandName] 7
      ,[ClassID] 8
      ,[ClassName] 9
      ,[StyleID] 10
      ,[StyleName] 11
      ,[ColorID] 12
      ,[ColorName] 13
      ,[Size] 14
      ,[SizeRange] 15
      ,[SizeUnitMeasureID] 16
      ,[Weight] 17
      ,[WeightUnitMeasureID] DEFAULT 'ounces' 18
      ,[UnitOfMeasureID] 19
      ,[UnitOfMeasureName] 20
      ,[StockTypeID] 21
      ,[StockTypeName] 22
      ,[UnitCost] 23
      ,[UnitPrice] 24
      ,[AvailableForSaleDate] 25
      ,[StopSaleDate] 26
      ,[Status] 27
      ,[ImageURL] 28
      ,[ProductURL] 29
      ,[ETLLoadID] 30
      ,[LoadDate] 31
      ,[UpdateDate] 32
)
FROM 'https://storagename.blob.core.windows.net/containername/DimProduct/*.txt'
WITH (
    FILE_TYPE = 'CSV',
	FIELDTERMINATOR = '|',
	FIELDQUOTE = '',
	ERRORFILE = 'https://storagename.blob.core.windows.net/containername/REJECT_Directory/DimProduct/' 
)

SELECT * FROM [stg].[DimProduct] WHERE [WeightUnitMeasureID] IS NULL


-------------------Incremental Data Refresh-----------------------
CREATE TABLE [cso].[DimProduct_refreshed]
WITH (CLUSTERED COLUMNSTORE INDEX,
		DISTRIBUTION = ROUND_ROBIN)
AS
SELECT * --Existing Rows
FROM
(    SELECT  *
     FROM    [cso].[DimProduct]
) a
UNION ALL
SELECT * --NewRows
FROM
(    SELECT  *
     FROM    [stg].[DimProduct]
) b
;

CREATE TABLE [cso].[DimProduct_refreshed]
WITH (CLUSTERED COLUMNSTORE INDEX,
		DISTRIBUTION = ROUND_ROBIN)
AS -- New rows and new versions of rows
	SELECT      *
	FROM        [stg].[DimProduct] s
UNION ALL --Keep rows that are not being updated
	SELECT     *
	FROM        [cso].[DimProduct] p
	WHERE NOT EXISTS
	(   SELECT *
		FROM   [src].[DimProduct] s
		WHERE  s.[ProductKey] = p.[ProductKey]
	)
;

RENAME OBJECT [cso].[DimProduct] TO [cso].[DimProduct_old];
RENAME OBJECT [cso].[DimProduct_refreshed] TO [cso].[DimProduct];

---Monitoring - Part 1
--DROP EXTERNAL TABLE [asb].[FactOnlineSales]
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
,   DATA_SOURCE = AzureDataLake_secured
,   FILE_FORMAT = TextFileFormatContoso
,   REJECT_TYPE = VALUE
,   REJECT_VALUE = 0
,	REJECTED_ROW_LOCATION = '/REJECT_Directory/FactOnlineSales/'
);

--DROP TABLE [stg].[FactOnlineSales]  
CREATE TABLE [stg].[FactOnlineSales]           
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN) 
AS 
SELECT * FROM [asb].[FactOnlineSales]
OPTION (LABEL = 'CTAS : Load [asb].[FactOnlineSales]');
GO

--DROP TABLE [cso].[FactOnlineSales]  
CREATE TABLE [cso].[FactOnlineSales]           
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([ProductKey])) 
AS 
SELECT * FROM [asb].[FactOnlineSales]
OPTION (LABEL = 'CTAS : Load [stg].[FactOnlineSales]');
GO
