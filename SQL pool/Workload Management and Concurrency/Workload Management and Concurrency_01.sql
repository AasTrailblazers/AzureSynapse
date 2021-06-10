--DROP TABLE [cso].[FactOnlineSales];
CREATE TABLE [cso].[FactOnlineSales]       
WITH (DISTRIBUTION = HASH([ProductKey])) --Hash Distributed
AS 
SELECT * FROM [asb].[FactOnlineSales]
UNION ALL
SELECT * FROM [asb].[FactOnlineSales]
OPTION (LABEL = 'CTAS : Load [cso].[FactOnlineSales]');

--DROP TABLE [cso].[FactOnlineSales];
CREATE TABLE [cso].[FactOnlineSales]       
WITH (DISTRIBUTION = HASH([ProductKey])) --Hash Distributed
AS 
SELECT * FROM [asb].[FactOnlineSales]
UNION ALL
SELECT * FROM [asb].[FactOnlineSales]

--DROP TABLE [cso].[FactOnlineSales];
CREATE TABLE [cso].[FactOnlineSales]       
WITH (DISTRIBUTION = HASH([ProductKey])) --Hash Distributed
AS 
SELECT * FROM [asb].[FactOnlineSales]
UNION ALL
SELECT * FROM [asb].[FactOnlineSales]
OPTION (LABEL = 'dataloadoperation')

--set session context
EXEC sys.sp_set_session_context @key = 'wlm_context', @value = 'dataloadoperation'

--DROP TABLE [cso].[FactOnlineSales];
CREATE TABLE [cso].[FactOnlineSales]       
WITH (DISTRIBUTION = HASH([ProductKey])) --Hash Distributed
AS 
SELECT * FROM [asb].[FactOnlineSales]
UNION ALL
SELECT * FROM [asb].[FactOnlineSales]

--turn off the wlm_context session setting
EXEC sys.sp_set_session_context @key = 'wlm_context', @value = null