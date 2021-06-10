--Run it in master database
CREATE LOGIN synapseadminlargerc WITH PASSWORD = '********************';
CREATE LOGIN userloginA WITH PASSWORD = '********************';
CREATE LOGIN userloginCEO WITH PASSWORD = '********************';

--Run it in user or SQL pool database
CREATE USER synapseadminlargerc for LOGIN synapseadminlargerc;
GRANT CONTROL ON DATABASE::LearnSynapseDB TO synapseadminlargerc;

CREATE USER userloginA for LOGIN userloginA;
GRANT CONTROL ON DATABASE::LearnSynapseDB TO userloginA;
CREATE USER userloginCEO for LOGIN userloginCEO;
GRANT CONTROL ON DATABASE::LearnSynapseDB TO userloginCEO;

EXEC sp_addrolemember 'largerc','synapseadminlargerc';
EXEC sp_droprolemember 'largerc', 'synapseadminlargerc';

SELECT  r.[name] AS role_principal_name
,       m.[name] AS member_principal_name
FROM    sys.database_role_members rm
JOIN    sys.database_principals AS r    ON rm.[role_principal_id]     = r.[principal_id]
JOIN    sys.database_principals AS m    ON rm.[member_principal_id]   = m.[principal_id]
WHERE   r.[name] IN ('smallrc','mediumrc','largerc', 'xlargerc');

SELECT  r.[request_id]                          AS Req_ID
,       r.[command]                             AS Req_command
,       r.[status]                              AS Req_Status
,       r.[submit_time]                         AS Req_SubmitTime
,       r.[start_time]                          AS Req_StartTime
,       DATEDIFF(ms,[submit_time],[start_time]) AS Req_WaitDuration_ms
,       r.[resource_class]                      AS Req_resource_class
,       r.[importance]							AS Req_importance
,       r.[group_name]							AS Req_group_name
,       r.[classifier_name]						AS Req_classifier_name
,       r.[resource_allocation_percentage]		AS Req_resource_allocation_percentage
FROM    sys.dm_pdw_exec_requests r
WHERE   r.[status] NOT IN ('Completed', 'Failed', 'Cancelled') AND [session_id] <> session_id();

-----------------------------------------------------------------------
--DROP WORKLOAD GROUP wgDataLoad 
CREATE WORKLOAD GROUP wgDataLoad 
WITH
  ( -- integer value range from 0 to 100
	MIN_PERCENTAGE_RESOURCE = 30                
	-- Specifies the maximum resource utilization for all requests in a workload group: range from 1 to 100
    , CAP_PERCENTAGE_RESOURCE = 100
	-- factor of 30 (guaranteed a minimum of 6 concurrency)
    , REQUEST_MIN_RESOURCE_GRANT_PERCENT = 5    
	-- when system resources are available, additional resources are allocated to a request.
	, REQUEST_MAX_RESOURCE_GRANT_PERCENT = 10
	-- Importance set at the workload group is a default importance for all requests in the workload group.
	, IMPORTANCE = NORMAL 						-- LOW | BELOW_NORMAL | NORMAL | ABOVE_NORMAL | HIGH -- Importance set at the workload group is a default importance for all requests in the workload group.
	-- maximum time, in seconds, that a query can execute before it is canceled.
	, QUERY_EXECUTION_TIMEOUT_SEC = 0 )

--DROP WORKLOAD GROUP wgDataQuery 
CREATE WORKLOAD GROUP wgDataQuery 
WITH
  ( MIN_PERCENTAGE_RESOURCE = 50                -- integer value
    , CAP_PERCENTAGE_RESOURCE = 100
    , REQUEST_MIN_RESOURCE_GRANT_PERCENT = 5    -- factor of 50 (guaranteed a minimum of 10 concurrency)
	, REQUEST_MAX_RESOURCE_GRANT_PERCENT = 10   -- when system resources are available, additional resources are allocated to a request.
	, IMPORTANCE = NORMAL );					-- LOW | BELOW_NORMAL | NORMAL | ABOVE_NORMAL | HIGH

SELECT * FROM sys.workload_management_workload_groups order by group_id
SELECT * FROM sys.dm_workload_management_workload_groups_stats order by group_id

-----------------------------------------------------------------------
EXEC sp_droprolemember 'largerc', 'synapseadminlargerc';

--DROP WORKLOAD CLASSIFIER classifierA
CREATE WORKLOAD CLASSIFIER classifierA WITH  
( WORKLOAD_GROUP = 'wgDataLoad'  
 ,MEMBERNAME     = 'synapseadminlargerc' --can be a database user, database role, Azure Active Directory login, or Azure Active Directory group
 ,IMPORTANCE     = NORMAL)

SELECT  r.[request_id]                          AS Req_ID
,       r.[command]                             AS Req_command
,       r.[status]                              AS Req_Status
,       r.[submit_time]                         AS Req_SubmitTime
,       r.[start_time]                          AS Req_StartTime
,       DATEDIFF(ms,[submit_time],[start_time]) AS Req_WaitDuration_ms
,       r.[resource_class]                      AS Req_resource_class
,       r.[importance]							AS Req_importance
,       r.[group_name]							AS Req_group_name
,       r.[classifier_name]						AS Req_classifier_name
,       r.[resource_allocation_percentage]		AS Req_resource_allocation_percentage
FROM    sys.dm_pdw_exec_requests r
WHERE   r.[status] NOT IN ('Completed', 'Failed', 'Cancelled') AND [session_id] <> session_id();

--DROP WORKLOAD CLASSIFIER classifierA
CREATE WORKLOAD CLASSIFIER classifierA WITH  
( WORKLOAD_GROUP = 'wgDataLoad'  
 ,MEMBERNAME     = 'synapseadminlargerc' --can be a database user, database role, Azure Active Directory login, or Azure Active Directory group
 ,IMPORTANCE     = NORMAL
 ,WLM_LABEL      = 'dataloadoperation')

SELECT * FROM sys.workload_management_workload_classifiers --WHERE classifier_id > 12
SELECT * FROM sys.workload_management_workload_classifier_details WHERE classifier_id > 12

SELECT  r.[request_id]                          AS Req_ID
,       r.[command]                             AS Req_command
,       r.[status]                              AS Req_Status
,       r.[submit_time]                         AS Req_SubmitTime
,       r.[start_time]                          AS Req_StartTime
,       DATEDIFF(ms,[submit_time],[start_time]) AS Req_WaitDuration_ms
,       r.[resource_class]                      AS Req_resource_class
,       r.[importance]							AS Req_importance
,       r.[group_name]							AS Req_group_name
,       r.[classifier_name]						AS Req_classifier_name
,       r.[resource_allocation_percentage]		AS Req_resource_allocation_percentage
FROM    sys.dm_pdw_exec_requests r
WHERE   r.[status] NOT IN ('Completed', 'Failed', 'Cancelled') AND [session_id] <> session_id();

--DROP WORKLOAD CLASSIFIER classifierA
CREATE WORKLOAD CLASSIFIER classifierA WITH  
( WORKLOAD_GROUP = 'wgDataLoad'  
 ,MEMBERNAME     = 'synapseadminlargerc' --can be a database user, database role, Azure Active Directory login, or Azure Active Directory group
 ,IMPORTANCE     = NORMAL
 ,WLM_CONTEXT    = 'dataloadoperation')

SELECT * FROM sys.workload_management_workload_classifiers --WHERE classifier_id > 12
SELECT * FROM sys.workload_management_workload_classifier_details WHERE classifier_id > 12

SELECT  r.[request_id]                          AS Req_ID
,       r.[command]                             AS Req_command
,       r.[status]                              AS Req_Status
,       r.[submit_time]                         AS Req_SubmitTime
,       r.[start_time]                          AS Req_StartTime
,       DATEDIFF(ms,[submit_time],[start_time]) AS Req_WaitDuration_ms
,       r.[resource_class]                      AS Req_resource_class
,       r.[importance]							AS Req_importance
,       r.[group_name]							AS Req_group_name
,       r.[classifier_name]						AS Req_classifier_name
,       r.[resource_allocation_percentage]		AS Req_resource_allocation_percentage
FROM    sys.dm_pdw_exec_requests r
WHERE   r.[status] NOT IN ('Completed', 'Failed', 'Cancelled') AND [session_id] <> session_id();

--DROP WORKLOAD CLASSIFIER classifierA
CREATE WORKLOAD CLASSIFIER classifierA WITH  
( WORKLOAD_GROUP = 'wgDataLoad'  
 ,MEMBERNAME     = 'synapseadminlargerc' --can be a database user, database role, Azure Active Directory login, or Azure Active Directory group
 ,IMPORTANCE     = NORMAL
 ,WLM_LABEL      = 'dataloadoperation'
 ,WLM_CONTEXT    = 'dataloadoperation'
 ,START_TIME     = '18:00'
 ,END_TIME       = '07:00'
 )

SELECT * FROM sys.workload_management_workload_classifiers --WHERE classifier_id > 12
SELECT * FROM sys.workload_management_workload_classifier_details WHERE classifier_id > 12

--DROP WORKLOAD CLASSIFIER classifierB
CREATE WORKLOAD CLASSIFIER classifierB WITH  
( WORKLOAD_GROUP = 'wgDataQuery'  
 ,MEMBERNAME     = 'userloginA'
 ,IMPORTANCE     = LOW)

--DROP WORKLOAD CLASSIFIER classifierC
 CREATE WORKLOAD CLASSIFIER classifierC WITH  
( WORKLOAD_GROUP = 'wgDataQuery'  
 ,MEMBERNAME     = 'userloginCEO'
 ,IMPORTANCE     = HIGH)

 ---------------------------------------------------------------
 CREATE WORKLOAD CLASSIFIER classifierA WITH  
( WORKLOAD_GROUP = 'wgDataQuery'  
 ,MEMBERNAME     = 'userloginA'
 ,IMPORTANCE     = HIGH -- LOW | BELOW_NORMAL | NORMAL | ABOVE_NORMAL | HIGH
 ,WLM_LABEL      = 'dataqueryoperation' )

CREATE WORKLOAD CLASSIFIER classifierB WITH  
( WORKLOAD_GROUP = 'wgDataQuery'  
 ,MEMBERNAME     = 'userloginA'
 ,IMPORTANCE     = LOW -- LOW | BELOW_NORMAL | NORMAL | ABOVE_NORMAL | HIGH
 ,START_TIME     = '18:00'
 ,END_TIME       = '07:00')

/*
https://docs.microsoft.com/en-us/sql/t-sql/statements/create-workload-classifier-transact-sql?toc=%2Fazure%2Fsynapse-analytics%2Fsql-data-warehouse%2Ftoc.json&bc=%2Fazure%2Fsynapse-analytics%2Fsql-data-warehouse%2Fbreadcrumb%2Ftoc.json&view=azure-sqldw-latest&preserve-view=true#classification-parameter-weighting

Classifier Parameter	Weight
USER					64
ROLE					32
WLM_LABEL				16
WLM_CONTEXT				8
START_TIME/END_TIME		4
*/
