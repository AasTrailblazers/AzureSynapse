------Monitoring - Part 2
--SELECT  r.[request_id]                          AS Req_ID
--,       r.[command]                             AS Req_command
--,       r.[status]                              AS Req_Status
--,       r.[submit_time]                         AS Req_SubmitTime
--,       r.[start_time]                          AS Req_StartTime
--,       DATEDIFF(ms,[submit_time],[start_time]) AS Req_WaitDuration_ms
--,       r.[resource_class]                      AS Req_resource_class
--,       r.[importance]							AS Req_importance
--,       r.[group_name]							AS Req_group_name
--,       r.[classifier_name]						AS Req_classifier_name
--,       r.[resource_allocation_percentage]		AS Req_resource_allocation_percentage
--FROM    sys.dm_pdw_exec_requests r
--WHERE   r.[status] NOT IN ('Completed', 'Failed', 'Cancelled') AND [session_id] <> session_id();

declare @reqid as varchar(50)	;
set @reqid = 'QID27477'

SELECT     step.[operation_type]                       AS operation_type
,           step.[location_type]                        AS location_type
,           step.[step_index]                           AS step_index
,           MIN(sreq.[start_time])                      AS min_start_time
,           MAX(sreq.[end_time])                        AS max_end_Time
,           MIN(sreq.[total_elapsed_time])/1000.0       AS min_duration_sec
,           MAX(sreq.[total_elapsed_time])/1000.0       AS max_duration_sec
,           AVG(sreq.[total_elapsed_time])/1000.0       AS avg_duration_sec
,           DATEDIFF(ms ,MIN(sreq.[start_time])
                        ,MAX(sreq.[end_time]))/1000.0   AS duration_sec
,           LEFT(step.[command],50)                     AS command         
FROM        sys.dm_pdw_sql_requests  sreq
JOIN        sys.dm_pdw_request_steps step   ON  sreq.[step_index]      = step.[step_index]
                                            AND sreq.[request_id]      = step.[request_id]
WHERE       step.[request_id] = @reqid
GROUP BY    step.[operation_type]
,           step.[location_type]
,           step.[step_index]
,           step.[command]
;

select * from sys.dm_pdw_dms_external_work where request_id =   @reqid
select * from sys.dm_pdw_dms_workers where request_id =   @reqid and type = 'WRITER'