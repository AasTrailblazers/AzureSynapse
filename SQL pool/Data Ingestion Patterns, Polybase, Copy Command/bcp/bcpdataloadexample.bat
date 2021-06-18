@echo off

set server=<ServerName>
set user=<UserName>
set password=<Password>

set database=<Database> 
set schema=<Schema>

set filelocation=D:\SynapseDataLoad

set tablename=<Table>
bcp %schema%.%tablename% in %filelocation%\%tablename%.txt -S %server% -d %database% -U %user% -P %password% -q -w -t "|" -r \n
