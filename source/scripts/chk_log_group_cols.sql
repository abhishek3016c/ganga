col owner for a10
col log_group_name for a20
col table_name for a10
col column_name for a15

select * from dba_log_groups
 where owner = 'TESTSRC'
order by table_name 
/

select * from dba_log_group_columns
 where owner = 'TESTSRC'
order by table_name, position
/
