Rem
Rem Copyright (c) 2008, 2012, Oracle and/or its affiliates. 
Rem All rights reserved. 
Rem
Rem    NAME
Rem      xiodemo.sql - Set up XStream Out to In Demo
Rem
Rem    DESCRIPTION
Rem      This script creates a Streams administrator user, 'stradm', at both
Rem      inst1 and inst2. Then it creates an XStream outbound server, named 
Rem      'XOUT', at inst1 and an inbound server, named 'XIN', at inst2.
Rem

COLUMN APPLY_USER format a30
COLUMN CONNECT_USER format a30
COLUMN CAPTURE_USER format a30
SET ECHO ON
SET FEEDBACK 1
SET NUMWIDTH 10
SET LINESIZE 80
SET TRIMSPOOL ON
SET TAB OFF
SET PAGESIZE 100

define inst1_sys_passwd=&1
define inst2_sys_passwd=&2

-------------------------------------------------------
-- (1) create stradm user, grant priv
-------------------------------------------------------
connect sys/&inst2_sys_passwd@inst2 as sysdba
drop user stradm cascade;
alter session set "_ORACLE_SCRIPT"=true;
create user stradm identified by stradm;

grant resource, unlimited tablespace, connect to stradm;
grant dba to stradm;
exec dbms_xstream_auth.grant_admin_privilege(grantee => 'stradm');

connect sys/&inst1_sys_passwd@inst1 as sysdba
drop user stradm cascade;
grant resource, unlimited tablespace, connect to stradm identified by stradm;
grant dba to stradm;
exec dbms_xstream_auth.grant_admin_privilege(grantee => 'stradm');

-------------------------------------------------------
-- (2) Set up XStreams Out at inst1
-------------------------------------------------------
-- create outbound server at inst1
connect stradm/stradm@inst1
exec DBMS_XSTREAM_ADM.CREATE_OUTBOUND(server_name => 'DBNEXUS_OUT');
-------------------------------------------------------
-- (3) Set up XStreams In at inst2
-------------------------------------------------------
connect stradm/stradm@inst2
begin
  dbms_xstream_adm.create_inbound('XIN', 
    queue_name  => 'queue2',  
    comment      => 'XStreams In');
end;
/
select apply_name, apply_user, purpose from dba_apply order by 1;
exec dbms_apply_adm.start_apply('XIN');

-------------------------------------------------------
-- (4) Wait for capture at inst1 to be up and running
-------------------------------------------------------
CONNECT SYSTEM/manager@INST1
create or replace procedure waitfor_sql
(
 sql_stmt_in  varchar2,  		-- sql stat
 wait_limit number := 600,      -- max wait time 600, then time out 
 wait_for_zero boolean := true,         -- default is to wait for zero count
 exec_stmt varchar2 := null,          -- stmt to execute
 exec_interval number := 20,           -- interval to execute a stmt 
 wait_for_count number := 1,         -- number to rows to wait for
 debug_mode boolean := false
)
authid current_user as
 sql_stmt  varchar2(25536);  		-- sql stat
 wait_for_sql boolean := true ;
 slept number := 0 ;
 cnt number;
 exec_cnt number := 0;
 exec_slept number :=0;
 sleep_interval number := 3;
begin

 sql_stmt := 'select count(*) from ( ' || sql_stmt_in || ' )';
 while (wait_for_sql)
 loop
  dbms_lock.sleep(sleep_interval);
  slept := slept + sleep_interval ;
  execute immediate sql_stmt into cnt;

  if (exec_stmt is not null) then
    exec_slept := exec_slept + sleep_interval;
    if (exec_slept >= exec_interval) then
       execute immediate exec_stmt;
       exec_cnt := exec_cnt + 1;
       exec_slept := 0;
    end if;
  end if;

  if (( wait_for_zero and cnt = 0)  OR
      ( not wait_for_zero and cnt >= wait_for_count)) 
  then
    wait_for_sql := false ;
    if (debug_mode) then
      dbms_output.put_line ( 
              ' !!! - OK. After ' || slept || ' secs, count = ' || cnt);
      if (exec_stmt is not null) then
        dbms_output.put_line ( ' exec count = ' || exec_cnt);
      end if;
    end if;
  elsif slept > wait_limit then		-- - Don't wait after the limit
    wait_for_sql := false;
    dbms_output.put_line ( 
              ' !!! - Time out. After ' || slept || ' secs, count = ' || cnt);
    if (exec_stmt is not null) then
      dbms_output.put_line ( ' exec count = ' || exec_cnt);
    end if;
  end if ;
 end loop ; 
end ;
/
show errors;
grant execute on waitfor_sql to stradm;

SET SERVEROUTPUT ON
declare
  capname varchar2(30);
begin
  select capture_name into capname from dba_xstream_outbound;
  WAITFOR_SQL(
    SQL_STMT_IN => 
         'select 1 from gv$xstream_capture where ' ||
         'capture_name=''' || capname || ''' and ' ||
         '(state=''CAPTURING CHANGES'' or state=''ENQUEUING MESSAGE'' or ' ||
         ' state=''WAITING FOR TRANSACTION'' or ' || 
         ' state=''WAITING FOR INACTIVE DEQUEUERS'' or ' || 
         ' state=''CREATING LCR'')', 
    WAIT_LIMIT => 1800,
    WAIT_FOR_ZERO => FALSE);
end;
/

-- check outbound server status
SELECT c.state
     FROM v$xstream_capture c, DBA_XSTREAM_OUTBOUND o 
     WHERE c.CAPTURE_NAME = o.CAPTURE_NAME AND
           o.SERVER_NAME = 'DBNEXUS_OUT';

connect stradm/stradm@inst1
select capture_name, state from v$xstream_capture;
select server_name, connect_user, capture_name, source_database, capture_user,
  queue_owner, queue_name, user_comment, create_date 
  from dba_xstream_outbound;

connect stradm/stradm@inst2
select server_name, queue_owner, queue_name, apply_user, user_comment, 
  create_date from dba_xstream_inbound;

-------------------------------------------------------
-- (5) Execute small workload
-------------------------------------------------------
connect stradm/stradm@inst1
insert into emp values( 101, 'Abhishek',  'PO', null, to_date('17-11-1981','dd-mm-yyyy'), 5000,   null, 10);
insert into emp values( 7698, 'BLAKE',  'MANAGER',  7839, to_date('1-5-1981','dd-mm-yyyy'), 2850,     null, 30);
commit;

