
Rem
Rem $Header: rdbms/demo/xstream/idkey/xoiddemo.sql /main/3 2012/08/01 12:25:29 jorgrive Exp $
Rem
Rem xoiddemo.sql
Rem
Rem Copyright (c) 2010, 2012, Oracle and/or its affiliates. 
Rem All rights reserved. 
Rem
Rem    NAME
Rem      xoiddemo.sql - Set up ID Key demo in XStream Out
Rem
Rem    DESCRIPTION
Rem      This script creates an XStream administrator user, 'stradm', at
Rem      inst1. Then it creates an XStream outbound server, named
Rem      'XOUT', at inst1 and runs a small workload in customers table. 
Rem
Rem    NOTES
Rem
Rem    MODIFIED   (MM/DD/YY)
Rem    jorgrive    07/18/12 - BUG 14239208 incorrect view
Rem    jmadduku    02/18/11 - Grant Unlimited Tablespace priv with RESOURCE
Rem    yurxu       06/21/10 - Created
Rem

SET ECHO ON
SET FEEDBACK 1
SET NUMWIDTH 10
SET LINESIZE 80
SET TRIMSPOOL ON
SET TAB OFF
SET PAGESIZE 100

define inst1_sys_passwd=&1

-------------------------------------------------------
-- (1) create stradm user, grant priv
-------------------------------------------------------
connect sys/&inst1_sys_passwd@inst1 as sysdba
drop user stradm cascade;
grant resource, UNLIMITED TABLESPACE, connect to stradm identified by stradm;
grant dba to stradm;
exec dbms_xstream_auth.grant_admin_privilege(grantee => 'stradm');

-------------------------------------------------------
-- (2) Set up XStream Out at inst1
-------------------------------------------------------
-- create outbound server at inst1
connect stradm/stradm@inst1
exec dbms_xstream_adm.create_outbound('XOUT');

-------------------------------------------------------
-- (3) Turn on ID KEY support
-- must turn on ID KEY support, otherwize there is no
-- LCR will be received from outbound server for the row
-- with unsupported data type.
-------------------------------------------------------
declare
  capname varchar2(30);
begin
  select capture_name into capname from dba_xstream_outbound;

  DBMS_CAPTURE_ADM.SET_PARAMETER(
    CAPTURE_NAME  => capname,
    PARAMETER   => 'CAPTURE_IDKEY_OBJECTS',
    VALUE       => 'Y');
end;
/

-------------------------------------------------------
-- (4) Wait for capture at inst1 to be up and running
-------------------------------------------------------
CONNECT SYSTEM/manager@INST1
-- give capture time to start up
exec dbms_lock.sleep(30)

-- check outbound server status
SELECT c.state
     FROM v$xstream_capture c, DBA_XSTREAM_OUTBOUND o 
     WHERE c.CAPTURE_NAME = o.CAPTURE_NAME AND
           o.SERVER_NAME = 'XOUT';

connect stradm/stradm@inst1
select capture_name, state from v$xstream_capture;
select server_name, connect_user, capture_name, source_database, capture_user,
  queue_owner, queue_name, user_comment, create_date 
  from dba_xstream_outbound;

-------------------------------------------------------
-- (5) Execute small workload
-------------------------------------------------------
connect oe/oe@inst1
declare 
  j NUMBER;
begin
  select count(*) into j from customers;
  for i in 1 .. 10 loop
    insert into customers values (1100 + i +j, 'Constantin',  'Welles',
      CUST_ADDRESS_TYP('514 W Superior St', '46901', 'Kokomo', 'IN', 'US'),
      NULL,
      NULL, 
      NULL, 
      NULL,
      NULL,
      NULL);
    commit;
  end loop;
end;
/
