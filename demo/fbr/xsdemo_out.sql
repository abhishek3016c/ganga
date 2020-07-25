Rem
Rem $Header: rdbms/demo/xstream/fbr/xsdemo_out.sql /main/4 2014/10/14 18:38:08 snalla Exp $
Rem
Rem xsdemo_out.sql
Rem
Rem Copyright (c) 2009, 2014, Oracle and/or its affiliates. 
Rem All rights reserved.
Rem
Rem    NAME
Rem      xsdemo_out.sql - XStream Demo outbound setup script
Rem
Rem    DESCRIPTION
Rem      This script setup outbound server for XStream Demo
Rem
Rem    NOTES
Rem      <other useful comments, qualifications, etc.>
Rem
Rem    MODIFIED   (MM/DD/YY)
Rem    snalla      10/02/14 - add column format
Rem    thoang      10/13/09 - change dba_xstream_* queries
Rem    tianli      02/20/09 - Created
Rem
   
COLUMN "CAPNAME" FORMAT A32 
 

SET ECHO ON
SET FEEDBACK 1
SET NUMWIDTH 10
SET LINESIZE 80
SET TRIMSPOOL ON
SET TAB OFF
SET PAGESIZE 100

----------------------------------------------------------------------------
-- before this setup script, user stradm must be created with dba priviledge
-- on xstream out site and connected as stradm user.
----------------------------------------------------------------------------
variable xsout_dbname varchar2(80);
variable capname VARCHAR2(30);
variable queuename VARCHAR2(30);

-- create outbound server at xstream_out site
begin
  dbms_xstream_adm.create_outbound('DBNEXUS_OUT');
end;
/

begin
   SELECT c.CAPTURE_NAME
     into :capname
     FROM dba_capture c, DBA_XSTREAM_OUTBOUND o 
     WHERE c.CAPTURE_NAME = o.CAPTURE_NAME AND
           o.SERVER_NAME = 'DBNEXUS_OUT';
end;
/

print capname;

begin
   SELECT QUEUE_NAME
     into :queuename
     FROM DBA_XSTREAM_OUTBOUND
     WHERE SERVER_NAME = 'DBNEXUS_OUT';
end;
/
print queuename

execute select global_name into :xsout_dbname from global_name
print xsout_dbname;

begin
 dbms_streams_adm.add_global_rules(
  streams_type            => 'CAPTURE',
  streams_name            => :capname,
  queue_name              => :queuename,
  include_dml             => TRUE,
  include_ddl             => TRUE,
  include_tagged_lcr      => TRUE,
  source_database         => :xsout_dbname);
END;
/

-- check outbound server status
SELECT c.state
     FROM v$xstream_capture c, DBA_XSTREAM_OUTBOUND o 
     WHERE c.CAPTURE_NAME = o.CAPTURE_NAME AND
           o.SERVER_NAME = 'DBNEXUS_OUT';

select capture_name, state from v$xstream_capture;
select server_name, connect_user, capture_name, source_database, capture_user,
  queue_owner, queue_name, user_comment, create_date
  from dba_xstream_outbound;
select server_name, source_database, processed_low_position,
 processed_low_time from dba_xstream_outbound_progress;

