Rem
Rem $Header: rdbms/demo/xstream/fbr/xsdemo_in.sql /main/2 2009/11/04 23:21:58 thoang Exp $
Rem
Rem xsdemo_in.sql
Rem
Rem Copyright (c) 2009, Oracle and/or its affiliates. All rights reserved. 
Rem
Rem    NAME
Rem      xsdemo_in.sql - XStream Demo inbound setup script
Rem
Rem    DESCRIPTION
Rem      <short description of component this file declares/defines>
Rem
Rem    NOTES
Rem      <other useful comments, qualifications, etc.>
Rem
Rem    MODIFIED   (MM/DD/YY)
Rem    thoang      10/13/09 - change dba_xstream_* queries
Rem    tianli      02/20/09 - Created
Rem

SET ECHO ON
SET FEEDBACK 1
SET NUMWIDTH 10
SET LINESIZE 80
SET TRIMSPOOL ON
SET TAB OFF
SET PAGESIZE 100

----------------------------------------------------------------------------
-- before this setup script, user stradm must be created with dba priviledge
-- on xstream inbound site and connected as stradm user.
----------------------------------------------------------------------------
begin
  dbms_streams_adm.set_up_queue(
    queue_table => 'xsin_qtab',
    queue_name  => 'xsin_queue',
    queue_user  => 'stradm');
end;
/

begin
  dbms_xstream_adm.create_inbound('DEMOIN', 
    queue_name  => 'xsin_queue',  
    comment      => 'Demo Inbound Server');
end;
/
select apply_name, apply_user, purpose from dba_apply order by 1;

select server_name, queue_owner, queue_name, apply_user, user_comment,
  create_date from dba_xstream_inbound;
select * from dba_xstream_inbound_progress;

