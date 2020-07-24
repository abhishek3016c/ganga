Rem
Rem $Header: rdbms/demo/xstream/fbr/xsdemo_cadm.sql /main/2 2011/05/27 08:25:53 jmadduku Exp $
Rem
Rem xsdemo_cadm.sql
Rem
Rem Copyright (c) 2009, 2011, Oracle and/or its affiliates. 
Rem All rights reserved. 
Rem
Rem    NAME
Rem      xsdemo_cadm.sql - create stradm user and grant privilege
Rem
Rem    DESCRIPTION
Rem      <short description of component this file declares/defines>
Rem
Rem    NOTES
Rem      <other useful comments, qualifications, etc.>
Rem
Rem    MODIFIED   (MM/DD/YY)
Rem    jmadduku    02/18/11 - Grant Unlimited Tablespace priv with RESOURCE
Rem    tianli      02/20/09 - Created
Rem

SET ECHO ON
SET FEEDBACK 1
SET NUMWIDTH 10
SET LINESIZE 80
SET TRIMSPOOL ON
SET TAB OFF
SET PAGESIZE 100

drop user stradm cascade;
grant resource, UNLIMITED TABLESPACE, connect to stradm identified by stradm;
grant dba to stradm;
exec dbms_streams_auth.grant_admin_privilege(grantee =>'stradm');
