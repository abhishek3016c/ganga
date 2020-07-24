Rem
Rem $Header: rdbms/demo/xstream/sqlgen/xsdemo_sgsch.sql /main/2 2011/04/06 03:08:08 snalla Exp $
Rem
Rem xsdemo_sgsch.sql
Rem
Rem Copyright (c) 2009, 2011, Oracle and/or its affiliates. 
Rem All rights reserved. 
Rem
Rem    NAME
Rem      xsdemo_sgsch.sql - <one-line expansion of the name>
Rem
Rem    DESCRIPTION
Rem      <short description of component this file declares/defines>
Rem
Rem    NOTES
Rem      <other useful comments, qualifications, etc.>
Rem
Rem    MODIFIED   (MM/DD/YY)
Rem    praghuna    05/26/09 - Created
Rem

SET ECHO ON
SET FEEDBACK 1
SET NUMWIDTH 10
SET LINESIZE 80
SET TRIMSPOOL ON
SET TAB OFF
SET PAGESIZE 100

create user xsdemosg identified by xsdemosg;
grant connect, resource, unlimited tablespace, create table to xsdemosg;

