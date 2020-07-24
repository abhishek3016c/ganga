Rem
Rem $Header: rdbms/demo/xstream/sqlgen/xsdemo_sgtab.sql /main/1 2009/06/11 23:06:29 praghuna Exp $
Rem
Rem sqlgentab.sql
Rem
Rem Copyright (c) 2009, Oracle and/or its affiliates. All rights reserved. 
Rem
Rem    NAME
Rem      sqlgentab.sql - <one-line expansion of the name>
Rem
Rem    DESCRIPTION
Rem      <short description of component this file declares/defines>
Rem
Rem    NOTES
Rem      <other useful comments, qualifications, etc.>
Rem
Rem    MODIFIED   (MM/DD/YY)
Rem    praghuna    05/25/09 - Created
Rem

SET ECHO ON
SET FEEDBACK 1
SET NUMWIDTH 10
SET LINESIZE 80
SET TRIMSPOOL ON
SET TAB OFF
SET PAGESIZE 100

drop table tab01;
drop table tab02;
drop table tab03;
drop table tab04;
drop table tab05;


create table tab01
(
num number primary key,
bf binary_float, 
bd binary_double,
chr char(10),
vchr varchar2(10),
nchr nchar(10),
nvchr nvarchar2(10),
r raw(10),
dt date,
ts timestamp,
tstz timestamp with time zone,
y2m  interval year to month,
d2s interval day to second
);


create table tab02(
n1 number primary key, 
long1 long
);

create table tab03 
(
  n1 number primary key,
  clob1 clob
);

create table tab04
(
  n1 number primary key,
  blob1 blob
);


create table tab05
(
  n1 number primary key,
  xml1 xmltype
);

Rem create and return a clob of size amt
create or replace function get_clob(amt number) return CLOB as
 buf clob;
 i number;
begin
  DBMS_LOB.CREATETEMPORARY(buf, TRUE, DBMS_LOB.SESSION);

  for i in 1..amt loop
      dbms_lob.write(buf,1,i,'A');
  end loop;

  return buf;
end;
/

Rem create and return a blob of size amt
create or replace function get_blob(amt number) return blob as
 buf blob;
 i number;
begin
  DBMS_LOB.CREATETEMPORARY(buf, TRUE, DBMS_LOB.SESSION);

  for i in 1..amt loop
      dbms_lob.write(buf,1,i,'A');
  end loop;

  return buf;
end;
/

