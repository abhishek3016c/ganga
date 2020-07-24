Rem
Rem $Header: rdbms/demo/xstream/sqlgen/xsdemo_sgdml.sql /main/1 2009/06/11 23:06:29 praghuna Exp $
Rem
Rem xsdemo_sgdml.sql
Rem
Rem Copyright (c) 2009, Oracle and/or its affiliates. All rights reserved. 
Rem
Rem    NAME
Rem      xsdemo_sgdml.sql - <one-line expansion of the name>
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


insert into tab01 values (
1,
1.1,
1.2,
'char10','varchar10',
N'nchar10', N'nvarchar10',
hextoraw('1234abcde'),
to_date('02-jan-2008','dd-mon-yyyy'),
to_timestamp('01-JAN-2008 06:30:15.000001 PM'),
to_timestamp_tz('01-JAN-2008 18:30:15.000001 US/Pacific PST',
'DD-MON-YYYY HH24:MI:SS.FF TZR TZD'),
to_yminterval('01-02'),
to_dsinterval('10 10:00:00.000005')
);

commit;

insert into tab01 values (
2,
2.1,
2.2,
'char10','varchar10',
N'nchar10', N'nvarchar10',
hextoraw('1234abcde'),
to_date('02-jan-2008','dd-mon-yyyy'),
to_timestamp('01-DEC-2008 06:30:15.000001 PM'),
to_timestamp_tz('01-JAN-2008 20:30:15.000001 US/Pacific PST',
'DD-MON-YYYY HH24:MI:SS.FF TZR TZD'),
to_yminterval('01-02'),
to_dsinterval('10 10:00:00.000005')
);

commit;

insert into tab01 values (
3,
1.1,
1.2,
'char10','varchar10',
N'nchar10', N'nvarchar10',
hextoraw('1234abcde'),
to_date('02-jan-2008','dd-mon-yyyy'),
to_timestamp('01-JAN-2008 06:30:15.000001 PM'),
to_timestamp_tz('01-JAN-2008 18:30:15.000001 US/Pacific PST',
'DD-MON-YYYY HH24:MI:SS.FF TZR TZD'),
to_yminterval('01-02'),
to_dsinterval('10 10:00:00.000005')
);

commit;

Rem insert null
insert into tab01 (num) values (4);
commit;



insert into tab02 values (1, 'long column');
commit;

insert into tab02 values (2, get_clob(3999));
commit;

insert into tab02 values (3, null);
commit;

insert into tab03 values (1,'firstclobcolumn');
commit;
insert into tab03 values (2, 'secondlobcolumn');
commit;

Rem large lob values that spans multiple chunks

DECLARE
  buf clob;
BEGIN
  buf := get_clob(20000);
  insert into tab03 values (3, buf);
  dbms_lob.freetemporary(buf);
end;
/
commit;

insert into tab03 values (4, null);
commit;

insert into tab04 values (1,'010203040506070809');
insert into tab04 values (2, '090807060504030201');
commit;

Rem large lob values that spans multiple chunks

DECLARE
  buf blob;
BEGIN
  buf := get_blob(20000);
  insert into tab04 values (3, buf);
  dbms_lob.freetemporary(buf);
end;
/
commit;

insert into tab05 values (1, '<xml />');
commit;

declare
  schema_doc varchar2(3000) :=
   '<?xml version="1.0" encoding="UTF-8" ?>
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
               targetNamespace="http://xmlns.oracle.com/TR"
               xmlns="http://xmlns.oracle.com/TR"
               elementFormDefault="qualified">
      <xs:element name="TestRun" type="TestRunType" />
      <xs:complexType name="TestRunType">
        <xs:sequence>
          <xs:element name="query" type="queryType" minOccurs="0"/>
          <xs:element name="query2" type="queryType" minOccurs="0"/>
          <xs:any namespace="##other" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
      </xs:complexType>
      <xs:complexType name="queryType">
        <xs:sequence>
          <xs:element name="elapsed_time" type="xs:string"
                      minOccurs="0" maxOccurs="1"/>
          <xs:element name="error_message" type="xs:string"
                      minOccurs="0" maxOccurs="1"/>
          <xs:element name="rows_processed" type="xs:string"
                      minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="qid" type="xs:string" />
      </xs:complexType>
    </xs:schema>
  ';
begin
   insert into tab05 values (2, schema_doc);
   commit;
end;
/

insert into tab05 values (3, null);
commit;

REM Updates


update tab01 set num = 11, bf = 1.2 where num = 2;

rem null setting for all the columns
update tab01 set bf= null, bd=null, chr=null, vchr=null,
               nchr = null, nvchr= null, r= null, dt = null,
               ts = null, tstz = null, y2m = null, d2s= null
where num = 3;

commit;



update tab02 set long1 = 'updated long column' where n1 = 1;
commit;

update tab02 set long1 = null where n1 = 2;
commit;

update tab02 set long1 = get_clob(3000) where n1 = 3;
commit;


update tab03 set clob1='updated first clobcolumn' where n1 = 1;
commit;

update tab03 set clob1=null where n1=2;
commit;

DECLARE
  buf clob;
BEGIN
  buf := get_clob(20000);
  update tab03 set clob1=buf where n1=3;
  dbms_lob.freetemporary(buf);
end;
/
commit;

rem lob write
declare 
  l1 clob;
  sze number;
  buf clob;
begin

   dbms_lob.createtemporary(buf,true);
   select clob1 into l1 from tab03 where n1 = 1 for update;
   
   for sze in 1..10000
   loop
     dbms_lob.write(buf, 1, sze,'W');
   end loop;

   dbms_lob.write(l1, dbms_lob.getlength(buf), 25, buf);
   commit;
   dbms_lob.freetemporary(buf);
end;
/

rem lob erase
declare 
  l1 clob;
  amt number;
begin
   select clob1 into l1 from tab03 where n1 = 1 for update;
   
   amt := 25;
   dbms_lob.erase(l1, amt, 20);
   commit;
end;
/

rem lob trim
declare 
  l1 clob;
  amt number;
begin
   select clob1 into l1 from tab03 where n1 = 1 for update;
   
   amt := 8000;
   dbms_lob.trim(l1, amt);
   commit;
end;
/
  
update tab05 set xml1 = '<xml />' where n1 = 2;
commit;


REM Deletes

delete from tab01 where num = 3;
commit;

delete from tab02 where n1=1;
commit;

delete from tab03 where n1 = 3;
commit;

delete from tab04 where n1 = 3;
commit;

delete from tab05 where n1=2;
commit;
