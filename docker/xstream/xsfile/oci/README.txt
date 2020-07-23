/
/ $Header: rdbms/demo/xstream/xsfile/oci/README.txt /main/1 2009/10/07 10:55:03 tianli Exp $
/
/ README.txt
/
/ Copyright (c) 2009, Oracle. All Rights Reserved.
/
/   NAME
/     README.txt -  XStream file based data exchange Demo User Guide
/
/   DESCRIPTION
/     <short description of component this file declares/defines>
/
/   NOTES
/     The XStream file format data exchange demo is used only to demonstrate
/     the usage of XStream API. This demo is not supported for production
/     purpose.   
/
/   MODIFIED   (MM/DD/YY)
/   tianli      09/10/09 - Creation
/

Overview:
---------

This sample program demonstrates a file based data exchange solution
using XStreamOut APIs.


                                          LCR 
  source db (XStream outbound server) ---------->    xsfile demo client
                                                          |
                                                          | write data
                                                         \|/
                                                      data files

The data files contain row changes in Comma Separated Value(CSV) format
or Oracle sql*loadr data file format. The data files can then be loaded 
into another database through bulk loading utilities such as Oracle 
SQL*Loader or DB2 import/load utility.

The row changes are stored in a change data capture format. For example, if a
table "xstest" has columns colA and colB, then the change record in the
file contains several control conlumns as well as the column colA and
colB. The control conlumns include the following:

  command_type      INSERT/UPDATE/DELETE
  object_owner      owner of the database object
  object_name       name of the database object 
  position          indicting where the LCR is in the stream
  column_list_type  NEW/OLD

-- table shape
   a tableshape file "xsfile.ts" is used to describe the tabel at the
   target database. This table shape file is a user generated CSV file
   that contains the column name and definition list in the same order 
   as the table defined in the target database. each line represents the 
   shape of one table. The first two fields are table owner and 
   table name. e.g.
        "stradm", "xstest", "colA", "number", "colB", "varchar2(30)"

   xsfile program ignores any LCR that does not have a table shape defined
   in the xsfile.ts file. 

CSV mode
---------

In CSV mode, xsfile program receives LCRs from the outbound server and
write the data into CSV files. this csv file then can be loaded
into other database or application. 

Where running in CSV mode, data files are named following the convention:
   'schema name'_'table name'_'sequence#'.csv
where the sequence number is assigned by the demo program. The sequence 
number starts from 1. 


sqlldr mode
-----------

In sqlldr mode, for each table, a control file will be generated. The 
control file is named following the convention: 
   'schema name'_'table name'_'sequence#'.ctl
and the data file is named following the convention:
   'schema name'_'table name'_'sequence#'.dat

As mentioned earlier, the row changes are loaded into a change
table. In sqlldr control file, the table name for the change table is 
named following the convention:
  'original_table_name'_chg
So, for the "xstest" table, the change table on the target database 
is named as "xstest_chg". This table must be created accordingly in the 
target database.

The control file only contains the definition and the actual data will
be stored in the data files, to load them into database via sqlldr,
data files must be appended to the end of control files. e.g.
   cat stradm_xstest_1.dat >> stradm_xstest.ctl
   cat stradm_xstest_2.dat >> stradm_xstest.ctl


package contents
----------------
This demo requires the following files
  demo/xstream/fbr/xsdemo_out.sql   - setup file for XStream Outbound Server
  demo/xstream/fbr/xsdemo_cadm.sql  - create user stradm and grant privilege
  demo/xstream/xsfile/oci/xsfile.c  - xsfile demo program


xsfile demo arguments
---------------------
  Usage: xsfile -svr <svr_name> -db <db_name> -usr <conn_user> 
                -tracing <true|false> -interval <interval_value> -mode <mode>

  svr      : outbound server name
  db       : database name of outbound server
  usr      : connect user to outbound server
  tracing  : turn on|off the tracing, tracing is off by default
  interval : number of batches before switching to next data file,
             each batch in XStream by default is 30 sec, the default
             value for the interval is 1
  mode     : currently support 'csv' or 'sqlldr' mode, default mode is csv


Requirements:
--------------
Oracle 11.2 or higher Enterprise Edition with XStream feature. 

Database preparation
--------------------
-- turn on "supplemental logging on all columns" for the tables you 
   want to capture in order to have unchanged columns in the LCRs. 
-- turn on archive log mode

Compilation:
------------
make -f $ORACLE_HOME/rdbms/demo/demo_rdbms.mk build EXE=xsfile OBJS=xsfile.o 

Running Demo
------------
Note: we are using the same database as the source and destination
database in the example. 

1. Setup XStream outbound

  -- On the database, connect as a user with "create user" and 
     "grant dba" privilege, e.g. sysdba

     SQL> @xsdemo_cadm
     SQL> connect stradm/stradm
     SQL> @xsdemo_out.sql

2. create a test table and change table on the database, e.g.
  -- SQL> create table xstest(colA number, 
                              colB varchar2(30));
     SQL> create table xstest_chg(command_type      varchar2(10),
                                  object_owner      varchar2(30),
                                  object_name       varchar2(30),
                                  position          raw(64),
                                  column_list_type  varchar2(3),
                                  colA              number, 
                                  colB              varchar2(30));

3. create a tableshape file xsfile.ts with the following contents:
  -- "stradm","xstest","colA","number","colB","varchar2(30)"

4. start the xsfile demo program, this will prompt you to enter the
   password for user stradm, which is "stradm"
  -- ./xsfile -svr demoout -db inst1 -usr stradm -mode sqlldr

5. on the database, insert some data, e.g.
     SQL> insert into xstest values(1, 'Oracle');
     SQL> commit;

   xsfile program will generate a control file stradm_xstest.ctl, and 
   writes the change records into data files.      

6. use sqlldr to load the change into change table. First you need to
   append data files to the end of the control file
  -- cat STRADM_XSTEST_1.dat >> stradm_xstest.ctl
     cat STRADM_XSTEST_2.dat >> stradm_xstest.ctl
     ...

  -- sqlldr userid=stradm/stradm control=stradm_xstest.ctl

