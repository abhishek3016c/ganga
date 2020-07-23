/
/ $Header: rdbms/demo/xstream/idkey/README.txt /main/1 2010/07/28 21:31:47 yurxu Exp $
/
/ README.txt
/
/ Copyright (c) 2010, Oracle. All Rights Reserved.
/
/   NAME
/     README.txt 
/
/   DESCRIPTION
/     Simple OCI/Java demo programs using OCIXStream API.
/
/   NOTES
/     <other useful comments, qualifications, etc.>
/
/   MODIFIED   (MM/DD/YY)
/   yurxu       07/19/10 - Creation
/

Overview:

The sample programs in this directory demonstrate the use of OCIXStream API 
to receive IDKEY LCRs and to query the source database with the RowID 
from a IDKEY LCR.

Directory Listing:
 
1) xoiddemo.sql - SQL script to set up an XStream outbound server at inst1 and
   turn on ID KEY supports for the capture. This script expects one database,
   inst1, already created.

2) oci/xoidkey.c - sample ID KEY client application for XStream Out. 
   This application receives the lcrs from an XStream outbound server 
   and displays each LCR contents to the standard output, if it is ID KEY LCR, 
   then queries the source database with the ROWID inside it. 

   Usage: xoidkey  -ob_svr <outbound_svr> -ob_db <outbound_db>
                   -ob_usr <conn_user> -ob_pwd <conn_user_pwd>
                   -db_usr <db_user> -db_pwd <db_user_pwd>
     ob_svr  : outbound server name
     ob_db   : database name of outbound server
     ob_usr  : connect user to outbound server
     ob_pwd  : password of outbound's connect user
     db_usr  : connect user to database
     db_pwd  : password of database's connect user

Instructions to run xoidkey:

  1) Create one database: inst1.
  2) Run xoiddemo.sql with sys_passwd for inst1.
  3) cd oci
  4) Compile and run the client program.

   make -f $ORACLE_HOME/rdbms/demo/demo_rdbms.mk build EXE=xoidkey OBJS=xoidkey.o 
   ./xoidkey or
   ./xoidkey -ob_svr xout -ob_db inst1 -ob_usr stradm -ob_pwd stradm -db_usr oe -db_pwd oe

The Java demo programs assumes a 11.2 database with TCP listener.

3) java/xsdemo_env.sh - shell script to setup environment in order to run the
   java demo clients.

4) java/xoidkey.java -  sample ID KEY client application for XStream Out.
   This application receives the lcrs from an XStream outbound server,
   if it is ID KEY LCR, then displays LCR contents to the standard output,  
   and queries the source database with the ROWID inside it.

   Usage: java xoidkey <xsout_oraclesid> <xsout_host> <xsout_port>
                       <ob_usrname> <ob_passwd> <xsout_servername>
                       <db_usrname> <db_passwd>

  1) Create one database with TCP listener.
  2) Run xoiddemo.sql with sys_passwd for inst1.
  3) cd java
  4) Compile and run the client program.

   a) source xsdemo_env.sh
   b) javac xoidkey.java
   c) java xoidkey <xsout_oraclesid> <xsout_host> <xsout_port>
                   <ob_usrname> <ob_passwd> <xsout_servername>
                   <db_usrname> <db_passwd>

