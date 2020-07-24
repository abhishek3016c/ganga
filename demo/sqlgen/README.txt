/
/ $Header: rdbms/demo/xstream/sqlgen/README.txt /main/1 2009/06/11 23:06:29 praghuna Exp $
/
/ README.txt
/
/ Copyright (c) 2009, Oracle. All Rights Reserved.
/
/   NAME
/     README.txt - XStream Replication using Sql generation Demo
/
/   DESCRIPTION
/     <short description of component this file declares/defines>
/
/   NOTES
/     This demo is written to demonstrate the usage of the replication
/     using the sql generation API provided in OCI and JAVA. This demo
/     is not meant for production purpose.
/
/   MODIFIED   (MM/DD/YY)
/   praghuna    05/26/09 - Creation
/

Overview:

This is a demonstration program that demonstrates the usage of sql generation
in doing replication using LCRs coming out of XStreamsOut.

The demo program creates a user XSDEMOSG in the source and destination 
databases and creates the tables of various types in the XSDEMOSG schema.
It then executes a set of dmls on the tables in XSDEMOSG schema on the
source db. The demo OCI client application, when run, connects to the 
XStreamsOUT servers to get the dml lcrs and use the lcr to replicate on the
destination db.

                                        LCR
 source db (XStream outbound server) ---------->   XSDemoOutClient
                                                       |
                                                       |
                                                      \|/
 destination db    <-----------------------   Generate Sql using SQLGen API
                     Execute generated SQL 


This sample package contains the following files
  xsdemo_out.sql         - setup file for XStream Outbound Server
  xsdemo_in.sql          - setup file for XStream Inbound Server
  xsdemo_cadm.sql        - create user stradm and grant privilege
  xsdemo_env.sh          - environment setup script for Java program 
  XSDemoOutClient.c      - Client that uses SQL Generation API 

Requirements:
-------------
Two 11.2 or higher database instances that accept TCP connections. You
need the following information so that the client programs can connect
to your database instances:
  -- host name
  -- port number
  -- Oracle SID


Evironment Setup:
-----------------
The CLASSPATH, LD_LIBRARY_PATH, and PATH need to be set appropriately. 
The JDK version must be 1.5 or higer.

In C-Shell, simply load the xsdemo_env.sh to setup the environment.
    %source xsdemo_env.sh

For other platform, make sure the CLASSPATH, LD_LIBRARY_PATH, and 
PATH environment variables are set appropriately. 

The CLASSPATH needs to contain the following entries:
    ${ORACLE_HOME}/rdbms/jlib/xstreams.jar
    ${ORACLE_HOME}/dbjava/lib/ojdbc5.jar

The LD_LIBRARY_PATH needs to contain the directory of OCI JDBC driver 
shared library objects (libocijdbc*.so). 
    e.g ${ORACLE_HOME}/lib

The PATH needs to contain the correct JDK executables.
    e.g. ${ORACLE_HOME}/jdk/bin

If instant client is used to run the client programs, make sure the
CLASSPATH and LD_LIBRARY_PATH have entry pointing to your instant
client directory

Compilation:
------------

To compile the Java client program, simply do:
    javac *.java


Compile the C files like you would compile any OCI program.

Running Demo:
-------------

1. Setup XStream outbound 

  -- On source database, connect as a user with "create user" and 
     "grant dba" privilege, e.g. sysdba

     SQL> @xsdemo_cadm
     SQL> @xsdemo_sgsch.sql
     SQL> connect stradm/stradm
     SQL> @xsdemo_out.sql
     SQL> connect xsdemosg/xsdemosg
     SQL> @xsdemo_sgtab.sql

  -- On destination database, connect as a user with "create user"
     and "grant dba" privilege, e.g. sysdba

     SQL> @xsdemo_cadm
     SQL> @xsdemo_sgsch.sql
     SQL> connect xsdemosg/xsdemosg
     SQL> @xsdemo_sgtab.sql

2.  Startup XSDemoOutClient with the appropriate parameters on a separate
    shell

    Without any parameters it captures the LCRs generates SQL with 
    inline values and executes the generated sql in the destination 
    db. 

    % XSDemoOutClient -help 
   
    The above gives more information on the available command line parameters.

3.  Do some workload on XSDEMOSG schema on the source
  
    SQL> connect xsdemosg/xsdemosg
    SQL> @xsdemo_sgdml

 Depending on the parameter XSDemoOutClient is started with, it will execute 
 the generated sql on the destination database. This can be verified by 
 selecting the rows from the tables in the schema XSDEMOSG. This should be
 same as the source db.
  
 xsdemo_sgdml.sql is just a sample. You can execute your own dmls on the
 source on any schema and this would be replicated to the same schema
 at the destination db. 

                                                   
                      

