/
/ $Header: rdbms/demo/xstream/fbr/README.txt /main/1 2009/03/19 14:36:30 tianli Exp $
/
/ READEME.txt
/
/ Copyright (c) 2009, Oracle. All Rights Reserved.
/
/   NAME
/     README.txt - XStream file based replication Demo User Guide
/
/   DESCRIPTION
/     <short description of component this file declares/defines>
/
/   NOTES
/     The file based replication example is used only to demonstrate
/     the usage of XStream API. This demo is not supported for 
/     production purpose.
/
/   MODIFIED   (MM/DD/YY)
/   tianli      02/20/09 - Creation
/


Overview:
---------

This sample program demonstrates a file based replication solution
using XStream APIs.

                                          LCR 
  source db (XStream outbound server) ---------->   XSDemoOutClient
                                                          |
                                                          | write LCRs
                                                         \|/
                                                      LCR log files
                                                          |
                                                          | read LCRs
                                                         \|/
  destination db (XStream inbound server) <------   XSDemoInClient
                                            LCR

On source database, databased changes are captured as Logical Change
Records (LCRs). OutClient program attaches to the XStream outbound
server on the source database and retrieve LCRs from the outbound
server. It then writes LCRs to a sequence of LCR log files. 

InClient program attaches to the XStream inbound server on the
destination database, reads LCRs from the LCR log files, and sends
them to the inbound server so that database changes will be applied
on the destination database.


This sample package contains the following files
  xsdemo_out.sql         - setup file for XStream Outbound Server
  xsdemo_in.sql          - setup file for XStream Inbound Server
  xsdemo_cadm.sql        - create user stradm and grant privilege
  xsdemo_env.sh          - environment setup script for Java program 
  XSDemoLCRWriter.java   - writing LCRs to a specified log file
  XSDemoLCRReader.java   - reading LCRs from a specified log file
  XSDemoOutClient.java   - client receiving LCRs from the outbound server
  XSDemoInClient.java    - client sending LCRs to the inbound server


Requirements:
--------------
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


Running Demo
------------

1. Setup XStream outbound and inbound server

  -- On source database, connect as a user with "create user" and 
     "grant dba" privilege, e.g. sysdba

     SQL> @xsdemo_cadm
     SQL> connect stradm/stradm
     SQL> @xsdemo_out.sql

  -- On destination database, connect as a user with "create user"
     and "grant dba" privilege, e.g. sysdba

     SQL> @xsdemo_cadm
     SQL> connect stradm/stradm
     SQL> @xsdemo_in.sql

2. Startup XSDemoOutClient and XSDemoInClient client program
  
  -- java XSDemoOutClient <oracle_sid> <host> <port>
  -- java XSDemoInclient <oracle_sid> <host> <port>

  -- java command line property options
      -DPrefix=<prefix>  Prefix for the demo LCR log files
      -DTracing=true     turn on the client side tracing
     e.g java -DPrefix=xsdemo_log -DTracing=true XSDemoInclient ...

3. do some workload on source and verify the result on destination
     e.g.  
     SQL> create table xsdemo_test(id number, name varchar2(30));
     SQL> insert into xsdemo_test values(1, "Oracle");
     SQL> commit;

  -- global rules are used to capture both ddl and dml for all schemas


LCR log file format and recovery
--------------------------------
This Demo writes LCRs to a sequence of LCR log files in the current
directory. The log file prefix by default is xsdemo_archive. The log 
file name sequence starts from 1. Users can also specify the prefix in 
Java command line system property.
  i.e. xsdemo_archive_1.fbr
       xsdemo_archive_2.fbr
       xsdemo_archive_3.fbr
       ...

Each log file contains a nubmer of LCRs (from one batch processing). 
XStreamOut client receives LCRs in a batch fashion (batch duration is 
30 second by default). A log file is the basic recovery unit. i.e., 
if any error occurs, the entire log file is discarded. 

In each log file, a fixed size header is reserved at the beginning of
the log file. This header should contain the LCRID (LCR position) of the
last LCR in the batch. When we start writing LCRs to this log file, this
header is empty, it is filled only after we have successfully written
all LCRs in the current batch into this log file. This header serves
the following two purpose:
1. we can examine the header of the file and determine if this log file 
   contains valid LCRs. If any error occurs during this batch, the
   header will not contains a valid LCR position and we can discard this 
   log file immediately
2. upon client restart or any error case that requires client re-attach
   to inbound/outbound server:
   -- outbound client needs to inform outbound server the last position
      of the LCR it has processed, so that outbound server knows where 
      in the entire LCR stream should it start sending LCRs to the client.
   -- inbound client needs to obtain the last position of the LCR the
      inbound server has processed, so that the inbound client knows 
      where should it start sending LCRS to the inbound server.
   Since the header contains the position of the last LCR in the logfile
   inbound/outbound clients can easily decide which log file should it
   start to write/read in the recovery case without actually reading
   any LCR.

To avoid scanning the file sequence and opening each log file and 
reading the LCR position from the header in case of recovery, 
OutClient and Inclient also keep track of which log file it has 
processed. In this demo, a progress file is used, but in real world 
application, a progress table in the database will certainly provide 
maximum guarantee.

