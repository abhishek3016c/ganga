/ $Header: rdbms/demo/xstream/oci/README.txt /main/4 2010/07/28 21:31:47 yurxu Exp $
/
/ README.txt
/
/   NAME
/     README.txt 
/
/   DESCRIPTION
/     Simple OCI demo programs using OCIXStream API.
/
/   NOTES
/     <other useful comments, qualifications, etc.>
/
/   MODIFIED   (MM/DD/YY)
/   yurxu       07/18/10 - Remove xoidkey.c
/   yurxu       06/20/10 - Add xoidkey.c
/   thoang      09/22/09 - Add xin.c
/   thoang      06/15/09 - Creation

Overview:

The sample programs in this directory demonstrate the use of OCIXStream API 
to receive/send LCRs and OCILCR API to access the LCR contents.

Directory Listing:

1) xiodemo.sql - SQL script to set up an XStream outbound server at inst1 and
   an inbound server at inst2. This script expects two databases, 
   inst1 & inst2, already created.

2) xout.c - sample XStream Out client application using non-callback method. 
   This application receives the lcrs from an XStream outbound server 
   and displays each LCR contents to the standard output. 

   Usage: xout -svr <svr_name> -db <db_name> -usr <conn_user> -pwd <password>

     svr  : outbound server name
     db   : database name of outbound server
     usr  : connect user to outbound server
     pwd  : password of outbound's connect user

3) xout_cb.c - sample XStream Out client application using callback method. 
   Same as xout.c except using callback method.

   Usage: xout_cb -svr <svr_name> -db <db_name> -usr <conn_user> -pwd <password>

     svr  : outbound server name
     db   : database name of outbound server
     usr  : connect user to outbound server
     pwd  : password of outbound's connect user

4) xio.c - sample XStream Out to In application using non-callback method. 
   This application streams lcrs from an XStream outbound server to inbound 
   server. It periodically gets the processed low position from the inbound 
   server to maintain the processed low position of the outbound server.

   Usage: xio -ob_svr <outbound_svr> -ob_db <outbound_db>
              -ob_usr <conn_user> -ob_pwd <conn_user_pwd>
              -ib_svr <inbound_svr> -ib_db <inbound_db>
              -ib_usr <apply_user> -ib_pwd <apply_user_pwd>

     ob_svr  : outbound server name
     ob_db   : database name of outbound server
     ob_usr  : connect user to outbound server
     ob_pwd  : password of outbound's connect user
     ib_svr  : inbound server name
     ib_db   : database name of inbound server
     ib_usr  : apply user for inbound server
     ib_pwd  : password of inbound's apply user

5) xin.c - sample XStream In application to demostrate how to construct DDL 
   and DML LCRs and send to an XStream Inbound server.

   Usage: xin [-usr <user name>] [-pwd <password>]
              [-db <db name>] [-svr <svr name>]

   To create an inbound server for xin program, execute 
   dbms_xstream.create_inbound procedure, for example, 
  
        begin
          dbms_xstream_adm.create_inbound(
            server_name => 'xin2',
            queue_name  => 'xin2_queue',
            comment     => 'XStreams In');
        end;
        /

   To start this inbound server, execute this command:
        exec dbms_apply_adm.start_apply('xin2');

Instructions:

1) Create two databases, inst1 and inst2.
2) Run xiodemo.sql. 
3) Compile and run each program.

   a) make -f $ORACLE_HOME/rdbms/demo/demo_rdbms.mk build EXE=xout OBJS=xout.o 
      xout -svr xout -db inst1 -usr stradm -pwd stradm 

   b) make -f $ORACLE_HOME/rdbms/demo/demo_rdbms.mk build EXE=xout_cb \
              OBJS=xout_cb.o
      xout_cb -svr xout -db inst1 -usr stradm -pwd stradm 

   c) make -f $ORACLE_HOME/rdbms/demo/demo_rdbms.mk build EXE=xio OBJS=xio.o
      xio -ob_svr xout -ob_db inst1 -ob_usr stradm -ob_pwd stradm \
          -ib_svr xin -ib_db inst2 -ib_usr stradm -ib_pwd stradm
 
   d) make -f $ORACLE_HOME/rdbms/demo/demo_rdbms.mk build EXE=xin OBJS=xin.o
      xin -svr xin2 -db inst1 -usr stradm -pwd stradm
