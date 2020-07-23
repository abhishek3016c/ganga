/ $Header: rdbms/demo/xstream/java/README.txt /main/3 2010/07/28 21:31:47 yurxu Exp $
/
/ README.txt
/
/   NAME
/     README.txt 
/
/   DESCRIPTION
/     Simple Java demo programs using XStream API.
/
/   NOTES
/     <other useful comments, qualifications, etc.>
/
/   MODIFIED   (MM/DD/YY)
/   yurxu       07/18/10 - Remove xoidkey.java
/   yurxu       06/20/10 - Add xoidkey.java
/   tianli      06/15/09 - Creation

Overview:

The sample programs in this directory demonstrate the use of XStream
Java API to receive/send/access the LCRs. The demo assumes two 11.2 databases
with TCP listeners.


Directory Listing:

1) xsdemo_env.sh - shell script to setup environment in order to run the
   java demo clients.


2) xout.java - sample XStream Out client application using non-callback method. 
   This application receives the lcrs from an XStream outbound server 
   and displays each LCR contents to the standard output. 
 
   Usage: java xout <xsout_oraclesid> <xsout_host> <xsout_port>
                    <xsout_username> <xsout_passwd> <xsout_servername>

3) xio.java - sample XStream Out to In application using non-callback method. 
   This application streams lcrs from an XStream outbound server to inbound 
   server. It periodically gets the processed low position from the inbound 
   server to maintain the processed low position of the outbound server.

   Usage: java xio  <xsin_oraclesid> <xsin_host> <xsin_port>
                    <xsin_username> <xsin_passwd> <xin_servername>
                    <xsout_oraclesid> <xsout_host> <xsout_port>
                    <xsout_username> <xsout_passwd> <xsout_servername>

Instructions:

1) Create two databases with TCP listeners.
2) Run ../oci/xiodemo.sql. 
3) Compile and run each program.

   a) source xsdemo_env.sh
   b) javac *.java
   c) run the java client. 
