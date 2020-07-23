/* $Header: rdbms/demo/xstream/java/xio.java /main/1 2009/07/07 13:43:31 thoang Exp $ */

/* Copyright (c) 2009, Oracle and/or its affiliates. All rights reserved. */

/*
   NAME   
      xio.java - XStream Out -> In Java API Test

   DESCRIPTION  
   This program first attaches to an XStream outbound server and inbound
   server. Next it goes into an infinite loop waiting for lcrs from the
   outbound server. Each lcr received is immediately sent to the inbound
   server. While transferring data, it periodically gets the processed 
   low position from the inbound server and sends that value to the 
   outbound server.
   
   This program waits indefinitely for transactions from the outbound server.
   Hit control-C to interrupt the program.

   If the program is restarted, the outbound server will start sending
   from the processed low position which was set during the previous run.
   
   environment setup: source demo/xstream/fbr/java/xsdemo_env.sh
   database setup: 
   compilation: javac xio.java
   Usage: java xio <xsin_oraclesid> <xsin_host> <xsin_port> 
                   <usrname> <passwd> <xsin_servername>
                   <xsout_oraclesid> <xsout_host> <xsout_port>
                   <usrname> <passwd> <xsout_servername>

   MODIFIED    (MM/DD/YY)
    tianli      07/06/09 - Creation
 */

/**
 *  @version $Header: rdbms/demo/xstream/java/xio.java /main/1 2009/07/07 13:43:31 thoang Exp $
 *  @author  tianli  
 *  @since   release specific (what release of product did this appear in)
 */

import oracle.streams.*;
import oracle.jdbc.internal.OracleConnection;
import oracle.jdbc.*;
import oracle.sql.*;
import java.sql.*;
import java.util.*;

public class xio
{
  public static String xsinusername = null;
  public static String xsinpasswd = null;
  public static String xsinName = null;
  public static String xsoutusername = null;
  public static String xsoutpasswd = null;
  public static String xsoutName = null;
  public static String in_url = null;
  public static String out_url = null;
  public static Connection in_conn = null;
  public static Connection out_conn = null;
  public static XStreamIn xsIn = null;
  public static XStreamOut xsOut = null;
  public static byte[] lastPosition = null;
  public static byte[] processedLowPosition = null;
    
  public static void main(String args[])
  {
    // get connection url to inbound and outbound server
    in_url = parseXSInArguments(args);    
    out_url = parseXSOutArguments(args);    

    // create connection to inbound and outbound server
    in_conn = createConnection(in_url, xsinusername, xsinpasswd);
    out_conn = createConnection(out_url, xsoutusername, xsoutpasswd);

    // attach to inbound and outbound server
    xsIn = attachInbound(in_conn);
    xsOut = attachOutbound(out_conn);
    
    // main loop to get lcrs 
    get_lcrs(xsIn, xsOut);
    
    // detach from inbound and outbound server
    detachInbound(xsIn);
    detachOutbound(xsOut);
  }
    
  // parse the arguments to get the conncetion url to inbound db
  public static String parseXSInArguments(String args[])
  {
    String trace, pref;
    String orasid, host, port;
    
    if (args.length != 12)
    {
      printUsage();
      System.exit(0);
    }

    orasid = args[0];
    host = args[1];
    port = args[2];
    xsinusername = args[3];
    xsinpasswd = args[4];
    xsinName = args[5];
    
    System.out.println("xsin_host = "+host);
    System.out.println("xsin_port = "+port);
    System.out.println("xsin_ora_sid = "+orasid);

    String in_url = "jdbc:oracle:oci:@"+host+":"+port+":"+orasid;
    System.out.println("xsin connection url: "+ in_url);

    return in_url;
  }

  // parse the arguments to get the conncetion url to outbound db
  public static String parseXSOutArguments(String args[])
  {
    String trace, pref;
    String orasid, host, port;
    
    if (args.length != 12)
    {
      printUsage();
      System.exit(0);
    }

    orasid = args[6];
    host = args[7];
    port = args[8];
    xsoutusername = args[9];
    xsoutpasswd = args[10];
    xsoutName = args[11];
    
    
    System.out.println("xsout_host = "+host);
    System.out.println("xsout_port = "+port);
    System.out.println("xsout_ora_sid = "+orasid);

    String out_url = "jdbc:oracle:oci:@"+host+":"+port+":"+orasid;
    System.out.println("xsout connection url: "+ out_url);

    return out_url;
  }

  // print out sample program usage message
  public static void printUsage()
  {
    System.out.println("");      
    System.out.println("Usage: java xio "+"<xsin_oraclesid> " + "<xsin_host> "
                                         + "<xsin_port> ");
    System.out.println("                "+"<xsin_username> " + "<xsin_passwd> "
                                         + "<xsin_servername> ");
    System.out.println("                "+"<xsout_oraclesid> " + "<xsout_host> "
                                         + "<xsout_port> ");
    System.out.println("                "+"<xsout_username> " + "<xsout_passwd> "
                                         + "<xsout_servername> ");
  }

  // create a connection to an Oracle Database
  public static Connection createConnection(String url, 
                                            String username, 
                                            String passwd)
  {
    try
    {
      DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
      return DriverManager.getConnection(url, username, passwd);
    }
    catch(Exception e)
    {
      System.out.println("fail to establish DB connection to: " +url);
      e.printStackTrace();
      return null;
    }
  }

  // attach to the XStream Inbound Server
  public static XStreamIn attachInbound(Connection in_conn)
  {
    XStreamIn xsIn = null;
    try
    {
      xsIn = XStreamIn.attach((OracleConnection)in_conn, xsinName,
                              "XSDEMOINCLIENT" , XStreamIn.DEFAULT_MODE);

      // use last position to decide where should we start sending LCRs  
      lastPosition = xsIn.getLastPosition();
      System.out.println("Attached to inbound server:"+xsinName);
      System.out.print("Inbound Server Last Position is: ");
      if (null == lastPosition)
      {
        System.out.println("null");
      }
      else
      {
        printHex(lastPosition);
      }
      return xsIn;
    }
    catch(Exception e)
    {
      System.out.println("cannot attach to inbound server: "+xsinName);
      System.out.println(e.getMessage());
      e.printStackTrace();
      return null;
    }        
  }

  // attach to the XStream Outbound Server    
  public static XStreamOut attachOutbound(Connection out_conn)
  {
    XStreamOut xsOut = null;

    try
    {
      // when attach to an outbound server, client needs to tell outbound
      // server the last position.
      xsOut = XStreamOut.attach((OracleConnection)out_conn, xsoutName,
                                lastPosition, XStreamOut.DEFAULT_MODE);
      System.out.println("Attached to outbound server:"+xsoutName);
      System.out.print("Last Position is: ");  
      if (lastPosition != null)
      {
        printHex(lastPosition);
      }
      else
      {
        System.out.println("NULL");
      }
      return xsOut;
    }
    catch(Exception e)
    {
      System.out.println("cannot attach to outbound server: "+xsoutName);
      System.out.println(e.getMessage());
      e.printStackTrace();
      return null;
    } 
  }

  // detach from the XStream Inbound Server
  public static void detachInbound(XStreamIn xsIn)
  {
    byte[] processedLowPosition = null;
    try
    {
      processedLowPosition = xsIn.detach(XStreamIn.DEFAULT_MODE);
      System.out.print("Inbound server processed low Position is: ");
      if (processedLowPosition != null)
      {
        printHex(processedLowPosition);
      }
      else
      {
        System.out.println("NULL");
      }
    }
    catch(Exception e)
    {
      System.out.println("cannot detach from the inbound server: "+xsinName);
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
  }

  // detach from the XStream Outbound Server    
  public static void detachOutbound(XStreamOut xsOut)
  {
    try
    {
      xsOut.detach(XStreamOut.DEFAULT_MODE);
    }
    catch(Exception e)
    {
      System.out.println("cannot detach from the outbound server: "+xsoutName);
      System.out.println(e.getMessage());
      e.printStackTrace();
    }       
  }

  public static void get_lcrs(XStreamIn xsIn, XStreamOut xsOut)
  {
    if (null == xsIn) 
    {
      System.out.println("xstreamIn is null");
      System.exit(0);
    }
 
    if (null == xsOut)
    {
      System.out.println("xstreamOut is null");
      System.exit(0);
    }

    try
    {
      while(true) 
      {
        // receive an LCR from outbound server
        LCR alcr = xsOut.receiveLCR(XStreamOut.DEFAULT_MODE);

        if (xsOut.getBatchStatus() == XStreamOut.EXECUTING) // batch is active
        {
          assert alcr != null;
          // send the LCR to the inbound server
          xsIn.sendLCR(alcr, XStreamIn.DEFAULT_MODE);

          // also get chunk data for this LCR if any
          if (alcr instanceof RowLCR)
          {
            // receive chunk from outbound then send to inbound
            if (((RowLCR)alcr).hasChunkData())
            {
              ChunkColumnValue chunk = null; 
              do
              {
                chunk = xsOut.receiveChunk(XStreamOut.DEFAULT_MODE);
                xsIn.sendChunk(chunk, XStreamIn.DEFAULT_MODE);
              } while (!chunk.isEndOfRow());
            }
          }
          processedLowPosition = alcr.getPosition();
        }
        else  // batch is end 
        {
          assert alcr == null;
          // flush the network
          xsIn.flush(XStreamIn.DEFAULT_MODE);
          // get the processed_low_position from inbound server
          processedLowPosition = 
              xsIn.getProcessedLowWatermark();
          // update the processed_low_position at oubound server
          if (null != processedLowPosition)
            xsOut.setProcessedLowWatermark(processedLowPosition, 
                                           XStreamOut.DEFAULT_MODE);
        }
      }
    }
    catch(Exception e)
    {
      System.out.println("exception when processing LCRs");
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
  }

  public static void printHex(byte[] b) 
  {
    for (int i = 0; i < b.length; ++i) 
    {
      System.out.print(
        Integer.toHexString((b[i]&0xFF) | 0x100).substring(1,3));
    }
    System.out.println("");
  }    
}
