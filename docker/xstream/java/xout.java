/* $Header: rdbms/demo/xstream/java/xout.java /main/1 2009/07/07 13:43:31 thoang Exp $ */

/* Copyright (c) 2009, Oracle and/or its affiliates. All rights reserved. */

/*
   NAME   
      xout.java - XStream Out Java API Test

   DESCRIPTION  
   This program first attaches to an XStream outbound server. Next it goes into 
   an infinite loop waiting for lcrs from the outbound server.  While transferring 
   data, it periodically updates the processed low position in the outbound server.
   
   This program waits indefinitely for transactions from the outbound server.
   Hit control-C to interrupt the program.

   If the program is restarted, the outbound server will start sending
   from the processed low position which was set during the previous run.
   
   environment setup: source demo/xstream/java/xsdemo_env.sh
   compilation: javac xout.java
   Usage: java xout <xsout_oraclesid> <xsout_host> <xsout_port>
                   <usrname> <passwd> <xsout_servername>

   MODIFIED    (MM/DD/YY)
    tianli      07/06/09 - Creation
 */

/**
 *  @version $Header: rdbms/demo/xstream/java/xout.java /main/1 2009/07/07 13:43:31 thoang Exp $
 *  @author  tianli  
 *  @since   release specific (what release of product did this appear in)
 */

import oracle.streams.*;
import oracle.jdbc.internal.OracleConnection;
import oracle.jdbc.*;
import oracle.sql.*;
import java.sql.*;
import java.util.*;

public class xout
{
  public static String xsinusername = null;
  public static String xsinpasswd = null;
  public static String xsinName = null;
  public static String xsoutusername = null;
  public static String xsoutpasswd = null;
  public static String xsoutName = null;
  public static String out_url = null;
  public static Connection out_conn = null;
  public static XStreamOut xsOut = null;
  public static byte[] lastPosition = null;
  public static byte[] processedLowPosition = null;
    
  public static void main(String args[])
  {
    // get connection url to inbound and outbound server
    out_url = parseXSOutArguments(args);    

    // create connection to inbound and outbound server
    out_conn = createConnection(out_url, xsoutusername, xsoutpasswd);

    // attach to inbound and outbound server
    xsOut = attachOutbound(out_conn);
    
    // main loop to get lcrs 
    get_lcrs(xsOut);
    
    // detach from inbound and outbound server
    detachOutbound(xsOut);
  }

  // parse the arguments to get the conncetion url to outbound db
  public static String parseXSOutArguments(String args[])
  {
    String trace, pref;
    String orasid, host, port;
    
    if (args.length != 6)
    {
      printUsage();
      System.exit(0);
    }

    orasid = args[0];
    host = args[1];
    port = args[2];
    xsoutusername = args[3];
    xsoutpasswd = args[4];
    xsoutName = args[5];
    
    
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
    System.out.println("Usage: java xout "+"<xsout_oraclesid> " + "<xsout_host> "
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

  public static void get_lcrs( XStreamOut xsOut)
  {
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

          // you can play with the LCR here

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
                // you can play with the chunk data here
              } while (!chunk.isEndOfRow());
            }
          }
          processedLowPosition = alcr.getPosition();
        }
        else  // batch is end 
        {
          assert alcr == null;
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
