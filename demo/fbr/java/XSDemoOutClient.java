/* $Header: rdbms/demo/xstream/fbr/java/XSDemoOutClient.java /main/1 2009/02/27 15:57:39 tianli Exp $ */

/* Copyright (c) 2009, Oracle and/or its affiliates.All rights reserved. */

/*
   DESCRIPTION
    XStream File Based Replication Demo outbound client

   PRIVATE CLASSES
    <list of private classes defined - with one-line descriptions>

   NOTES
    <other useful comments, qualifications, etc.>

   MODIFIED    (MM/DD/YY)
    tianli      02/20/09 - Creation
 */

/**
 *  @version $Header: rdbms/demo/xstream/fbr/java/XSDemoOutClient.java /main/1 2009/02/27 15:57:39 tianli Exp $
 *  @author  tianli  
 *  @since   release specific (what release of product did this appear in)
 */

import oracle.streams.*;
import oracle.jdbc.internal.OracleConnection;
import oracle.jdbc.*;
import oracle.sql.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.File;
import java.io.RandomAccessFile;

public class XSDemoOutClient
{
  // default values and constants
  public static String prefix = "xsdemo_archive";
  public static final String xsoutProgressFile = "xsdemo_out_progress";   
  public static final String xsoutName = "DEMOOUT";
  public static final String xsoutusername = "stradm"; 
  public static final String xsoutpasswd = "stradm";

  public static boolean tracing  = false; 
    
  public static void main(String args[])
  {
    // parse arguments, and check progress
    String out_url = parseArguments(args);
    long suffix = getProgress()+1;
    
    // start the main loop
    System.out.println("Starting XSDemo OutClient ...");
    startXSOut(out_url, prefix, suffix, xsoutName, xsoutusername, xsoutpasswd);
    System.out.println("XSDemo outclient terminated");
  }

  public static void printUsage()
  {
    System.out.println("");      
    System.out.println("Usage: java [-DPrefix=<prefix>]");
    System.out.println("            [-DTracing=<true|false>]");
    System.out.println("            XSDemoOutClient " +"<oracle_sid> " + 
                       "<host> "+ "<port> ");
    System.out.println("");     
    System.out.println("To specify the Prefix of log files:");
    System.out.println("  - use java system property: e.g."+
                       " -DPrefix=xsdemo_archive");
    System.out.println("To turn on Client Tracing:");
    System.out.println("  - use java system property: e.g. -DTracing=true");
  }

  public static String parseArguments(String args[])
  {
    String trace;

    if (args.length < 3)
    {
      printUsage();
      System.exit(0);
    }
      
    System.out.println("host = "+args[1]);
    System.out.println("port = "+args[2]);
    System.out.println("ora_sid = "+args[0]);

    String out_url = "jdbc:oracle:oci:@"+args[1]+":"+args[2]+":"+args[0];
    System.out.println("xsout connection url: "+out_url);

    // set prefix, by default, prefix is set to xsdemo_archive
    String pref = System.getProperty("Prefix");
    if (pref != null)
    {
      prefix = pref;
    }

    trace = System.getProperty("Tracing");
    if (trace != null)
    {
      try 
      {
        tracing = Boolean.parseBoolean(trace);
      } 
      catch(Exception e) 
      {
        System.out.println(e.getMessage());
      }
    }
        
    return out_url;
  }

  /**
   * This is the main loop for OutClient.
   *
   * @param url connection url used in a jdbc oci driver
   * @param prefix client side LCR log file prefix
   * @param suffix client side LCR log file sequence number. This is the 
   *               current sequence number we should use. 
   * @param xsoutName outbound server name
   * @param username logon user name  
   * @param passwd logon user password
   */  
  private static void startXSOut(String url, 
                                 String prefix, 
                                 long suffix,
                                 String xsoutName, 
                                 String username, 
                                 String passwd)
  {
    byte[] lastPosition = null;
    Connection out_conn = null;
    XStreamOut xsOut = null;
    int retry = 0;
    XSDemoLCRWriter lcrWriter = null;
    byte[] processed_low_position = null;
    
    // create a jdbc oci connection
    try
    {
      DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
      out_conn = DriverManager.getConnection(url, username, passwd);
    }
    catch(Exception e)
    {
      System.out.println("fail to establish database connection");
      e.printStackTrace();
      return;
    }

    // main loop
    while(true)
    {
      if (retry >=3)
      {
        System.out.println("exit after 3 attach retry");
        return;
      }

      // when it first starts, processed_low_position is null, this indicates
      // outbound server should send us LCRs from the beginning of the entire
      // LCR stream. upon restart or error recovery, it will retrieve the 
      // processed_low_position from log files. 
      if (suffix != 1)
      {
        processed_low_position = getProcessedLowPosition(prefix, suffix-1);
      }
        
      try
      {
        // when attach to an outbound server, client needs to tell outbound
        // server the last position.
        lastPosition = processed_low_position;
        xsOut = XStreamOut.attach((OracleConnection)out_conn, xsoutName,
                                  lastPosition, XStreamOut.DEFAULT_MODE);
        System.out.println("Attached to outbound server:"+xsoutName);
        System.out.println("Last Position is: ");  
        if (lastPosition != null)
        {
          printHex(lastPosition);
        }
        else
        {
          System.out.println("NULL");        
        }
      }
      catch(Exception e)
      {
        retry++;
        System.out.println("cannot attach to outbound server: "+xsoutName);
        System.out.println(e.getMessage());
        continue;
      }
 
      //attach succeed, clear retry count
      retry = 0;
      long lcrCount = 0;      
   
      // initialize LCRWriter
      try
      {      
        lcrWriter = new XSDemoLCRWriter(prefix, suffix);
        lcrWriter.setTracing(tracing);
      }
      catch(IOException e)
      {
        // if we cannot open the log files, have to exit
        System.out.println("exception when creating logfile");
        e.printStackTrace();
        break;
      }

      try
      {
        System.out.println("Writing to LCR log: "+lcrWriter.getFileName());
          
        while(true) 
        {
          // receive an LCR from outbound server
          LCR alcr = xsOut.receiveLCR(XStreamOut.DEFAULT_MODE);

          if (xsOut.getBatchStatus() == XStreamOut.EXECUTING) // batch is active
          {
            assert alcr != null;
            // writes this LCR to log file
            lcrWriter.writeLCR(alcr);              
            
            // also writes chunk data for this LCR if any
            if (alcr instanceof RowLCR)
            {
              // receive chunk using receiveChunk() API
              if (((RowLCR)alcr).hasChunkData())
              {
                ChunkColumnValue chunk;                
                do
                {
                  chunk = xsOut.receiveChunk(XStreamOut.DEFAULT_MODE);
                  lcrWriter.writeChunk(chunk);
                } while (!chunk.isEndOfRow());
              }
            }
            processed_low_position = alcr.getPosition();
            lcrCount++;
          }
          else // batch is end 
          {
            // make sure alcr is null when batch is finished
            assert alcr == null;

            // This means we have processed some LCRs, need to move to a new 
            // log file.
            if (lcrCount > 0)
            {
                System.out.println("total LCR in this log"+lcrCount);
              // write processed_low_position to the current log and close it
              lcrWriter.close(processed_low_position);

              System.out.println("Finished writing to archive log: "+
                                  lcrWriter.getFileName());    

              // update the progress of outclient
              updateProgress(suffix);
              
              // update the processed_low_position at oubound server
              xsOut.setProcessedLowWatermark(processed_low_position, 
                                             XStreamOut.DEFAULT_MODE);

              // proceed to the next log file
              suffix++;
              lcrWriter = new XSDemoLCRWriter(prefix, suffix);
              lcrWriter.setTracing(tracing);
              lcrCount = 0;
              System.out.println("Writing to LCR log: "+
                                 lcrWriter.getFileName());
            }
            else // system is idle, hasn't received any lcr during this batch
            {
              processed_low_position = xsOut.getFetchLowWatermark();
              // Right now we don't create new log file if it's idle. 
              // this can prevent capture purging the redo log at source
              // optimization: write processed_low_position into log file 
              // as well if it's idle for certain amount of time, and
              // call setProcessedLowWatermark().
            }
          }
        }
      }
      catch(StreamsException e)
      {  
        // if StreamsException, we will try to detach and re-attach
        System.out.println("StreamsException while receiving LCRs from server:"
                           +xsoutName);
        e.printStackTrace();
      }
      catch(Exception e)
      {
        System.out.println("exception while receiving LCRs");
        e.printStackTrace();
        break;
      }

      // try to close log file, if file is already closed, this is a no-op
      try
      {
        lcrWriter.close(null);
      }
      catch(IOException e)
      {
        System.out.println("IO Exception while closing LCRWriter"+
                           lcrWriter.getFileName());
        e.printStackTrace();
      }

      try
      {
        xsOut.detach(XStreamOut.DEFAULT_MODE);
        xsOut = null;
      }
      catch(Exception e)
      {
        System.out.println("cannot detach from outbound server: "+xsoutName);
        System.out.println(e.getMessage());
        return;
      }
    }

    // try to cleanup if out of the main loop, this has
    // to be an error case
    try
    {
      lcrWriter.close(null);
      if (xsOut != null)
      {
        xsOut.detach(XStreamOut.DEFAULT_MODE);
      }
    }
    catch(Exception e)
    {
      System.out.println("cannot cleanup the resource before exit");
      System.out.println(e.getMessage());
    }

  }

  // print out the HEX representation of the byte array
  public static void printHex(byte[] b) 
  {
    for (int i = 0; i < b.length; ++i) 
    {
      System.out.print(Integer.toHexString((b[i]&0xFF) | 0x100).substring(1,3));
    }
    System.out.println("");
  }   

  // write the log file suffix into progress file. This is used for restarting
  // the client.
  private static void updateProgress(long suffix)
  {
    try
    {
      RandomAccessFile raf = new RandomAccessFile(xsoutProgressFile,"rws");
      raf.writeLong(suffix);
      raf.close();
    }
    catch(Exception e)
    {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
    
  }

  // get the log file suffix from progress file. 
  private static long getProgress()
  {
    File f = new File(xsoutProgressFile);
    long suffix = 0;
    
    if (f.exists())
    {
      try
      {
        RandomAccessFile raf = new RandomAccessFile(xsoutProgressFile,"r");
        suffix = raf.readLong();
        raf.close();
      }
      catch(Exception e)
      {
        System.out.println(e.getMessage());
        e.printStackTrace();
      }   
    }
    else
    {
      suffix = 0;
    }
    return suffix;
  }

  // get the processedLowPosition in a log file. which is the first
  // 68 reserved bytes. (4 bytes length + 64 bytes preallocated position)
  private static byte[] getProcessedLowPosition(String prefix, long suffix)
  {
    byte[] pos = null;
      
    try
    {
      String filename = getArchiveLogName(prefix, suffix);
      RandomAccessFile raf = new RandomAccessFile(filename,"r");
      int size = raf.readInt();
      if (size == 0)
        return null;
      else
      {
        pos = new byte[size];
        raf.read(pos);
      }
      raf.close();
    }
    catch(Exception e)
    {
      System.out.println(e.getMessage());
      e.printStackTrace();            
    }
    
    return pos;
  }

  // get the name of the log file given the prefix and suffix
  private static String getArchiveLogName(String prefix, long suffix)
  {
    return prefix+"_"+suffix+".fbr";
  }
    
}
