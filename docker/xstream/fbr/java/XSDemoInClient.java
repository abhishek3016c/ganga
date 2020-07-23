/* $Header: rdbms/demo/xstream/fbr/java/XSDemoInClient.java /main/1 2009/02/27 15:57:39 tianli Exp $ */

/* Copyright (c) 2009, Oracle and/or its affiliates.All rights reserved. */

/*
   DESCRIPTION
    XStream File Based Replication Demo inbound client

   PRIVATE CLASSES
    <list of private classes defined - with one-line descriptions>

   NOTES
    <other useful comments, qualifications, etc.>

   MODIFIED    (MM/DD/YY)
    tianli      02/20/09 - Creation
 */

/**
 *  @version $Header: rdbms/demo/xstream/fbr/java/XSDemoInClient.java /main/1 2009/02/27 15:57:39 tianli Exp $
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
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.DataInputStream;
import java.io.RandomAccessFile;

public class XSDemoInClient
{
  // default values and constants
  public static String prefix = "xsdemo_archive";
  public static final String xsinProgressFile = "xsdemo_in_progress";
  public static final String xsinName = "DEMOIN";
  public static final String xsinusername = "stradm"; 
  public static final String xsinpasswd = "stradm";
  public static boolean tracing = false;
    
  public static void main(String args[])
  {
    //parse arguments, and check progress
    String in_url = parseArguments(args);    
    long suffix = getProgress();
    
    // start the loop
    System.out.println("Starting XSDemo InClient ...");
    startXSIn(in_url, prefix, suffix, xsinName, xsinusername, xsinpasswd);
    System.out.println("XSDemo InClient finished");    
  }

  public static void printUsage()
  {
    System.out.println("");      
    System.out.println("Usage: java [-DPrefix=<prefix>]");
    System.out.println("            [-DTracing=<true|false>]");
    System.out.println("            XSDemoInClient " +"<oracle_sid> " + 
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
    String trace, pref, defaultsid;
    String orasid, host, port;
    
    if (args.length < 3)
    {
      printUsage();
      System.exit(0);
    }

    // set prefix, by default, prefix is set to xsdemo_archive
    pref = System.getProperty("Prefix");
    if (pref != null)
    {
      prefix = pref;
    }

    // set tracing, by default, tracing is off
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

    // check if we need to manipulate the ora_sid arguments
    // which is for Oracle internal testing only
    defaultsid = System.getProperty("DefaultSID");
    if (defaultsid != null)
    {
      orasid = args[0]+"2";
    }
    else
    {
      orasid = args[0];
    }
      
    host = args[1];
    port = args[2];
    
    System.out.println("host = "+host);
    System.out.println("port = "+port);
    System.out.println("ora_sid = "+orasid);

    String in_url = "jdbc:oracle:oci:@"+host+":"+port+":"+orasid;
    System.out.println("xsin connection url: "+ in_url);

    return in_url;
  }

  public static void startXSIn(String url, String prefix, long suffix, 
                               String xsinName, String username, String passwd)
  {
    Connection in_conn;
    XStreamIn xsIn = null;
    int retry = 0;
    XSDemoLCRReader lcrReader = null;
    byte[] processedLowPosition;
    
    try
    {
      DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
      in_conn = DriverManager.getConnection(url, username, passwd);
    }
    catch(Exception e)
    {
      System.out.println("fail to establish database connection");
      e.printStackTrace();
      return;
    }

    while(true)
    {
      byte[] lastPosition;
        
      if (retry >=3)
      {
        System.out.println("exit after 3 attach retry");
        return;
      }
        
      try
      {
        xsIn = XStreamIn.attach((OracleConnection)in_conn, xsinName,
                                "XSDEMOINCLIENT" , XStreamIn.DEFAULT_MODE);
        // use last position to decide where should we start sending LCRs  
        lastPosition = xsIn.getLastPosition();
        System.out.println("Attached to inbound server:"+xsinName);
        System.out.println("Inbound Server Last Position is:");
        if (null == lastPosition)
        {
          System.out.println("null");
        }
        else
        {
          printHex(lastPosition);
        }
      }
      catch(Exception e)
      {
        retry++;
        System.out.println("cannot attach to inbound server: "+xsinName);
        System.out.println(e.getMessage());
        continue;
      }
      //attach succeed, clear retry count
      retry = 0;
      long lcrCount = 0;      

      // initialize LCRReader
      try
      {
        // need to decide where should we starting sending lcr
        suffix = checkProgress(xsIn, lastPosition, prefix, suffix) + 1;

        lcrReader = getLCRReader(xsIn, prefix,suffix);
        lcrReader.setTracing(tracing);
        byte[] pos = lcrReader.getProcessedLowPosition();
        System.out.println("processed low position in log file "+
                           lcrReader.getFileName());
        printHex(pos);
        System.out.println("started reading LCRs from "+
                           lcrReader.getFileName());
      }
      catch (StreamsException se)
      {
        // this exception is thrown by xsIn.flush, connection is broken
        // has to exit the program.
        System.out.println(se.getMessage());
        se.printStackTrace();
        break;
      }
      catch (Exception e)
      {
         System.out.println("fail to initialize LCRReader");
         System.out.println(e.getMessage());
         e.printStackTrace();
         break;
      }

      try
      {
        while(true) 
        {
          LCR alcr = lcrReader.readLCR();

          if (null != alcr)
          {
            int status = xsIn.sendLCR(alcr, XStreamIn.DEFAULT_MODE);

            if (alcr instanceof RowLCR)
            {
              // process chunk using sendChunk() API
              if (((RowLCR)alcr).hasChunkData())
              {
                while(true)
                {
                  ChunkColumnValue chunk = lcrReader.readChunk();
                  assert chunk != null;
                  xsIn.sendChunk(chunk, XStreamIn.DEFAULT_MODE);

                  if (chunk.isEndOfRow())
                  {
                    break;
                  }
                }
              }
            }
          }
          else // batch is end(end of current log file), switch to next log file
          {
            xsIn.flush(XStreamIn.DEFAULT_MODE);              
            lcrReader.close();
            updateProgress(suffix);
            System.out.println("finished reading LCRs from "+
                               lcrReader.getFileName());
             
            suffix++;
            lcrReader = getLCRReader(xsIn, prefix, suffix);
            lcrReader.setTracing(tracing);
            System.out.println("started reading LCRs from "+
                               lcrReader.getFileName());
          }
        }
      }
      catch(StreamsException e)
      {  // if StreamsException, we will try to detach and re-attach
        System.out.println("StreamsException while sending LCRs to server:"
                           +xsinName);
        System.out.println(e.getMessage());
        e.printStackTrace();
      }
      catch(Exception e)
      {
        System.out.println("exception while sending LCRs");
        System.out.println(e.getMessage());
        e.printStackTrace();
        break;
      }

      // try to close log file, if file is already closed, this is a no-op
      try
      {
        lcrReader.close();
      }
      catch (IOException e)
      {
        System.out.println("IO Exception while closing the LCRReader"+
                           lcrReader.getFileName());
        e.printStackTrace();
      }

      try
      {
        processedLowPosition = xsIn.detach(XStreamIn.DEFAULT_MODE);
        xsIn = null;
      }
      catch(Exception e)
      {
        System.out.println("cannot detach from inbound server: "+xsinName);
        System.out.println(e.getMessage());
        return;
      }
    }

    // try to cleanup the conncetion if out of the main loop, this has
    // to be an error case
    try
    {
      lcrReader.close();
      if (xsIn != null)
      {
        xsIn.detach(XStreamIn.DEFAULT_MODE);
      }
    }
    catch(Exception e)
    {
      System.out.println("cannot cleanup the resource before exit");
      System.out.println(e.getMessage());
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

  private static XSDemoLCRReader getLCRReader(XStreamIn xsIn, String prefix, 
                                              long suffix)
    throws StreamsException
  {
    XSDemoLCRReader reader = null;
    String filename = prefix+"_"+suffix+".fbr";
    while (true)
    {
      File f = new File(filename);
      if (f.exists())
      {
        try
        {
          RandomAccessFile raf = new RandomAccessFile(filename,"r");
          int poslen = raf.readInt();
          raf.close();
          if (poslen != 0)
          {
            reader =  new XSDemoLCRReader(prefix, suffix);
            break;
          }
          else
          {
            System.out.println("waiting for archive log: "+filename+
                                " to be completed");
            Thread.sleep(30000);
            
            // check the xstream connection. If it is broken, this will
            // throws StreamsException
            xsIn.flush(XStreamIn.DEFAULT_MODE);
          }
          
        }
        catch(StreamsException se)
        {
          // thrown by xsIn.flush(), just throw it out
          throw se;
        }
        catch(Exception e)
        {
          System.out.println(e.getMessage()); 
          e.printStackTrace();
          System.out.println("retry checking log: "+filename);
        }
      }
      else
      {
        System.out.println("waiting for archive log: "+filename);
        // sleep 30 secs if the file doesn't exist
        try
        {
          Thread.sleep(30000);
        }
        catch(Exception e)
        {
          System.out.println(e.getMessage()); 
          e.printStackTrace();
        }
        // check the xstream connection. If it is broken, it will
        // throws StreamsException
        xsIn.flush(XStreamIn.DEFAULT_MODE);
      }
    }
    return reader;
  }

  private static void updateProgress(long suffix)
  {
    try
    {
      RandomAccessFile raf = new RandomAccessFile(xsinProgressFile,"rws");
      raf.writeLong(suffix);
      raf.close();
    }
    catch(Exception e)
    {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
    
  }

  private static long getProgress()
  {
    File f = new File(xsinProgressFile);
    long suffix = 0;
    
    if (f.exists())
    {
      try
      {
        RandomAccessFile raf = new RandomAccessFile(xsinProgressFile,"r");
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

  private static int comparePosition(byte[] lhs, byte[] rhs)
  {
    int cmp_len;
    int result = 0;
    
    cmp_len = (lhs.length < rhs.length) ? lhs.length : rhs.length;
    for (int i =0; i< cmp_len; i++)
    {
      if (lhs[i] > rhs[i])
      {
        result = 1;
        break;
      }
      else if (lhs[i] < rhs[i])
      {
        result = -1;
        break;
      }
    }

    if (0 == result)
      return lhs.length - rhs.length;
    else
      return result;
  }

  // go through (backward) log files and check the lastest log file that
  // has processed low position 
  // start sending LCRs
  private static long checkProgress(XStreamIn xsIn, byte[] lwm, 
                                    String prefix, long suffix)
    throws StreamsException
  {
    XSDemoLCRReader reader;
      
    while (suffix > 1)
    {
      reader = getLCRReader(xsIn, prefix, suffix);
      byte[] pos = reader.getProcessedLowPosition();
      if (tracing)
      {
        System.out.println("processed low position in "+
                           getArchiveLogName(prefix, suffix)+" is: ");
        printHex(lwm);
                
      }
      
      int result = comparePosition(pos, lwm);
      
      if (result <= 0) // processedLowPosition in the log <= lwm
      {
        break;
      }
      
      suffix--;
    }

    return suffix;
  }

  private static String getArchiveLogName(String prefix, long suffix)
  {
    return prefix+"_"+suffix+".fbr";
  }
        
}
