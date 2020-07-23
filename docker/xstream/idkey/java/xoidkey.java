/* $Header: rdbms/demo/xstream/idkey/java/xoidkey.java /main/1 2010/07/28 21:31:47 yurxu Exp $ */

/* Copyright (c) 2010, Oracle and/or its affiliates. All rights reserved. */

/*
   NAME   
      xoidkey.java - XStream Out Java API Test

  DESCRIPTION
   This program attaches to an XStream outbound server and waits for
   transactions from that server. For each ID key LCR received, it queries
   the source database with the rowid in that LCR.
 
   This program waits indefinitely for transactions from the outbound server.
   Hit control-C to interrupt the program.

   environment setup: source demo/xstream/idkey/java/xsdemo_env.sh
   compilation: javac xoidkey.java
   Usage: java xoidkey <xsout_oraclesid> <xsout_host> <xsout_port>
                       <ob_usrname> <ob_passwd> <xsout_servername>
                       <db_usrname> <db_passwd>

   E.T.,  java xoidkey x222 dadvmh0062 17746 stradm stradm xout oe oe

   MODIFIED    (MM/DD/YY)
    yurxu       06/22/10 - Creation
 */

/**
 *  @version $Header: rdbms/demo/xstream/idkey/java/xoidkey.java /main/1 2010/07/28 21:31:47 yurxu Exp $
 *  @author  yurxu  
 *  @since   release specific (what release of product did this appear in)
 */

import oracle.streams.*;
import oracle.jdbc.internal.OracleConnection;
import oracle.jdbc.*;
import oracle.sql.*;
import java.sql.*;
import java.util.*;
import java.text.*;

public class xoidkey
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
  // the count of lcrs received
  public static int lcrCount;
  // for database connection
  public static String dbusername = null;
  public static String dbpasswd = null;
  public static Connection db_conn = null;
  public static String rowID = null;

    
  public static void main(String args[])
  {
    // get connection url to outbound server
    out_url = parseXSOutArguments(args);    

    // create connection to outbound server
    out_conn = createConnection(out_url, xsoutusername, xsoutpasswd);

    // create connection to source database
    db_conn = createConnection(out_url, dbusername, dbpasswd);

    // attach to outbound server
    xsOut = attachOutbound(out_conn);
    
    // main loop to get lcrs 
    get_lcrs(xsOut);
    
    // detach from outbound server
    detachOutbound(xsOut);
  }

  // parse the arguments to get the conncetion url to outbound db
  public static String parseXSOutArguments(String args[])
  {
    String trace, pref;
    String orasid, host, port;
    
    if (args.length != 8)
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
    dbusername = args[6];
    dbpasswd = args[7];
    
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
    System.out.println("Usage: java xoidkey "+"<xsout_oraclesid> " 
                                         + "<xsout_host> "
                                         + "<xsout_port> ");
    System.out.println("                "+" <xsout_username> " 
                                         + "<xsout_passwd> "
                                         + "<xsout_servername> ");
    System.out.println("                "+" <db_username> " 
                                         + "<db_passwd> ");
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

  // getRowID - Get RowID of this ID Key LCR
  public static void getRowID (LCR alcr)
  {
    // print out RowID
    try
    {
      Object row_id = alcr.getAttribute("ROW_ID");
      if (row_id != null)
        System.out.println("ROW ID: " + ((CHAR)row_id).toString());
      rowID = (((CHAR)row_id).toString());
    }
    catch (Exception e)
    {
      printString("ROW_ID get failed: " + e.getMessage());
    }
  }

  // Handle RowID of this ID Key LCR.
  // Get RowID of this ID Key LCR, then query source database with the RowID.
  public static void handleIDKeyLCR (LCR alcr)
  {
    // get rowid from LCR
    getRowID (alcr);

    // query the source DB with ROW ID
    // and print the result
    String qryStr = "select count(*) from customers " +
                    " where rowid = " + "'" + rowID + "'";
    System.out.println("query statment ="+ qryStr);

    try
    {            
      Statement stmt = db_conn.createStatement();
      ResultSet rset = stmt.executeQuery(qryStr);
      ResultSetMetaData rsMeta = rset.getMetaData();
      int colCount = rsMeta.getColumnCount();
      DecimalFormat m_df = new DecimalFormat("##.####");

      while(rset.next()) 
      {
        for (int iCol = 1; iCol <= colCount; iCol++)
        {     
          Object obj = rset.getObject(iCol);
          String colContent = null;
          if(obj instanceof java.lang.Number) 
          {
            colContent = m_df.format(obj);
            System.out.println("result " + colContent);
          }
        }        
      }
    }
    catch(Exception e)
    {
       System.out.println("fail to query Row ID ");
       System.out.println(e.getMessage());
    }
  }

  // detach from the XStream Outbound Server    
  public static void printLCR(LCR alcr)
  {
     lcrCount++;
     System.out.println("Streams: -- printing "+"["+
                        (lcrCount)+"]"+" LCR --");
     System.out.println("");
     // if it is an ID Key LCR
     if (!((DefaultRowLCR)alcr).hasIDKeyColumnsOnly())
       return;
     System.out.println(" ID Key LCR ");

     printString("command type: "+ alcr.getCommandType());
     printString("srcdb name: "+   alcr.getSourceDatabaseName());
     printString("object owner: "+ alcr.getObjectOwner());
     printString("object name: "+ alcr.getObjectName());
     printString("transaction id: "+ alcr.getTransactionId());
     printString("source time: "+ (alcr.getSourceTime()).stringValue());
     byte[] raw = alcr.getPosition();

     // print out LCR SCN
     try
     {
       if (null == raw )
       {
          printString("position is null");  
       }
       NUMBER pos = XStreamUtility.getSCNFromPosition(raw);
       printString("postion: "+ pos.intValue());
     }
     catch (Exception e)
     {
       printString("get position failed: "+ e.getMessage());
       printString("raw position is: ");
       printBinary(raw);
       printHex(raw);
     }

     // print out LCR Commit SCN
     try
     {
       if (null == raw )
       {
         printString("position is null");  
       }
       NUMBER commitPos = XStreamUtility.getCommitSCNFromPosition(raw);      
       printString("commit postion: "+ commitPos.intValue());
     }
     catch (Exception e)
     {
       printString("get commit postion failed: "+ e.getMessage());
       printString("raw position is: ");
       printBinary(raw);
       printHex(raw);
     } 

     // print out tag
     System.out.print("LCR tag: ");
     byte[] tag = alcr.getTag();
     if (null == tag)
       printString("tag is null.");
     else
       printHex(alcr.getTag());

     // print out collumn information
     if (alcr instanceof RowLCR)
     {
       if ((alcr.getCommandType()).compareTo(RowLCR.INSERT) == 0 ||
           (alcr.getCommandType()).compareTo(RowLCR.UPDATE) == 0 ||
           (alcr.getCommandType()).compareTo(RowLCR.LOB_WRITE) == 0 ||
           (alcr.getCommandType()).compareTo(RowLCR.LOB_ERASE) == 0 ||
           (alcr.getCommandType()).compareTo(RowLCR.LOB_TRIM) == 0)
       {          
         printString("------ printing new column list ------");          
         ColumnValue[] collist = ((RowLCR)alcr).getNewValues();
         printColumnList(collist);
       }
 
       if ((alcr.getCommandType()).compareTo(RowLCR.DELETE) == 0 ||
           (alcr.getCommandType()).compareTo(RowLCR.UPDATE) == 0 )     
       {
         printString("------ printing old column list ------");          
         ColumnValue[] collist = ((RowLCR)alcr).getOldValues();
         printColumnList(collist);
       }
     }

     // handle RowID of this ID Key LCR
     handleIDKeyLCR (alcr);    
  }

  // a loop to process LCRs
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

          // print ID Key LCRs
          printLCR(alcr);

          // also get chunk data for this LCR if any
          if (alcr instanceof RowLCR)
          {
            // receive chunk from outbound
            if (((RowLCR)alcr).hasChunkData())
            {
              ChunkColumnValue chunk = null; 
              printString("This RowLCR contains chunk data");
              do
              {
                chunk = xsOut.receiveChunk(XStreamOut.DEFAULT_MODE);

                // print chunk here
                // print
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

  public static void printColumnList(ColumnValue[] collist)
  {
    printString("total -"+collist.length + "- columns");
    for (int i = 0; i < collist.length; i++)
    {
      printString("column ["+i+"]"+" name: "+collist[i].getColumnName());
      if (collist[i] instanceof ChunkColumnValue)
      {
        printString("column [" + i + "] contains chunk data");
        if (null != ((ChunkColumnValue)collist[i]).getChunkOffset())
        {                
           printString("column [" + i + "] offset is: "+
             (((ChunkColumnValue)collist[i]).getChunkOffset()).toString());
        }
        
        if (null != ((ChunkColumnValue)collist[i]).getChunkOperationSize())
        {                
          printString("column [" + i + "] size is: "+
            (((ChunkColumnValue)collist[i]).getChunkOperationSize()).toString());
        }
 
        printString("column [" + i + "] chunktype is:"+
          ((ChunkColumnValue)collist[i]).getChunkType());  
        printString("column [" + i + "] datatype is:"+
          ((ChunkColumnValue)collist[i]).getColumnDataType());

        if ( ((ChunkColumnValue)collist[i]).isLastChunk())
        {
          printString("column [" + i + "] has last chunk data");            
        }
        if ( ((ChunkColumnValue)collist[i]).isEmptyChunk())
        {
          printString("column [" + i + "] has empty chunk data");  
        }
      }

      if (collist[i].getTDEFlag())
      {              
        printString("column [" + i + "] has TDE enabled");
      }

      Datum coldata = collist[i].getColumnData();
      if (coldata == null )
      {
        printString("column [" + i + "] is null");
      }
      else if (coldata instanceof NUMBER)
      {
        try
        {
          printString("column ["+i+"]"+" number: "+ 
                      ((NUMBER)coldata).stringValue());                   
        }
        catch (Exception e)
        {
          printString("column number error: "+ e.getMessage());
        }
      }
      else if(coldata instanceof CHAR)
      {
        try
        {
          printString("coldata["+i+"]"+" stringValue(): "+
                      ((CHAR)coldata).stringValue());
          printString("coldata["+i+"]"+" char set: "+
                      ((CHAR)coldata).oracleId());
        }
        catch (Exception e)
        {
          printString("column char error: "+e.getMessage());
        }
      }
      else if (coldata instanceof DATE)
      {
        try
        {
          SimpleDateFormat formatter = new SimpleDateFormat("MMM-dd-yyyy");
          printString("column ["+i+"]"+" date: "+
                      formatter.format(((DATE)coldata).dateValue()));
        }
        catch (Exception e)
        {
          printString("column date error: "+ e.getMessage());
        }
      }
      else if (coldata instanceof TIMESTAMP)
      {
        try
        {
          Timestamp my_ts = ((TIMESTAMP)coldata).timestampValue();
          if (my_ts.getNanos() > 0)
          {
            SimpleDateFormat formatter = 
              new SimpleDateFormat("yyyy/MMM/dd hh:mm:ss:S");
            printString("column ["+i+"]"+" timestamp: "+
                        formatter.format(my_ts));
          }
          else
          {
            SimpleDateFormat formatter =
              new SimpleDateFormat("yyyy/MMM/dd hh:mm:ss");
            printString("column ["+i+"]"+" timestamp: "+
                        formatter.format(my_ts));
          }
        }
        catch (Exception e)
        {
          printString("column date error: "+ e.getMessage());
        }
      }
      else if (coldata instanceof RAW)
      {
        try
        {
          printString("column ["+i+"]"+" raw: "+
                      ((RAW)coldata).stringValue());
        }
        catch (Exception e)
        {
          printString("column date error: "+ e.getMessage());
        }
      }
      else if (coldata instanceof INTERVALYM)
      {
        try
        {
          printString("column ["+i+"]"+" intervalym: "+
                      ((INTERVALYM)coldata).stringValue());
        }
        catch (Exception e)
        {
          printString("column date error: "+ e.getMessage());
        }
      }
      else if (coldata instanceof INTERVALDS)
      {
        try
        {
          printString("column ["+i+"]"+" intervalds: "+
                      ((INTERVALDS)coldata).stringValue());
        }
        catch (Exception e)
        {
          printString("column date error: "+ e.getMessage());
        }
      }
      else if (coldata instanceof BINARY_FLOAT)
      {
        try
        {
          printString("column ["+i+"]"+" binary_float: "+
                      ((BINARY_FLOAT)coldata).stringValue());
        }
        catch (Exception e)
        {
           printString("column date error: "+ e.getMessage());
        }
      }
      else if (coldata instanceof BINARY_DOUBLE)
      {
        try
        {
          printString("column ["+i+"]"+" binary_double: "+
                      ((BINARY_DOUBLE)coldata).stringValue());
        }
        catch (Exception e)
        {
          printString("column date error: "+ e.getMessage());
        }
      }
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
  public static void printString(String str)
  {
    System.out.println(str);
    System.out.flush();
  }
  public static void printBinary(byte[] b) 
  {
    for (int i = 0; i < b.length; ++i) 
    {
      System.out.print(Integer.toBinaryString(b[i]));
      System.out.print("-");
    }
    System.out.println("");
  }   
}
