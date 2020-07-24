/* $Header: rdbms/demo/xstream/fbr/java/XSDemoLCRReader.java /main/1 2009/02/27 15:57:39 tianli Exp $ */

/* Copyright (c) 2009, Oracle and/or its affiliates.All rights reserved. */

/*
   DESCRIPTION
    XSDemo reads LCRs from a log file

   PRIVATE CLASSES
    <list of private classes defined - with one-line descriptions>

   NOTES
    <other useful comments, qualifications, etc.>

   MODIFIED    (MM/DD/YY)
    tianli      02/20/09 - Creation
 */

/**
 *  @version $Header: rdbms/demo/xstream/fbr/java/XSDemoLCRReader.java /main/1 2009/02/27 15:57:39 tianli Exp $
 *  @author  tianli  
 *  @since   release specific (what release of product did this appear in)
 */

import oracle.streams.*;
import oracle.jdbc.internal.OracleConnection;
import oracle.jdbc.*;
import oracle.sql.*;
import java.sql.*;
import java.util.*;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;

public class XSDemoLCRReader
{
  private String prefix = null;
  private long suffix = 0;
  private DataInputStream dis = null;
  private FileInputStream fis = null;
  private BufferedInputStream bis = null;
  private static final String star = "*";
  private String filename= null;
  private boolean tracing = false;
  private byte[] processedLowPosition;
  public static final int headerSize = 70; // 4B leng + 64B position + 2B marker
    
  public XSDemoLCRReader(String prefix, long suffix)
    throws IOException, StreamsException
  {
    this.prefix = prefix;
    this.suffix = suffix;
    this.filename = prefix+"_"+suffix+".fbr";
    this.fis = new FileInputStream(this.filename);
    this.bis = new BufferedInputStream(this.fis);
    this.dis = new DataInputStream(this.bis);
    int poslen = this.dis.readInt();
    if (poslen == 0)
    {
      throw new StreamsException("invalid processed low position");
    }

    // read processed low position    
    this.processedLowPosition = new byte[poslen];
    this.dis.read(processedLowPosition);

    // read endmarker to ensure this position is valid
    char endmarker = this.dis.readChar();
    if (endmarker != 'Y')
    {
      throw new StreamsException("invalid endmarker");
    }
    
    // skip empty bytes
    byte[] tmpbytes = new byte[64-poslen];
    this.dis.read(tmpbytes);
  }
    
  public byte[] getProcessedLowPosition()
  {
    return this.processedLowPosition;
  }
    
  public void setTracing(boolean tracing)
  {
    this.tracing = tracing;
  }
    
  public String getFileName()
  {
    return this.filename;
  }
    
  public void close()
    throws IOException
  {
    this.fis.close();
  }
    
  public LCR readLCR()
      throws IOException, StreamsException
  {
    LCR alcr;

    String lcrtype = readString();
    if (tracing)
    {
      System.out.println("lcrtype field is "+lcrtype);
    }
    
    if (lcrtype.compareTo("ROWLCR") == 0)
    {
      if (tracing)
      {
        System.out.println("reading an RowLCR");
      }
      alcr = readRowLCR();
    }
    else if (lcrtype.compareTo("DDLLCR") == 0)
    {
      if (tracing)
      {
        System.out.println("reading an DDLLCR");
      }
      alcr = readDDLLCR();
    }
    else if (lcrtype.compareTo("EOF") == 0)
    {
      alcr = null;
    }
    else
    {
      throw new StreamsException("corruptted archive file: "+this.filename);
    }
              
    return alcr;
  }
  

  private RowLCR readRowLCR()
    throws IOException, StreamsException
  {
    if (tracing)
    {
      System.out.println("reading an RowLCR");
    }

    RowLCR rowlcr = new DefaultRowLCR();

    // read the header portion
    readLCRHeader(rowlcr);

    // read the lcr body
    if ((rowlcr.getCommandType()).compareTo(RowLCR.INSERT) == 0)
    {
      readInsert(rowlcr);
    }
    else if ((rowlcr.getCommandType()).compareTo(RowLCR.UPDATE) == 0)
    {
      readUpdate(rowlcr);
    }
    else if ((rowlcr.getCommandType()).compareTo(RowLCR.DELETE) == 0)
    {
      readDelete(rowlcr);
    }
    else if ((rowlcr.getCommandType()).compareTo(RowLCR.COMMIT) == 0)
    {
      readCommit(rowlcr);
    }
    else if ((rowlcr.getCommandType()).compareTo(RowLCR.LOB_WRITE) == 0)
    {
      readLobWrite(rowlcr);        
    }
    else if ((rowlcr.getCommandType()).compareTo(RowLCR.LOB_ERASE) == 0)
    {
      readLobErase(rowlcr);             
    }
    else if ((rowlcr.getCommandType()).compareTo(RowLCR.LOB_TRIM) == 0)
    {
      readLobTrim(rowlcr);             
    }

    return rowlcr;
  }
   
  private DDLLCR readDDLLCR()
    throws IOException
  {
    if (tracing)
    {
      System.out.println("reading an DDLLCR");
    }

    DDLLCR ddllcr = new DefaultDDLLCR();

    // read the header portion
    readLCRHeader(ddllcr);
    
    // read the lcr body
    ddllcr.setObjectType(readString()); 
    ddllcr.setDDLText(readString()); 
    ddllcr.setCurrentSchema(readString());
    ddllcr.setLogonUser(readString());
    ddllcr.setBaseTableOwner(readString());
    ddllcr.setBaseTableName(readString());
    return ddllcr;
  }

  private void readLCRHeader(LCR lcr)
    throws IOException
  {
    if (tracing)
    {
      System.out.println("reading LCR Header");
    }

    lcr.setCommandType(readString());
    lcr.setSourceDatabaseName(readString());
    lcr.setObjectOwner(readString());
    lcr.setObjectName(readString());
    lcr.setTransactionId(readString());
    lcr.setSourceTime(new oracle.sql.DATE(readBytes()));
    lcr.setPosition(readBytes());
    lcr.setTag(readBytes());
  }

  private void readInsert(RowLCR lcr)
    throws IOException, StreamsException
  {
    ColumnValue[] collist = null;
    if (tracing)
    {
      System.out.println("reading an INSERT");
    }
    // insert has only newvalue list
    collist = readColumnList();
    lcr.setNewValues(collist);

    // hasChunkData Flag
    if (readString().compareTo("Y") == 0)
    {
      lcr.setChunkDataFlag(true);
    }
    else
    {
      lcr.setChunkDataFlag(false);     
    }
  }

  private void readUpdate(RowLCR lcr)
    throws IOException, StreamsException
  {
    ColumnValue[] collist = null;
    if (tracing)
    {
      System.out.println("reading an UPDATE");
    }
    // update has both newvalue and oldvalue list
    collist = readColumnList();
    lcr.setNewValues(collist);

    collist = readColumnList();
    lcr.setOldValues(collist);

    // hasChunkData Flag
    if (readString().compareTo("Y") == 0)
    {
      lcr.setChunkDataFlag(true);
    }
    else
    {
      lcr.setChunkDataFlag(false);     
    }
  }

  private void readDelete(RowLCR lcr)
    throws IOException, StreamsException
  {
    ColumnValue[] collist = null;
    if (tracing)
    {
      System.out.println("reading an DELETE");
    }
    // delete has only oldvalue list
    collist = readColumnList();
    lcr.setOldValues(collist);    
  }

  private void readCommit(RowLCR lcr)
    throws IOException, StreamsException
  {
    if (tracing)
    {
      System.out.println("reading an Commit");
    }
    // nothing else for Commit except the header
  }

  private void readLobWrite(RowLCR lcr)
    throws IOException, StreamsException
  {
    ColumnValue[] collist = null;
    if (tracing)
    {
      System.out.println("reading an LOB_WRITE");
    }

    // lob write has only newvalue list
    collist = readColumnList();
    lcr.setNewValues(collist);
    lcr.setChunkDataFlag(true);    
  }

  private void readLobErase(RowLCR lcr)
    throws IOException, StreamsException
  {
    ColumnValue[] collist = null;
    if (tracing)
    {
      System.out.println("reading an LOB_ERASE");
    }
    // lob erase has only newvalue list
    collist = readColumnList();
    lcr.setNewValues(collist);  
    // lob erase doesn't have chunk data
  }

  private void readLobTrim(RowLCR lcr)
    throws IOException, StreamsException
  {
    ColumnValue[] collist = null;
    if (tracing)
    {
      System.out.println("reading an LOB_TRIM");
    }
    // lob trim has only newvalue list
    collist = readColumnList();
    lcr.setNewValues(collist);
    // lob trim doesn't have chunk data
  }
    
  public ChunkColumnValue readChunk()
    throws IOException, StreamsException
  {
    ChunkColumnValue chunk;
      
    if (tracing)
    {
      System.out.println("reading a chunk for this LCR");
    }
    // column name
    String columnName = readString();
    // column data type
    int columnDataType = readInt();
    // Column data
    Datum columnData = readColumnData(columnDataType);
      
    // column TDE flag
    boolean tdeFlag;
    if ((readString()).compareTo("Y") == 0)
    {
      tdeFlag = true;
    }
    else
    {
      tdeFlag = false;
    }

    // chunk type
    int chunkType = readInt();

    // chunk offset
    String offset = readString();
    BigInteger chunkOffset;        
    if (offset.compareTo("NULL") == 0)
    {
      chunkOffset = null;
    }
    else
    {                
      chunkOffset = new BigInteger(offset);
    }

    // chunk size   
    String csize = readString();
    BigInteger chunkSize;   
    if (csize.compareTo("NULL") == 0)
    {
      chunkSize = null;
    }
    else
    {                
      chunkSize = new BigInteger(csize);
    }

    chunk = new DefaultChunkColumnValue(columnName,
                                        columnData ,
                                        chunkType,
                                        chunkOffset,
                                        chunkSize);

    // chunk last chunk flag
    boolean isLastChunk;        
    if (readString().compareTo("Y") == 0)
    {
      isLastChunk = true;
    }
    else
    {
      isLastChunk = false;
    }
    chunk.setLastChunk(isLastChunk);
    
    // chunk empty chunk flag
    boolean isEmptyChunk;        
    if (readString().compareTo("Y") == 0)
    {
      isEmptyChunk = true;
    }
    else
    {
      isEmptyChunk = false;
    }
    chunk.setEmptyChunk(isEmptyChunk);

    // chunk end of row flag
    boolean isEndOfRow;        
    if (readString().compareTo("Y") == 0)
    {
      isEndOfRow = true;
    }
    else
    {
      isEndOfRow = false;
    }
    chunk.setEndOfRow(isEndOfRow);

    // xml charsetid
    if (chunk.getChunkType() == ChunkColumnValue.XMLTYPE)
    {
      chunk.setCharsetId(readInt());
    }        

    chunk.setTDEFlag(tdeFlag);

    return chunk;
  }

  private ColumnValue[] readColumnList()
      throws IOException, StreamsException
  {
    ColumnValue[] collist;
    if (tracing)
    {
      System.out.println("reading column list");
    }
      
    int size = readInt();
    collist = new ColumnValue[size];
    
    for (int i = 0; i < size; i++)
    {
      // column name
      String columnName = readString();
      // column data type
      int colDataType = readInt();
      // Column data
      Datum columnData = readColumnData(colDataType);

      // column TDE flag
      boolean tdeFlag;
      if (readString().compareTo("Y") == 0)
      {
        tdeFlag = true;
      }
      else
      {
        tdeFlag = false;
      }
      
      boolean chunkedColumn;
      if (readString().compareTo("Y") == 0)
      {
        chunkedColumn = true;
      }
      else
      {
        chunkedColumn = false;
      }

      if (chunkedColumn)
      {
        // chunk type
        int chunkType = readInt();

        // chunk offset
        String offset = readString();
        BigInteger chunkOffset;        
        if (offset.compareTo("NULL") == 0)
        {
          chunkOffset = null;
        }
        else
        {                
          chunkOffset = new BigInteger(offset);
        }

        // chunk size   
        String csize = readString();
        BigInteger chunkSize;   
        if (csize.compareTo("NULL") == 0)
        {
          chunkSize = null;
        }
        else
        {                
          chunkSize = new BigInteger(csize);
        }

        collist[i] = new DefaultChunkColumnValue(columnName,
                                                 columnData ,
                                                 chunkType,
                                                 chunkOffset,
                                                 chunkSize);

        // chunk last chunk flag
        boolean isLastChunk;        
        if (readString().compareTo("Y") == 0)
        {
          isLastChunk = true;
        }
        else
        {
          isLastChunk = false;
        }
        ((ChunkColumnValue)collist[i]).setLastChunk(isLastChunk);
        
         
        // chunk empty chunk flag
        boolean isEmptyChunk;        
        if (readString().compareTo("Y") == 0)
        {
          isEmptyChunk = true;
        }
        else
        {
          isEmptyChunk = false;
        }
        ((ChunkColumnValue)collist[i]).setEmptyChunk(isEmptyChunk);

        // chunk end of row flag
        boolean isEndOfRow;        
        if (readString().compareTo("Y") == 0)
        {
          isEndOfRow = true;
        }
        else
        {
          isEndOfRow = false;
        }
        ((ChunkColumnValue)collist[i]).setEndOfRow(isEndOfRow);

        // xml charsetid
        if (((ChunkColumnValue)collist[i]).getChunkType() == 
              ChunkColumnValue.XMLTYPE)
        {
          ((ChunkColumnValue)collist[i]).setCharsetId(readInt());
        }        
      }
      else
      {
        collist[i] = new DefaultColumnValue(columnName,
                                            columnData,
                                            colDataType);
      }
  
      // set tde flag
      collist[i].setTDEFlag(tdeFlag);
    } 

    return collist;
  }

  private Datum readColumnData(int dataType)
      throws IOException, StreamsException
  {
    Datum data = null;
    if (readString().compareTo("Y") != 0)
    {
      switch(dataType)
      {
        case ColumnValue.CHAR:
          CharacterSet charset = CharacterSet.make(readInt());
          try
          {    
            data = new oracle.sql.CHAR(readString(), charset);
          }
          catch(SQLException e)
          {
            throw new StreamsException(
              "fail to create an oracle.sql.CHAR object", e);
          }
          break;
        case ColumnValue.NUMBER:
          data = new oracle.sql.NUMBER(readBytes());
          break;
        case ColumnValue.DATE:
          data = new oracle.sql.DATE(readBytes());
          break;
        case ColumnValue.RAW:
          data = new oracle.sql.RAW(readBytes());
          break;
        case ColumnValue.TIMESTAMP:
          data = new oracle.sql.TIMESTAMP(readBytes());
          break;
        case ColumnValue.TIMESTAMPTZ:
          data = new oracle.sql.TIMESTAMPTZ(readBytes());
          break;
        case ColumnValue.TIMESTAMPLTZ:
          data = new oracle.sql.TIMESTAMPLTZ(readBytes());
          break;
        case ColumnValue.BINARY_FLOAT:
          data = new oracle.sql.BINARY_FLOAT(readBytes());
          break;
        case ColumnValue.BINARY_DOUBLE:
          data = new oracle.sql.BINARY_DOUBLE(readBytes());
          break;
        case ColumnValue.INTERVALYM:
          data = new oracle.sql.INTERVALYM(readBytes());
          break;
        case ColumnValue.INTERVALDS:
          data = new oracle.sql.INTERVALDS(readBytes());
          break;
      }
    }
    else
    {
      if (tracing)
      {
        System.out.println("read column data is null");
      }              
    }
    
    
    return data;
  }     
    
  private String readString()
    throws IOException
  {
    String str;

    // read isNull flag
    int s = this.dis.readInt();
    char[] chars = new char[s];
    for (int i=0; i<s; i++)
    {
      chars[i] = this.dis.readChar();
    }
    String isNull = new String(chars);

    if (isNull.compareTo("Y") !=0)
    {
      int size = this.dis.readInt();
      if (size != 0)
      {
        char[] chararray = new char[size];
        for (int i=0; i<size; i++)
        {
          chararray[i] = this.dis.readChar();
        }
        str = new String(chararray);
      }
      else
      {
        str = new String("");
      }
    }
    else
    {
      str = null;
    }
    
    if (tracing)
    {
      System.out.println("reading string:"+str);
    }    

    return str;
  }

  private byte[] readBytes()
    throws IOException
  {
    byte[] bytearray;

    if (readString().compareTo("Y") !=0)
    {
      int size = this.dis.readInt(); 
      if (size == 0)
      {
        bytearray = new byte[0];
      }
      else
      {
        bytearray = new byte[size];
        this.dis.read(bytearray);
      }
    }
    else
    {
      bytearray = null; 
    }
    
    if (tracing)
    {
      System.out.print("reading bytes:");
      if (bytearray == null)
      {
        System.out.println("NULL");
      }
      else
      {
        XSDemoInClient.printHex(bytearray);
      }
    }

    return bytearray;
  }

  private int readInt()
    throws IOException
  {
    int value = this.dis.readInt();
    if (tracing)
    {
      System.out.println("reading integer:"+value);
    }    
    return value;
  }
    
}
