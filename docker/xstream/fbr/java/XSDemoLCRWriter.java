/* $Header: rdbms/demo/xstream/fbr/java/XSDemoLCRWriter.java /main/1 2009/02/27 15:57:39 tianli Exp $ */

/* Copyright (c) 2009, Oracle and/or its affiliates.All rights reserved. */

/*
   DESCRIPTION
    XSDemo writes LCRs to a log file

   PRIVATE CLASSES
    <list of private classes defined - with one-line descriptions>

   NOTES
    <other useful comments, qualifications, etc.>

   MODIFIED    (MM/DD/YY)
    tianli      02/20/09 - Creation
 */

/**
 *  @version $Header: rdbms/demo/xstream/fbr/java/XSDemoLCRWriter.java /main/1 2009/02/27 15:57:39 tianli Exp $
 *  @author  tianli  
 *  @since   release specific (what release of product did this appear in)
 */

import oracle.streams.*;
import oracle.jdbc.internal.OracleConnection;
import oracle.jdbc.*;
import oracle.sql.*;
import java.sql.*;
import java.util.*;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

public class XSDemoLCRWriter
{
  private String prefix = null;
  private long suffix = 0;
  private DataOutputStream dos = null;
  private FileOutputStream fos = null;
  private BufferedOutputStream bos = null;
  private static final String star = "*";
  private String filename= null;
  private boolean tracing = false;
  private boolean closed = false;
  public static final int headerSize = 70; // 4B leng + 64B position + 2B marker
     
  public XSDemoLCRWriter(String prefix, long suffix)
    throws IOException
  {
    this.prefix = prefix;
    this.suffix = suffix;
    this.filename = prefix+"_"+suffix+".fbr";
    // create the lcr writer with specified filename
    // start writing from the begining of the file
    this.fos = new FileOutputStream(filename);
    this.bos = new BufferedOutputStream(fos);
    this.dos = new DataOutputStream(bos);
    // reserve 64 bytes for processed_low_position
    // amd 2 bytes for a endmarker character
    // first write the length of the processed_low_position
    // it is zero when we still writting this log file and
    // it will be updated once we finish writting into this log file
    this.dos.writeInt(0);
    byte[] reserve = new byte[66];
    this.dos.write(reserve, 0, 66);
    this.dos.flush();

    // the log file is open for write
    this.closed = false;
  }

  public void setTracing(boolean tracing)
  {
    this.tracing = tracing;
  }
    
  public String getFileName()
  {
    return this.filename;
  }    

  public void close(byte[] processed_low_position)
    throws IOException
  {
    if (this.closed)
    {
      return;
    }
 
    writeString("EOF"); 
    this.dos.flush();
    this.fos.close();
 
    if (processed_low_position != null)
    {
      RandomAccessFile raf = new RandomAccessFile(this.filename,"rws");
      raf.writeInt(processed_low_position.length);
      raf.write(processed_low_position, 0, processed_low_position.length);
      raf.writeChar('Y');
      raf.close();
    }

    this.closed = true;
  }

  public void writeLCR(LCR lcr)
    throws IOException
  {
    if (lcr instanceof RowLCR)
    {
      if (tracing)
      {
        System.out.println("writing an RowLCR");
      }
      writeString("ROWLCR");
      writeRowLCR((RowLCR)lcr);
    }
    else
    {
      if (tracing)
      {
        System.out.println("writing an DDLLCR");
      }
      writeString("DDLLCR");
      writeDDLLCR((DDLLCR)lcr);
    }
    this.dos.flush();
  }

  public void writeChunk(ChunkColumnValue chunk)
    throws IOException
  {
    if (tracing)
    {
      System.out.println("writing a chunk for this LCR");
    }
    // column name
    writeString(chunk.getColumnName());
    // column data type
    writeInt(chunk.getColumnDataType());
    // Column data
    writeColumnData(chunk.getColumnData());
      
    // column TDE flag
    if (chunk.getTDEFlag())
    {
      writeString("Y");
    }
    else
    {
      writeString("N");
    }

    // chunk type
    writeInt(((ChunkColumnValue)chunk).getChunkType());

    // chunk offset
    if (null != ((ChunkColumnValue)chunk).getChunkOffset())
    {                
      writeString((((ChunkColumnValue)chunk).getChunkOffset()).toString());
    }
    else
    {
      writeString("NULL");
    }
      
    // chunk size
    if (null != ((ChunkColumnValue)chunk).getChunkOperationSize())
    {                
      writeString((((ChunkColumnValue)chunk).getChunkOperationSize())
                  .toString());
    }
    else
    {
      writeString("NULL");
    }

    // chunk last chunk bit
    if (((ChunkColumnValue)chunk).isLastChunk())
    {
      writeString("Y");
    }
    else
    {
      writeString("N");
    }

    // chunk empty chunk bit
    if (((ChunkColumnValue)chunk).isEmptyChunk())
    {
      writeString("Y");
    }
    else
    {
      writeString("N");
    }

    // endoofrow
    if (((ChunkColumnValue)chunk).isEndOfRow())
    {
      writeString("Y");
    }
    else
    {
      writeString("N");
    }

    // XMLType charsetid
    if (((ChunkColumnValue)chunk).getChunkType() == 
        ChunkColumnValue.XMLTYPE)
    {
      writeInt(((ChunkColumnValue)chunk).getCharsetId());
    }

    this.dos.flush();
  }
    
  private void writeRowLCR(RowLCR lcr)
    throws IOException
  {
    writeLCRHeader(lcr);
    if (tracing)
    {
      System.out.println("writing RowLCR body");
    }

    if ((lcr.getCommandType()).compareTo(RowLCR.INSERT) == 0)
    {
      writeInsert(lcr);
    }
    else if ((lcr.getCommandType()).compareTo(RowLCR.UPDATE) == 0)
    {
      writeUpdate(lcr);
    }
    else if ((lcr.getCommandType()).compareTo(RowLCR.DELETE) == 0)
    {
      writeDelete(lcr);
    }
    else if ((lcr.getCommandType()).compareTo(RowLCR.COMMIT) == 0)
    {
      writeCommit(lcr);
    }
    else if ((lcr.getCommandType()).compareTo(RowLCR.LOB_WRITE) == 0)
    {
      writeLobWrite(lcr);        
    }
    else if ((lcr.getCommandType()).compareTo(RowLCR.LOB_ERASE) == 0)
    {
      writeLobErase(lcr);             
    }
    else if ((lcr.getCommandType()).compareTo(RowLCR.LOB_TRIM) == 0)
    {
      writeLobTrim(lcr);             
    }
  }

  private void writeDDLLCR(DDLLCR lcr)
    throws IOException
  {
    writeLCRHeader(lcr);
    if (tracing)
    {
      System.out.println("writing DDLLCR body");
    }

    writeString(((DDLLCR)lcr).getObjectType()); 
    writeString(((DDLLCR)lcr).getDDLText()); 
    writeString(((DDLLCR)lcr).getCurrentSchema());
    writeString(((DDLLCR)lcr).getLogonUser());
    writeString(((DDLLCR)lcr).getBaseTableOwner());
    writeString(((DDLLCR)lcr).getBaseTableName());
    // TODO: add edition name
  }
    
  private void writeLCRHeader(LCR lcr)
    throws IOException
  {
    if (tracing)
    {
      System.out.println("writing LCR header");
    }

    writeString(lcr.getCommandType());
    writeString(lcr.getSourceDatabaseName());
    writeString(lcr.getObjectOwner());
    writeString(lcr.getObjectName());
    writeString(lcr.getTransactionId());
    writeBytes((lcr.getSourceTime()).getBytes());
    writeBytes(lcr.getPosition());
    writeBytes(lcr.getTag());

    // TODO: add attributes and non first class LCR field
  }

  private void writeInsert(RowLCR lcr)
    throws IOException
  {
    ColumnValue[] collist = null;

    // For insert, only new column list
    collist = ((RowLCR)lcr).getNewValues();
    writeColumnList(collist);

    // write hasChunkData Flag
    if (lcr.hasChunkData())
    {
      writeString("Y");
    }
    else
    {
      writeString("N");
    }
  }
  
  private void writeUpdate(RowLCR lcr)
    throws IOException
  {
    ColumnValue[] collist = null;

    // For update, get new column list first
    collist = ((RowLCR)lcr).getNewValues();
    writeColumnList(collist);

    // get old column list then
    collist = ((RowLCR)lcr).getOldValues();
    writeColumnList(collist);

    // write hasChunkData Flag
    if (lcr.hasChunkData())
    {
      writeString("Y");
    }
    else
    {
      writeString("N");
    }
  }
    
  private void writeDelete(RowLCR lcr)
    throws IOException
  {
    ColumnValue[] collist = null;

    // For delete, get old column list only
    collist = ((RowLCR)lcr).getOldValues();
    writeColumnList(collist);                
  }

  private void writeCommit(RowLCR lcr)
    throws IOException
  {
    // For commit, nothing else other than the LCR header
  }

  private void writeLobWrite(RowLCR lcr)
    throws IOException
  {
    ColumnValue[] collist = null;

    assert lcr.hasChunkData();
    // For lob write, only new column list
    collist = ((RowLCR)lcr).getNewValues();
    writeColumnList(collist);
  }

  private void writeLobErase(RowLCR lcr)
    throws IOException
  {
    ColumnValue[] collist = null;

    // For lob erase, only new column list
    collist = ((RowLCR)lcr).getNewValues();
    writeColumnList(collist);
  }

  private void writeLobTrim(RowLCR lcr)
    throws IOException
  {
    ColumnValue[] collist = null;

    collist = ((RowLCR)lcr).getNewValues();
    writeColumnList(collist);
  }

  private void writeColumnList(ColumnValue[] collist)
    throws IOException
  {
    if (tracing)
    {
      System.out.println("writing column list");
    }

    writeInt(collist.length);
    for (int i = 0; i < collist.length; i++)
    {
      // column name
      writeString(collist[i].getColumnName());
      // column data type
      writeInt(collist[i].getColumnDataType());
      // Column data
      writeColumnData(collist[i].getColumnData());
      
      // column TDE flag
      if (collist[i].getTDEFlag())
      {
        writeString("Y");
      }
      else
      {
        writeString("N");
      }

      if (collist[i] instanceof ChunkColumnValue)
      {
        writeString("Y");
      }
      else // not chunked column
      {
        writeString("N");
      }

      if (collist[i] instanceof ChunkColumnValue)
      {
        // chunk type
        writeInt(((ChunkColumnValue)collist[i]).getChunkType());

        // chunk offset
        if (null != ((ChunkColumnValue)collist[i]).getChunkOffset())
        {                
          writeString((((ChunkColumnValue)collist[i]).getChunkOffset())
                        .toString());
        }
        else
        {
          writeString("NULL");
        }
      
        // chunk size
        if (null != ((ChunkColumnValue)collist[i]).getChunkOperationSize())
        {                
          writeString((((ChunkColumnValue)collist[i]).getChunkOperationSize())
                        .toString());
        }
        else
        {
          writeString("NULL");
        }

        // chunk last chunk bit
        if (((ChunkColumnValue)collist[i]).isLastChunk())
        {
          writeString("Y");
        }
        else
        {
          writeString("N");
        }

        // chunk empty chunk bit
        if (((ChunkColumnValue)collist[i]).isEmptyChunk())
        {
          writeString("Y");
        }
        else
        {
          writeString("N");
        }

        // endoofrow
        if (((ChunkColumnValue)collist[i]).isEndOfRow())
        {
          writeString("Y");
        }
        else
        {
          writeString("N");
        }

        // XMLType charsetid
        if (((ChunkColumnValue)collist[i]).getChunkType() == 
              ChunkColumnValue.XMLTYPE)
        {
          writeInt(((ChunkColumnValue)collist[i]).getCharsetId());
        }        
      }
    } 
  }

  private void writeColumnData(Datum data)
    throws IOException
  {
    if (tracing)
    {
      System.out.println("writing columnData:");
      if (data == null)
      {
        System.out.println("Datum data is null");
      }
      else
      {
        byte[] rawdata = data.shareBytes();
        System.out.print("rawdata buffer is:");
        if (rawdata == null)
        {
          System.out.println("NULL");
        }
        else
        {
          XSDemoOutClient.printHex(rawdata);
        }       
      }
    }

    if (data == null)
    {
      writeString("Y");
    }
    else
    {
      writeString("N");
      if (data instanceof oracle.sql.CHAR)
      {
        writeInt(((CHAR)data).oracleId());
        writeString(((CHAR)data).toString());
      }
      else
      {
        writeBytes(data.shareBytes());
      }
    }
  }

  private void writeString(String str)
    throws IOException
  {
    if (tracing)
    {
      System.out.println("writing string:"+str);
    }

    String isNull;
    if (str == null)
    {
      isNull = "Y";
      this.dos.writeInt(isNull.length());
      this.dos.writeChars(isNull);
    }
    else
    {
      isNull = "N";
      this.dos.writeInt(isNull.length());
      this.dos.writeChars(isNull);

      if (str.length() == 0)
      {
        this.dos.writeInt(0);
      }
      else
      {
        this.dos.writeInt(str.length());
        this.dos.writeChars(str);
      }
    }
  }
    
  private void writeBytes(byte[] value)
    throws IOException
  {
    if (tracing)
    {
      System.out.print("writing bytes:");
      if (value == null)
      {
        System.out.println("NULL");
      }
      else
      {
        XSDemoOutClient.printHex(value);
      }
    }    

    if (value == null)
    {
      writeString("Y");
    }
    else
    {
      writeString("N");
      if (value.length == 0)
      {
        this.dos.writeInt(0);
      }
      else
      {
        this.dos.writeInt(value.length);
        this.dos.write(value, 0, value.length);
      }
    }   
  }

  private void writeInt(int value)
    throws IOException
  {
    if (tracing)
    {
      System.out.println("writing integer:"+value);
    }    

    this.dos.writeInt(value);
  }
}
