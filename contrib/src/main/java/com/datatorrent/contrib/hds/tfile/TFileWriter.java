package com.datatorrent.contrib.hds.tfile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.file.tfile.TFile.Writer;

import com.datatorrent.contrib.hds.HDSFileAccess.HDSFileWriter;

public final class TFileWriter implements HDSFileWriter
{
  private Writer writer;
  
  private FSDataOutputStream fsdos;
  
  public TFileWriter(FSDataOutputStream stream, int minBlockSize, String compressName, String comparator, Configuration conf) throws IOException
  {
    this.fsdos = stream;
    writer = new Writer(stream, minBlockSize, compressName, comparator, conf);
    
  }

  @Override
  public void close() throws IOException
  {
    writer.close();
    fsdos.close();
  }

  @Override
  public void append(byte[] key, byte[] value) throws IOException
  {
    writer.append(key, value);
  }

  @Override
  public int getFileSize() 
  {
    return fsdos.size();
  }

}
