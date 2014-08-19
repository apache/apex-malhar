/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hds.tfile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile;

import com.datatorrent.contrib.hds.HDSFileAccessFSImpl;

public class TFileImpl extends HDSFileAccessFSImpl
{
  private int minBlockSize = 64 * 1024;

  private String compressName = TFile.COMPRESSION_NONE;
  
  private String comparator = "memcmp";
  
  private int chunkSize = 1024 * 1024;
  
  private int inputBufferSize = 256 * 1024;
  
  private int outputBufferSize = 256 * 1024;
  
  @Override
  public HDSFileReader getReader(long bucketKey, String fileName) throws IOException
  {
    FSDataInputStream fsdis =  getInputStream(bucketKey, fileName);
    long fileLength = fs.getContentSummary(new Path(getBucketPath(bucketKey), fileName)).getLength();
    setupConfig(fs.getConf());
    return new TFileReader(fsdis, fileLength, fs.getConf());
  }
  
  
  private void setupConfig(Configuration conf)
  {
    conf.set("tfile.io.chunk.size", String.valueOf(chunkSize));
    conf.set("tfile.fs.input.buffer.size", String.valueOf(inputBufferSize));
    conf.set("tfile.fs.output.buffer.size", String.valueOf(outputBufferSize));
  }


  @Override
  public HDSFileWriter getWriter(long bucketKey, String fileName) throws IOException
  {
    FSDataOutputStream fsdos = getOutputStream(bucketKey, fileName);
    setupConfig(fs.getConf());
    return new TFileWriter(fsdos, minBlockSize, compressName, comparator, fs.getConf());
  }
  
  public int getMinBlockSize()
  {
    return minBlockSize;
  }


  public void setMinBlockSize(int minBlockSize)
  {
    this.minBlockSize = minBlockSize;
  }


  public String getCompressName()
  {
    return compressName;
  }


  public void setCompressName(String compressName)
  {
    this.compressName = compressName;
  }


  public String getComparator()
  {
    return comparator;
  }


  public void setComparator(String comparator)
  {
    this.comparator = comparator;
  }


  public int getChunkSize()
  {
    return chunkSize;
  }


  public void setChunkSize(int chunkSize)
  {
    this.chunkSize = chunkSize;
  }


  public int getInputBufferSize()
  {
    return inputBufferSize;
  }


  public void setInputBufferSize(int inputBufferSize)
  {
    this.inputBufferSize = inputBufferSize;
  }


  public int getOutputBufferSize()
  {
    return outputBufferSize;
  }


  public void setOutputBufferSize(int outputBufferSize)
  {
    this.outputBufferSize = outputBufferSize;
  }


}
