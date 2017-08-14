/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.fileaccess;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.file.tfile.DTFile;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.io.file.tfile.TFile.Reader;
import org.apache.hadoop.io.file.tfile.TFile.Writer;

/**
 * A TFile wrapper with FileAccess API
 * <ul>
 * <li>{@link TFileImpl.DefaultTFileImpl} return default TFile {@link Reader} and {@link Writer} for IO operations</li>
 * <li>{@link TFileImpl.DTFileImpl} return DTFile {@link org.apache.hadoop.io.file.tfile.DTFile.Reader}(which is faster than default TFile reader) and {@link Writer} for IO operations</li>
 * </ul>
 *
 * @since 2.0.0
 */
@InterfaceStability.Evolving
public abstract class TFileImpl extends FileAccessFSImpl
{
  private int minBlockSize = 64 * 1024;

  private String compressName = TFile.COMPRESSION_NONE;

  private String comparator = "memcmp";

  private int chunkSize = 1024 * 1024;

  private int inputBufferSize = 256 * 1024;

  private int outputBufferSize = 256 * 1024;


  private void setupConfig(Configuration conf)
  {
    conf.set("tfile.io.chunk.size", String.valueOf(chunkSize));
    conf.set("tfile.fs.input.buffer.size", String.valueOf(inputBufferSize));
    conf.set("tfile.fs.output.buffer.size", String.valueOf(outputBufferSize));
  }


  @Override
  public FileWriter getWriter(long bucketKey, String fileName) throws IOException
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

  /**
   * Return {@link TFile} {@link Reader}
   */
  public static class DefaultTFileImpl extends TFileImpl
  {

    @Override
    public FileReader getReader(long bucketKey, String fileName) throws IOException
    {
      FSDataInputStream fsdis = getInputStream(bucketKey, fileName);
      long fileLength = getFileSize(bucketKey, fileName);
      super.setupConfig(fs.getConf());
      return new TFileReader(fsdis, fileLength, fs.getConf());
    }

  }

  /**
   * Return {@link DTFile} {@link org.apache.hadoop.io.file.tfile.DTFile.Reader}
   */
  public static class DTFileImpl extends TFileImpl
  {

    @Override
    public FileReader getReader(long bucketKey, String fileName) throws IOException
    {
      FSDataInputStream fsdis = getInputStream(bucketKey, fileName);
      long fileLength = getFileSize(bucketKey, fileName);
      super.setupConfig(fs.getConf());
      return new DTFileReader(fsdis, fileLength, fs.getConf());
    }

  }


}
