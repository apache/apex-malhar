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
package com.datatorrent.lib.io.fs;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;
import com.datatorrent.api.Context;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.FSSliceReader;
import com.datatorrent.lib.io.block.ReaderContext;

/**
 * S3BlockReader extends from BlockReader and serves the functionality of read objects and
 * parse Block metadata
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class S3BlockReader extends FSSliceReader
{
  protected transient String s3bucketUri;
  private String bucketName;

  public S3BlockReader()
  {
    this.readerContext = new S3BlockReaderContext();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    s3bucketUri = fs.getScheme() + "://" + bucketName;
  }

  /**
   * Extracts the bucket name from the given uri
   * @param s3uri s3 uri
   * @return name of the bucket
   */
  @VisibleForTesting
  protected static String extractBucket(String s3uri)
  {
    return s3uri.substring(s3uri.indexOf('@') + 1, s3uri.indexOf("/", s3uri.indexOf('@')));
  }

  /**
   * Create the stream from the bucket uri and block path.
   * @param block block metadata
   * @return stream
   * @throws IOException
   */
  @Override
  protected FSDataInputStream setupStream(BlockMetadata.FileBlockMetadata block) throws IOException
  {
    FSDataInputStream ins = fs.open(new Path(s3bucketUri + block.getFilePath()));
    ins.seek(block.getOffset());
    return ins;
  }

  /**
   * BlockReadeContext for reading S3 Blocks. Stream could't able be read the complete block.
   * This will wait till the block reads completely.
   */
  private static class S3BlockReaderContext extends ReaderContext.FixedBytesReaderContext<FSDataInputStream>
  {
    /**
     * S3 File systems doesn't read the specified block completely while using readFully API.
     * This will read small chunks continuously until will reach the specified block size.
     * @return the block entity
     * @throws IOException
     */
    @Override
    protected Entity readEntity() throws IOException
    {
      entity.clear();
      int bytesToRead = length;
      if (offset + length >= blockMetadata.getLength()) {
        bytesToRead = (int)(blockMetadata.getLength() - offset);
      }
      byte[] record = new byte[bytesToRead];
      int bytesRead = 0;
      while (bytesRead < bytesToRead) {
        bytesRead += stream.read(record, bytesRead, bytesToRead - bytesRead);
      }
      entity.setUsedBytes(bytesRead);
      entity.setRecord(record);
      return entity;
    }
  }

  /**
   * Get the S3 bucket name
   * @return bucket
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Set the bucket name
   * @param bucketName bucket name
   */
  public void setBucketName(String bucketName)
  {
    this.bucketName = bucketName;
  }
}
