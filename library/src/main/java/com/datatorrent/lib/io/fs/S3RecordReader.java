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

import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.fs.FSDataInputStream;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.common.io.ByteStreams;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.FSSliceReader;
import com.datatorrent.lib.io.block.ReaderContext;

/**
 * This operator can be used for reading records/tuples from S3 in parallel
 * (without ordering guarantees between tuples). The operator currently handles
 * only delimited records. Output tuples are byte[].
 *
 * Typically, this operator will be connected to output of FileSplitterInput to
 * read records in parallel.
 *
 * @since 3.7.0
 */
@Evolving
public class S3RecordReader extends FSSliceReader
{
  private transient AmazonS3 s3Client;
  private String bucketName;
  private String accessKey;
  private String secretAccessKey;
  private int overflowBufferSize;

  /**
   * Port to emit individual records/tuples as byte[]
   */
  public final transient DefaultOutputPort<byte[]> records = new DefaultOutputPort<byte[]>();

  public S3RecordReader()
  {
    this.readerContext = new S3DelimitedRecordReaderContext();
    /*
     * Set default overflowBufferSize to 1MB
     */
    overflowBufferSize = 1024 * 1024;
  }

  /**
   * Extracts the bucket name from the given uri
   *
   * @param s3uri
   *          s3 uri
   * @return name of the bucket
   */
  protected static String extractBucket(String s3uri)
  {
    return s3uri.substring(s3uri.indexOf('@') + 1, s3uri.indexOf("/", s3uri.indexOf('@')));
  }

  /**
   * Extracts the accessKey from the given uri
   *
   * @param s3uri
   *          given s3 uri
   * @return the accessKey
   */
  protected static String extractAccessKey(String s3uri)
  {
    return s3uri.substring(s3uri.indexOf("://") + 3, s3uri.indexOf(':', s3uri.indexOf("://") + 3));
  }

  /**
   * Extracts the secretAccessKey from the given uri
   *
   * @param s3uri
   *          given s3uri
   * @return the secretAccessKey
   */
  protected static String extractSecretAccessKey(String s3uri)
  {
    return s3uri.substring(s3uri.indexOf(':', s3uri.indexOf("://") + 1) + 1, s3uri.indexOf('@'));
  }

  /**
   * Extract the file path from given block and set it to the readerContext
   *
   * @param block
   *          block metadata
   * @return stream
   * @throws IOException
   */
  @Override
  protected FSDataInputStream setupStream(BlockMetadata.FileBlockMetadata block) throws IOException
  {
    String filePath = block.getFilePath();
    // File path would be the path after bucket name.
    // Check if the file path starts with "/"
    if (filePath.startsWith("/")) {
      filePath = filePath.substring(1);
    }

    ((S3DelimitedRecordReaderContext)readerContext).setFilePath(filePath);
    ((S3DelimitedRecordReaderContext)readerContext).setFileLength(block.getFileLength());
    return null;
  }

  /**
   * Initialize the reader context
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    s3Client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretAccessKey));
    ((S3DelimitedRecordReaderContext)readerContext).setBucketName(bucketName);
    ((S3DelimitedRecordReaderContext)readerContext).setS3Client(s3Client);
  }

  /**
   * Read the block data and emit records based on reader context
   *
   * @param blockMetadata
   *          block
   * @throws IOException
   */
  protected void readBlock(BlockMetadata blockMetadata) throws IOException
  {
    readerContext.initialize(stream, blockMetadata, consecutiveBlock);
    /*
     * Initialize the buffersize and overflowBuffersize
     */
    int bufferSize = Long.valueOf(blockMetadata.getLength() - blockMetadata.getOffset()).intValue();
    ((S3DelimitedRecordReaderContext)readerContext).setBufferSize(bufferSize);
    if (overflowBufferSize > bufferSize) {
      ((S3DelimitedRecordReaderContext)readerContext).setOverflowBufferSize(bufferSize);
    } else {
      ((S3DelimitedRecordReaderContext)readerContext).setOverflowBufferSize(overflowBufferSize);
    }
    ReaderContext.Entity entity;
    while ((entity = readerContext.next()) != null) {

      counters.getCounter(ReaderCounterKeys.BYTES).add(entity.getUsedBytes());

      byte[] record = entity.getRecord();

      if (record != null) {
        counters.getCounter(ReaderCounterKeys.RECORDS).increment();
        records.emit(record);
      }
    }
  }

  /**
   * RecordReaderContext for reading S3 Records.
   */
  private static class S3DelimitedRecordReaderContext
      extends ReaderContext.ReadAheadLineReaderContext<FSDataInputStream>
  {
    /**
     * Amazon clinet used to read bytes from S3
     */
    private transient AmazonS3 s3Client;
    /**
     * S3 bucket name
     */
    private transient String bucketName;
    /**
     * path of file being processed in bucket
     */
    private transient String filePath;
    /**
     * length of the file being processed
     */
    private transient long fileLength;

    /**
     * S3 block read would be achieved through the AmazonS3 client. Following
     * are the steps to achieve: (1) Create the objectRequest from bucketName
     * and filePath. (2) Set the range to the above created objectRequest. (3)
     * Get the object portion through AmazonS3 client API. (4) Get the object
     * content from the above object portion.
     *
     * @param usedBytes
     *          bytes read till now from current offset
     * @param bytesToFetch
     *          the number of bytes to be fetched
     * @return the number of bytes read, -1 if 0 bytes read
     * @throws IOException
     */

    @Override
    protected int readData(long usedBytes, int bytesToFetch) throws IOException
    {
      GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, filePath);
      rangeObjectRequest.setRange(offset + usedBytes, (offset + usedBytes + bytesToFetch - 1));
      S3Object objectPortion = s3Client.getObject(rangeObjectRequest);
      S3ObjectInputStream wrappedStream = objectPortion.getObjectContent();
      byte[] buffer = ByteStreams.toByteArray(wrappedStream);
      wrappedStream.close();
      setBuffer(buffer);
      int bufferLength = buffer.length;
      if (bufferLength <= 0) {
        return -1;
      }
      return bufferLength;
    }

    @Override
    protected boolean checkEndOfStream(long usedBytesFromOffset)
    {
      if ((offset + usedBytesFromOffset) >= fileLength) {
        return true;
      }
      return false;
    }

    /**
     * Set the AmazonS3 service
     *
     * @param s3Client
     *          given s3Client
     */
    public void setS3Client(AmazonS3 s3Client)
    {
      this.s3Client = s3Client;
    }

    /**
     * Set the bucket name
     *
     * @param bucketName
     *          given bucketName
     */
    public void setBucketName(String bucketName)
    {
      this.bucketName = bucketName;
    }

    /**
     * Sets the file path
     *
     * @param filePath
     *          given filePath
     */
    public void setFilePath(String filePath)
    {
      this.filePath = filePath;
    }

    /**
     * @param fileLength
     *          length of the file to which the block belongs
     */
    public void setFileLength(long fileLength)
    {
      this.fileLength = fileLength;
    }

  }

  /**
   * Size of bytes to be retrieved when a record overflows
   *
   * @param overflowBufferSize
   */
  public void setOverflowBufferSize(int overflowBufferSize)
  {
    this.overflowBufferSize = overflowBufferSize;
  }

  /**
   * Get the S3 bucket name
   *
   * @return bucket
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Set the bucket name where the file resides
   *
   * @param bucketName
   *          bucket name
   */
  public void setBucketName(String bucketName)
  {
    this.bucketName = bucketName;
  }

  /**
   * Return the access key
   *
   * @return the accessKey
   */
  public String getAccessKey()
  {
    return accessKey;
  }

  /**
   * Set the access key
   *
   * @param accessKey
   *          given accessKey
   */
  public void setAccessKey(String accessKey)
  {
    this.accessKey = accessKey;
  }

  /**
   * Return the secretAccessKey
   *
   * @return the secretAccessKey
   */
  public String getSecretAccessKey()
  {
    return secretAccessKey;
  }

  /**
   * Set the secretAccessKey
   *
   * @param secretAccessKey
   *          secretAccessKey
   */
  public void setSecretAccessKey(String secretAccessKey)
  {
    this.secretAccessKey = secretAccessKey;
  }
}
