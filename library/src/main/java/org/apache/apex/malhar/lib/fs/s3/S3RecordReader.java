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

package org.apache.apex.malhar.lib.fs.s3;

import java.io.IOException;
import java.util.Arrays;

import org.apache.apex.malhar.lib.fs.FSRecordReader;
import org.apache.apex.malhar.lib.io.block.BlockMetadata;
import org.apache.apex.malhar.lib.io.block.ReaderContext;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.fs.FSDataInputStream;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.esotericsoftware.kryo.NotNull;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;

/**
 * This operator can be used for reading records/tuples from S3 in parallel
 * (without ordering guarantees between tuples). Records can be delimited (e.g.
 * newline) or fixed width records. Output tuples are byte[].
 *
 * Typically, this operator will be connected to output of FileSplitterInput to
 * read records in parallel.
 *
 * @since 3.7.0
 */
@Evolving
public class S3RecordReader extends FSRecordReader
{
  private String endPoint;
  @NotNull
  private String bucketName;
  @NotNull
  private String accessKey;
  @NotNull
  private String secretAccessKey;
  private int overflowBufferSize;

  public S3RecordReader()
  {
    /*
     * Set default overflowBufferSize to 1MB
     */
    overflowBufferSize = 1024 * 1024;
  }

  /**
   * S3 reader doesn't make use of any stream, hence returns a null value
   *
   * @param block
   *          block metadata
   * @return stream (null object)
   * @throws IOException
   */
  @Override
  protected FSDataInputStream setupStream(BlockMetadata.FileBlockMetadata block) throws IOException
  {
    return null;
  }

  /**
   * Returns an instance of S3FixedWidthRecordReaderContext after setting
   * recordLength and bucketName and initializing s3Client
   *
   * @return S3DelimitedRecordReaderContext
   */
  @Override
  protected ReaderContext<FSDataInputStream> createFixedWidthReaderContext()
  {
    S3FixedWidthRecordReaderContext fixedBytesReaderContext = new S3FixedWidthRecordReaderContext();
    fixedBytesReaderContext.setLength(this.getRecordLength());
    fixedBytesReaderContext.getS3Params().initializeS3Client(accessKey, secretAccessKey, endPoint);
    fixedBytesReaderContext.getS3Params().setBucketName(bucketName);
    return fixedBytesReaderContext;
  }

  /**
   * Returns an instance of S3DelimitedRecordReaderContext after setting
   * bucketName and overflowBuffersize and initializing the s3Client
   *
   * @return S3DelimitedRecordReaderContext
   */
  @Override
  protected ReaderContext<FSDataInputStream> createDelimitedReaderContext()
  {
    S3DelimitedRecordReaderContext delimitedRecordReaderContext = new S3DelimitedRecordReaderContext();
    delimitedRecordReaderContext.getS3Params().initializeS3Client(accessKey, secretAccessKey, endPoint);
    delimitedRecordReaderContext.getS3Params().setBucketName(bucketName);
    delimitedRecordReaderContext.setOverflowBufferSize(overflowBufferSize);
    return delimitedRecordReaderContext;
  }

  /**
   * S3RecordReaderParams is used to hold the common parameters used by the
   * DelimitedRecordReaderContext and FixedWidthReacordReaderContext for S3
   */
  protected static class S3RecordReaderParams
  {
    /**
     * Amazon client used to read bytes from S3
     */
    private AmazonS3 s3Client;
    /**
     * S3 bucket name
     */
    private String bucketName;
    /**
     * path of file being processed in bucket
     */
    private String filePath;
    /**
     * length of the file being processed
     */
    private long fileLength;

    /**
     * Initialize the AmazonS3 client using the accessKey, secretAccessKey, sets
     * endpoint for the s3Client if provided
     *
     * @param accessKey
     * @param secretAccessKey
     * @param endPoint
     */
    public void initializeS3Client(@javax.validation.constraints.NotNull String accessKey,
        @javax.validation.constraints.NotNull String secretAccessKey, String endPoint)
    {
      Preconditions.checkNotNull(accessKey);
      Preconditions.checkNotNull(secretAccessKey);
      s3Client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretAccessKey));
      if (endPoint != null) {
        s3Client.setEndpoint(endPoint);
      }
    }

    /**
     * Set the AmazonS3 service
     *
     * @param s3Client
     *          given s3Client
     */
    public void setS3Client(@javax.validation.constraints.NotNull AmazonS3 s3Client)
    {
      Preconditions.checkNotNull(s3Client);
      this.s3Client = s3Client;
    }

    /**
     * Returns the AmazonS3 service
     *
     * @return s3Client
     */
    public AmazonS3 getS3Client()
    {
      return s3Client;
    }

    /**
     * Set the bucket name
     *
     * @param bucketName
     *          given bucketName
     */
    public void setBucketName(@javax.validation.constraints.NotNull String bucketName)
    {
      Preconditions.checkNotNull(bucketName);
      this.bucketName = bucketName;
    }

    /**
     * Returns the bucket name
     *
     * @return bucketName
     */
    public String getBucketName()
    {
      return bucketName;
    }

    /**
     * Returns the file path
     *
     * @return filePath
     */
    public String getFilePath()
    {
      return filePath;
    }

    /**
     * Returns the length of the file to which the block belongs
     *
     * @return fileLength
     */
    public long getFileLength()
    {
      return fileLength;
    }

    /**
     * This method reads the blockMetadata input parameter and initializes the
     * fileBlock and fileLength
     *
     * @param blockMetadata
     */
    public void initialzeFilepathAndFileLength(BlockMetadata blockMetadata)
    {
      if (blockMetadata instanceof BlockMetadata.FileBlockMetadata) {
        BlockMetadata.FileBlockMetadata fileBlockMetadata = (BlockMetadata.FileBlockMetadata)blockMetadata;
        fileLength = fileBlockMetadata.getFileLength();
        filePath = fileBlockMetadata.getFilePath();
        // File path would be the path after bucket name.
        // Check if the file path starts with "/"
        if (filePath.startsWith("/")) {
          filePath = filePath.substring(1);
        }
      }
    }
  }

  /**
   * RecordReaderContext for reading delimited S3 Records.
   */
  protected static class S3DelimitedRecordReaderContext
      extends ReaderContext.ReadAheadLineReaderContext<FSDataInputStream>
  {
    /**
     * S3 parameters
     */
    private transient S3RecordReaderParams s3Params;

    public S3DelimitedRecordReaderContext()
    {
      s3Params = new S3RecordReaderParams();
    }

    @Override
    public void initialize(FSDataInputStream stream, BlockMetadata blockMetadata, boolean consecutiveBlock)
    {
      super.initialize(stream, blockMetadata, consecutiveBlock);
      s3Params.initialzeFilepathAndFileLength(blockMetadata);
      /*
       * Initialize the bufferSize and overflowBufferSize
       */
      int bufferSize = Long.valueOf(blockMetadata.getLength() - blockMetadata.getOffset()).intValue();
      this.setBufferSize(bufferSize);
      if (overflowBufferSize > bufferSize) {
        this.setOverflowBufferSize(bufferSize);
      } else {
        this.setOverflowBufferSize(overflowBufferSize);
      }
    }

    /**
     * S3 block read would be achieved through the AmazonS3 client. Following
     * are the steps to achieve: (1) Create the objectRequest from bucketName
     * and filePath. (2) Set the range to the above created objectRequest. (3)
     * Get the object portion through AmazonS3 client API. (4) Get the object
     * content from the above object portion.
     *
     * @param bytesFromCurrentOffset
     *          bytes read till now from current offset
     * @param bytesToFetch
     *          the number of bytes to be fetched
     * @return the number of bytes read, -1 if 0 bytes read
     * @throws IOException
     */

    @Override
    protected int readData(final long bytesFromCurrentOffset, final int bytesToFetch) throws IOException
    {
      GetObjectRequest rangeObjectRequest = new GetObjectRequest(s3Params.bucketName, s3Params.filePath);
      rangeObjectRequest.setRange(offset + bytesFromCurrentOffset, offset + bytesFromCurrentOffset + bytesToFetch - 1);
      S3Object objectPortion = s3Params.s3Client.getObject(rangeObjectRequest);
      S3ObjectInputStream wrappedStream = objectPortion.getObjectContent();
      buffer = ByteStreams.toByteArray(wrappedStream);
      wrappedStream.close();
      int bufferLength = buffer.length;
      if (bufferLength <= 0) {
        return -1;
      }
      return bufferLength;
    }

    @Override
    protected boolean checkEndOfStream(final long usedBytesFromOffset)
    {
      if ((offset + usedBytesFromOffset) >= s3Params.fileLength) {
        return true;
      }
      return false;
    }

    /**
     * Returns the S3RecordReaderParams object
     *
     * @return s3Params
     */
    protected S3RecordReaderParams getS3Params()
    {
      return s3Params;
    }
  }

  /**
   * RecordReaderContext for reading fixed width S3 Records.
   */
  protected static class S3FixedWidthRecordReaderContext
      extends ReaderContext.FixedBytesReaderContext<FSDataInputStream>
  {
    /**
     * S3 parameters
     */
    private transient S3RecordReaderParams s3Params;

    /**
     * used to hold data retrieved from S3
     */
    protected transient byte[] buffer;

    /**
     * current offset within the byte[] buffer
     */
    private transient int bufferOffset;

    public S3FixedWidthRecordReaderContext()
    {
      s3Params = new S3RecordReaderParams();
    }

    @Override
    public void initialize(FSDataInputStream stream, BlockMetadata blockMetadata, boolean consecutiveBlock)
    {
      super.initialize(stream, blockMetadata, consecutiveBlock);
      s3Params.initialzeFilepathAndFileLength(blockMetadata);
      try {
        int bytesRead = this.getBlockFromS3();
        if (bytesRead == -1) {
          return;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      this.setBufferOffset(0);
    }

    /**
     * S3 block read would be achieved through the AmazonS3 client. Following
     * are the steps to achieve: (1) Create the objectRequest from bucketName
     * and filePath. (2) Set the range to the above created objectRequest. Set
     * the range so that it gets aligned with the fixed width records. (3) Get
     * the object portion through AmazonS3 client API. (4) Get the object
     * content from the above object portion.
     */
    protected int getBlockFromS3() throws IOException
    {
      long startOffset = blockMetadata.getOffset()
          + (this.length - (blockMetadata.getOffset() % this.length)) % this.length;
      long endOffset = blockMetadata.getLength()
          + ((this.length - (blockMetadata.getLength() % this.length)) % this.length) - 1;
      if (endOffset == (startOffset - 1) || startOffset > s3Params.fileLength) {
        /*
         * If start and end offset is same, it means no record starts in this block
         */
        return -1;
      }
      if (endOffset >= s3Params.fileLength) {
        endOffset = s3Params.fileLength - 1;
      }
      offset = startOffset;
      return readData(startOffset, endOffset);
    }

    /**
     * Reads data from S3 starting from startOffset till the endOffset and
     * returns the number of bytes read
     *
     * @param startOffset
     *          offset from where to read
     * @param endOffset
     *          offset till where to read
     * @return number of bytes read
     * @throws IOException
     */
    protected int readData(long startOffset, long endOffset) throws IOException
    {
      GetObjectRequest rangeObjectRequest = new GetObjectRequest(s3Params.bucketName, s3Params.filePath);
      rangeObjectRequest.setRange(startOffset, endOffset);
      S3Object objectPortion = s3Params.s3Client.getObject(rangeObjectRequest);
      S3ObjectInputStream wrappedStream = objectPortion.getObjectContent();
      buffer = ByteStreams.toByteArray(wrappedStream);
      wrappedStream.close();
      return buffer.length;
    }

    @Override
    protected ReaderContext.Entity readEntity() throws IOException
    {
      entity.clear();
      /*
       * In case file length is not a multiple of record length, the last record may not have length = recordLength.
       * The data to be read from buffer array should be less in this case.
       */
      long bufferLength = length;
      if (offset + length > s3Params.fileLength) {
        bufferLength = s3Params.fileLength - offset;
      }
      byte[] record = Arrays.copyOfRange(buffer, Long.valueOf(bufferOffset).intValue(),
          Long.valueOf(bufferOffset + bufferLength).intValue());
      bufferOffset += record.length;
      entity.setRecord(record);
      entity.setUsedBytes(record.length);
      return entity;
    }

    /**
     * Sets the offset within the current buffer
     *
     * @param bufferOffset
     *          offset within the current buffer
     */
    protected void setBufferOffset(int bufferOffset)
    {
      this.bufferOffset = bufferOffset;
    }

    /**
     * Sets the S3RecordReaderParams object
     *
     * @param s3Params
     *          S3RecordReaderParams object
     */
    protected void setS3Params(S3RecordReaderParams s3Params)
    {
      this.s3Params = s3Params;
    }

    /**
     * Returns the S3RecordReaderParams object
     *
     * @return s3Params
     */
    protected S3RecordReaderParams getS3Params()
    {
      return s3Params;
    }
  }

  /**
   * Size of bytes to be retrieved when a record overflows
   *
   * return overflowBufferSize
   */
  public int getOverflowBufferSize()
  {
    return overflowBufferSize;
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
  public void setBucketName(@javax.validation.constraints.NotNull String bucketName)
  {
    Preconditions.checkNotNull(bucketName);
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
  public void setAccessKey(@javax.validation.constraints.NotNull String accessKey)
  {
    Preconditions.checkNotNull(accessKey);
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
  public void setSecretAccessKey(@javax.validation.constraints.NotNull String secretAccessKey)
  {
    Preconditions.checkNotNull(secretAccessKey);
    this.secretAccessKey = secretAccessKey;
  }

  /**
   * S3 endpoint
   *
   * @param endPoint
   *          endpoint to be used for S3
   */
  public void setEndPoint(String endPoint)
  {
    this.endPoint = endPoint;
  }

  /**
   * S3 endpoint
   *
   * @return s3 endpoint
   */
  public String getEndPoint()
  {
    return endPoint;
  }
}
