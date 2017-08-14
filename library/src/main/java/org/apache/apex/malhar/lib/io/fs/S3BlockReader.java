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
package org.apache.apex.malhar.lib.io.fs;

import java.io.IOException;

import org.apache.apex.malhar.lib.io.block.BlockMetadata;
import org.apache.apex.malhar.lib.io.block.FSSliceReader;
import org.apache.apex.malhar.lib.io.block.ReaderContext;
import org.apache.hadoop.fs.FSDataInputStream;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.common.io.ByteStreams;

import com.datatorrent.api.Context;

/**
 * S3BlockReader extends from BlockReader and serves the functionality of read objects and
 * parse Block metadata
 *
 * @since 3.5.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class S3BlockReader extends FSSliceReader
{
  private transient AmazonS3 s3Client;
  private String bucketName;
  private String accessKey;
  private String secretAccessKey;


  public S3BlockReader()
  {
    this.readerContext = new S3BlockReaderContext();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    s3Client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretAccessKey));
    ((S3BlockReaderContext)readerContext).setBucketName(bucketName);
    ((S3BlockReaderContext)readerContext).setS3Client(s3Client);
  }

  /**
   * Extracts the bucket name from the given uri
   * @param s3uri s3 uri
   * @return name of the bucket
   */
  public static String extractBucket(String s3uri)
  {
    return s3uri.substring(s3uri.indexOf('@') + 1, s3uri.indexOf("/", s3uri.indexOf('@')));
  }

  /**
   * Extracts the accessKey from the given uri
   * @param s3uri given s3 uri
   * @return the accessKey
   */
  public static String extractAccessKey(String s3uri)
  {
    return s3uri.substring(s3uri.indexOf("://") + 3, s3uri.indexOf(':', s3uri.indexOf("://") + 3));
  }

  /**
   * Extracts the secretAccessKey from the given uri
   * @param s3uri given s3uri
   * @return the secretAccessKey
   */
  public static String extractSecretAccessKey(String s3uri)
  {
    return s3uri.substring(s3uri.indexOf(':', s3uri.indexOf("://") + 1) + 1, s3uri.indexOf('@'));
  }

  /**
   * Extract the file path from given block and set it to the readerContext
   * @param block block metadata
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
    ((S3BlockReaderContext)readerContext).setFilePath(filePath);
    return null;
  }

  /**
   * BlockReadeContext for reading S3 Blocks.
   */
  private static class S3BlockReaderContext extends ReaderContext.FixedBytesReaderContext<FSDataInputStream>
  {
    private transient AmazonS3 s3Client;
    private transient String bucketName;
    private transient String filePath;
    /**
     * S3 block read would be achieved through the AmazonS3 client. Following are the steps to achieve:
     * (1) Create the objectRequest from bucketName and filePath.
     * (2) Set the range to the above created objectRequest.
     * (3) Get the object portion through AmazonS3 client API.
     * (4) Get the object content from the above object portion.
     * @return the block entity
     * @throws IOException
     */
    @Override
    protected Entity readEntity() throws IOException
    {
      entity.clear();
      GetObjectRequest rangeObjectRequest = new GetObjectRequest(
          bucketName, filePath);
      rangeObjectRequest.setRange(offset, blockMetadata.getLength() - 1);
      S3Object objectPortion = s3Client.getObject(rangeObjectRequest);
      S3ObjectInputStream wrappedStream = objectPortion.getObjectContent();
      byte[] record = ByteStreams.toByteArray(wrappedStream);
      entity.setUsedBytes(record.length);
      entity.setRecord(record);
      wrappedStream.close();
      return entity;
    }

    /**
     * Return the AmazonS3 service
     * @return the s3Client
     */
    public AmazonS3 getS3Client()
    {
      return s3Client;
    }

    /**
     * Set the AmazonS3 service
     * @param s3Client given s3Client
     */
    public void setS3Client(AmazonS3 s3Client)
    {
      this.s3Client = s3Client;
    }

    /**
     * Get the bucket name
     * @return the bucketName
     */
    public String getBucketName()
    {
      return bucketName;
    }

    /**
     * Set the bucket name
     * @param bucketName given bucketName
     */
    public void setBucketName(String bucketName)
    {
      this.bucketName = bucketName;
    }

    /**
     * Get the file path
     * @return the file path
     */
    public String getFilePath()
    {
      return filePath;
    }

    /**
     * Sets the file path
     * @param filePath given filePath
     */
    public void setFilePath(String filePath)
    {
      this.filePath = filePath;
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
   * Set the bucket name where the file resides
   * @param bucketName bucket name
   */
  public void setBucketName(String bucketName)
  {
    this.bucketName = bucketName;
  }

  /**
   * Return the access key
   * @return the accessKey
   */
  public String getAccessKey()
  {
    return accessKey;
  }

  /**
   * Set the access key
   * @param accessKey given accessKey
   */
  public void setAccessKey(String accessKey)
  {
    this.accessKey = accessKey;
  }

  /**
   * Return the secretAccessKey
   * @return the secretAccessKey
   */
  public String getSecretAccessKey()
  {
    return secretAccessKey;
  }

  /**
   * Set the secretAccessKey
   * @param secretAccessKey secretAccessKey
   */
  public void setSecretAccessKey(String secretAccessKey)
  {
    this.secretAccessKey = secretAccessKey;
  }
}
