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

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fs.FSRecordCompactionOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.fs.AbstractReconciler;

/**
 * This operator uploads files to Amazon S3 after files are finalized and
 * frozen by the committed callback.
 *
 * S3TupleOutputModule uses this operator in conjunction with S3CompactionOperator
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class S3Reconciler extends AbstractReconciler<FSRecordCompactionOperator.OutputMetaData, FSRecordCompactionOperator.OutputMetaData>
{
  /**
   * Access key id for Amazon S3
   */
  @NotNull
  private String accessKey;

  /**
   * Secret key for Amazon S3
   */
  @NotNull
  private String secretKey;

  /**
   * Bucket name for data upload
   */
  @NotNull
  private String bucketName;

  /**
   * S3 End point
   */
  private String endPoint;

  /**
   * Directory name under S3 bucket
   */
  @NotNull
  private String directoryName;

  /**
   * Client instance for connecting to Amazon S3
   */
  protected transient AmazonS3 s3client;

  /**
   * FileSystem instance for reading intermediate directory
   */
  protected transient FileSystem fs;

  protected transient String filePath;

  private static final String TMP_EXTENSION = ".tmp";

  @Override
  public void setup(Context.OperatorContext context)
  {
    s3client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
    filePath = context.getValue(DAG.APPLICATION_PATH);
    try {
      fs = FileSystem.newInstance(new Path(filePath).toUri(), new Configuration());
    } catch (IOException e) {
      logger.error("Unable to create FileSystem: {}", e.getMessage());
    }
    super.setup(context);
  }

  /**
   * Enques the tuple for processing after committed callback
   */
  @Override
  protected void processTuple(FSRecordCompactionOperator.OutputMetaData outputMetaData)
  {
    logger.debug("enque : {}", outputMetaData);
    enqueueForProcessing(outputMetaData);
  }

  /**
   * Uploads the file on Amazon S3 using putObject API from S3 client
   */
  @Override
  protected void processCommittedData(FSRecordCompactionOperator.OutputMetaData outputMetaData)
  {
    try {
      Path path = new Path(outputMetaData.getPath());
      if (fs.exists(path) == false) {
        logger.debug("Ignoring non-existent path assuming replay : {}", path);
        return;
      }
      FSDataInputStream fsinput = fs.open(path);
      ObjectMetadata omd = new ObjectMetadata();
      omd.setContentLength(outputMetaData.getSize());
      String keyName = directoryName + Path.SEPARATOR + outputMetaData.getFileName();
      PutObjectRequest request = new PutObjectRequest(bucketName, keyName, fsinput, omd);
      if (outputMetaData.getSize() < Integer.MAX_VALUE) {
        request.getRequestClientOptions().setReadLimit((int)outputMetaData.getSize());
      } else {
        throw new RuntimeException("PutRequestSize greater than Integer.MAX_VALUE");
      }
      if (fs.exists(path)) {
        logger.debug("Trying to upload : {}", path);
        s3client.putObject(request);
        logger.debug("Uploading : {}", keyName);
      }
    } catch (FileNotFoundException e) {
      logger.debug("Ignoring non-existent path assuming replay : {}", outputMetaData.getPath());
    } catch (IOException e) {
      logger.error("Unable to create Stream: {}", e.getMessage());
    }
  }

  /**
   * Clears intermediate/temporary files if any
   */
  @Override
  public void endWindow()
  {
    logger.info("in endWindow()");
    while (doneTuples.peek() != null) {
      FSRecordCompactionOperator.OutputMetaData metaData = doneTuples.poll();
      logger.debug("found metaData = {}", metaData);
      committedTuples.remove(metaData);
      try {
        Path dest = new Path(metaData.getPath());
        //Deleting the intermediate files and when writing to tmp files
        // there can be vagrant tmp files which we have to clean
        FileStatus[] statuses = fs.listStatus(dest.getParent());

        for (FileStatus status : statuses) {
          String statusName = status.getPath().getName();
          if (statusName.endsWith(TMP_EXTENSION) && statusName.startsWith(metaData.getFileName())) {
            //a tmp file has tmp extension always preceded by timestamp
            String actualFileName = statusName.substring(0,
                statusName.lastIndexOf('.', statusName.lastIndexOf('.') - 1));
            logger.debug("actualFileName = {}", actualFileName);
            if (metaData.getFileName().equals(actualFileName)) {
              logger.debug("deleting stray file {}", statusName);
              fs.delete(status.getPath(), true);
            }
          } else if (statusName.equals(metaData.getFileName())) {
            logger.info("deleting s3-compaction file {}", statusName);
            fs.delete(status.getPath(), true);
          }
        }
      } catch (IOException e) {
        logger.error("Unable to Delete a file: {}", metaData.getFileName());
      }
    }
  }

  /**
   * Get access key id
   *
   * @return Access key id for Amazon S3
   */
  public String getAccessKey()
  {
    return accessKey;
  }

  /**
   * Set access key id
   *
   * @param accessKey
   *          Access key id for Amazon S3
   */
  public void setAccessKey(@NotNull String accessKey)
  {
    this.accessKey = Preconditions.checkNotNull(accessKey);
  }

  /**
   * Get secret key
   *
   * @return Secret key for Amazon S3
   */
  public String getSecretKey()
  {
    return secretKey;
  }

  /**
   * Set secret key
   *
   * @param secretKey
   *          Secret key for Amazon S3
   */
  public void setSecretKey(@NotNull String secretKey)
  {
    this.secretKey = Preconditions.checkNotNull(secretKey);
  }

  /**
   * Get bucket name
   *
   * @return Bucket name for data upload
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Set bucket name
   *
   * @param bucketName
   *          Bucket name for data upload
   */
  public void setBucketName(@NotNull String bucketName)
  {
    this.bucketName = Preconditions.checkNotNull(bucketName);
  }

  /**
   * Get directory name
   *
   * @return Directory name under S3 bucket
   */
  public String getDirectoryName()
  {
    return directoryName;
  }

  /**
   * Set directory name
   *
   * @param directoryName
   *          Directory name under S3 bucket
   */
  public void setDirectoryName(@NotNull String directoryName)
  {
    this.directoryName = Preconditions.checkNotNull(directoryName);
  }

  /**
   * Return the S3 End point
   *
   * @return S3 End point
   */
  public String getEndPoint()
  {
    return endPoint;
  }

  /**
   * Set the S3 End point
   *
   * @param endPoint
   *          S3 end point
   */
  public void setEndPoint(String endPoint)
  {
    this.endPoint = endPoint;
  }

  /**
   * Set Amazon S3 client
   *
   * @param s3client
   *          Client for Amazon S3
   */
  @VisibleForTesting
  void setS3client(AmazonS3 s3client)
  {
    this.s3client = s3client;
  }

  private static final Logger logger = LoggerFactory.getLogger(S3Reconciler.class);

}
