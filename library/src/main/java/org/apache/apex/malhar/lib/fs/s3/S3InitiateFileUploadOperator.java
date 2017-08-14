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
import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.io.fs.AbstractFileSplitter;
import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 * This is an S3 Initiate file upload operator which can be used to initiate file upload and emits the upload id.
 * Initiate the given file for upload only if the file contains more than one block.
 * This operator is useful in context of S3 Output Module.
 *
 * @since 3.7.0
 */
@InterfaceStability.Evolving
public class S3InitiateFileUploadOperator implements Operator, Operator.CheckpointNotificationListener
{
  @NotNull
  private String bucketName;
  @NotNull
  private String accessKey;
  @NotNull
  private String secretAccessKey;
  private String endPoint;
  @NotNull
  private String outputDirectoryPath;
  private WindowDataManager windowDataManager = new FSWindowDataManager();
  protected transient AmazonS3 s3Client;
  protected transient long currentWindowId;
  protected transient List<UploadFileMetadata> currentWindowRecoveryState;

  public final transient DefaultOutputPort<UploadFileMetadata> fileMetadataOutput = new DefaultOutputPort<>();

  public final transient DefaultOutputPort<UploadFileMetadata> uploadMetadataOutput = new DefaultOutputPort<>();

  /**
   * This input port receive file metadata and those files will be upload into S3.
   */
  public final transient DefaultInputPort<AbstractFileSplitter.FileMetadata> filesMetadataInput = new DefaultInputPort<AbstractFileSplitter.FileMetadata>()
  {
    @Override
    public void process(AbstractFileSplitter.FileMetadata tuple)
    {
      processTuple(tuple);
    }
  };

  /**
   * For the input file, initiate the upload and emit the UploadFileMetadata through the fileMetadataOutput,
   * uploadMetadataOutput ports.
   * @param tuple given tuple
   */
  protected void processTuple(AbstractFileSplitter.FileMetadata tuple)
  {
    if (currentWindowId <= windowDataManager.getLargestCompletedWindow()) {
      return;
    }
    String keyName = getKeyName(tuple.getFilePath());
    String uploadId = "";
    if (tuple.getNumberOfBlocks() > 1) {
      InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, keyName);
      initRequest.setObjectMetadata(createObjectMetadata());
      InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
      uploadId = initResponse.getUploadId();
    }
    UploadFileMetadata uploadFileMetadata = new UploadFileMetadata(tuple, uploadId, keyName);
    fileMetadataOutput.emit(uploadFileMetadata);
    uploadMetadataOutput.emit(uploadFileMetadata);
    currentWindowRecoveryState.add(uploadFileMetadata);
  }

  /**
   * Creates the empty object metadata for initiate multipart upload request.
   * @return the ObjectMetadata
   */
  public ObjectMetadata createObjectMetadata()
  {
    return new ObjectMetadata();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    outputDirectoryPath = StringUtils.removeEnd(outputDirectoryPath, Path.SEPARATOR);
    currentWindowRecoveryState = new ArrayList<>();
    windowDataManager.setup(context);
    s3Client = createClient();
  }

  /**
   * Create AmazonS3 client using AWS credentials
   * @return AmazonS3
   */
  protected AmazonS3 createClient()
  {
    AmazonS3 client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretAccessKey));
    if (endPoint != null) {
      client.setEndpoint(endPoint);
    }
    return client;
  }

  /**
   * Generates the key name from the given file path and output directory path.
   * @param filePath file path to upload
   * @return key name for the given file
   */
  private String getKeyName(String filePath)
  {
    return outputDirectoryPath + Path.SEPARATOR + StringUtils.removeStart(filePath, Path.SEPARATOR);
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    if (windowId <= windowDataManager.getLargestCompletedWindow()) {
      replay(windowId);
    }
  }

  @Override
  public void endWindow()
  {
    if (currentWindowId > windowDataManager.getLargestCompletedWindow()) {
      try {
        windowDataManager.save(currentWindowRecoveryState, currentWindowId);
      } catch (IOException e) {
        throw new RuntimeException("Unable to save recovery", e);
      }
    }
    currentWindowRecoveryState.clear();
  }

  @Override
  public void teardown()
  {
    windowDataManager.teardown();
  }

  protected void replay(long windowId)
  {
    try {
      @SuppressWarnings("unchecked")
      List<UploadFileMetadata> recoveredData = (List<UploadFileMetadata>)windowDataManager.retrieve(windowId);
      if (recoveredData != null) {
        for (UploadFileMetadata upfm : recoveredData) {
          uploadMetadataOutput.emit(upfm);
          fileMetadataOutput.emit(upfm);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {

  }

  @Override
  public void checkpointed(long windowId)
  {

  }

  @Override
  public void committed(long windowId)
  {
    try {
      windowDataManager.committed(windowId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Return the name of the bucket in which to create the multipart upload.
   * @return bucket name
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Set the name of the bucket in which to create the multipart upload.
   * @param bucketName bucket name
   */
  public void setBucketName(@NotNull String bucketName)
  {
    this.bucketName = Preconditions.checkNotNull(bucketName);
  }

  /**
   * Return the AWS access key
   * @return AWS access key
   */
  public String getAccessKey()
  {
    return accessKey;
  }

  /**
   * Sets the AWS access key
   * @param accessKey given access key
   */
  public void setAccessKey(@NotNull String accessKey)
  {
    this.accessKey = Preconditions.checkNotNull(accessKey);
  }

  /**
   * Return the AWS secret access key
   * @return the AWS secret access key
   */
  public String getSecretAccessKey()
  {
    return secretAccessKey;
  }

  /**
   * Sets the AWS secret access key
   * @param secretAccessKey secret access key
   */
  public void setSecretAccessKey(@NotNull String secretAccessKey)
  {
    this.secretAccessKey = Preconditions.checkNotNull(secretAccessKey);
  }

  /**
   * Output directory path for the files to upload
   * @return the output directory path
   */
  public String getOutputDirectoryPath()
  {
    return outputDirectoryPath;
  }

  /**
   * Sets the output directory path for uploading new files.
   * @param outputDirectoryPath output directory path
   */
  public void setOutputDirectoryPath(@NotNull String outputDirectoryPath)
  {
    this.outputDirectoryPath = Preconditions.checkNotNull(outputDirectoryPath);
  }

  /**
   * Returns the window data manager.
   * @return the windowDataManager
   */
  public WindowDataManager getWindowDataManager()
  {
    return windowDataManager;
  }

  /**
   * Sets the window data manager
   * @param windowDataManager given windowDataManager
   */
  public void setWindowDataManager(@NotNull WindowDataManager windowDataManager)
  {
    this.windowDataManager = Preconditions.checkNotNull(windowDataManager);
  }

  /**
   * Returns the AWS S3 end point
   * @return the S3 end point
   */
  public String getEndPoint()
  {
    return endPoint;
  }

  /**
   * Sets the AWS S3 end point
   * @param endPoint S3 end point
   */
  public void setEndPoint(String endPoint)
  {
    this.endPoint = endPoint;
  }

  /**
   * A file upload metadata which contains file metadata, upload id, key name.
   */
  public static class UploadFileMetadata
  {
    private AbstractFileSplitter.FileMetadata fileMetadata;
    private String uploadId;
    private String keyName;

    // For Kryo
    public UploadFileMetadata()
    {
    }

    public UploadFileMetadata(AbstractFileSplitter.FileMetadata fileMetadata, String uploadId, String keyName)
    {
      this.fileMetadata = fileMetadata;
      this.uploadId = uploadId;
      this.keyName = keyName;
    }

    @Override
    public int hashCode()
    {
      return keyName.hashCode();
    }

    /**
     * Returns the name of the key generated from file path.
     * @return the key name
     */
    public String getKeyName()
    {
      return keyName;
    }

    /**
     * Return the file metadata of a file.
     * @return the fileMetadata
     */
    public AbstractFileSplitter.FileMetadata getFileMetadata()
    {
      return fileMetadata;
    }

    /**
     * Returns the unique upload id of a file.
     * @return the upload Id of a file
     */
    public String getUploadId()
    {
      return uploadId;
    }
  }
}
