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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.io.block.AbstractBlockReader;
import org.apache.apex.malhar.lib.io.block.BlockMetadata;
import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.hadoop.classification.InterfaceStability;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.netlet.util.Slice;

/**
 * This operator can be used to upload the block into S3 bucket using multi-part feature or putObject API.
 * Upload the block into S3 using multi-part feature only if the number of blocks of a file is > 1.
 * This operator is useful in context of S3 Output Module.
 *
 * @since 3.7.0
 */

@InterfaceStability.Evolving
public class S3BlockUploadOperator implements Operator, Operator.CheckpointNotificationListener, Operator.IdleTimeHandler
{
  private static final Logger LOG = LoggerFactory.getLogger(S3BlockUploadOperator.class);
  @NotNull
  private String bucketName;
  @NotNull
  private String accessKey;
  @NotNull
  private String secretAccessKey;
  private String endPoint;
  private Map<String, S3BlockMetaData> blockInfo = new HashMap<>();
  private transient Map<Long, String> blockIdToFilePath = new HashMap<>();
  private WindowDataManager windowDataManager = new FSWindowDataManager();
  protected transient AmazonS3 s3Client;
  private transient long currentWindowId;
  private transient List<AbstractBlockReader.ReaderRecord<Slice>> waitingTuples;
  private transient Map<String, UploadBlockMetadata> currentWindowRecoveryState;
  public final transient DefaultOutputPort<UploadBlockMetadata> output = new DefaultOutputPort<>();

  /**
   * This input port receives incoming tuple's(Block data).
   */
  public final transient DefaultInputPort<AbstractBlockReader.ReaderRecord<Slice>> blockInput = new DefaultInputPort<AbstractBlockReader.ReaderRecord<Slice>>()
  {
    @Override
    public void process(AbstractBlockReader.ReaderRecord<Slice> tuple)
    {
      uploadBlockIntoS3(tuple);
    }
  };

  /**
   * Input port to receive Block meta data
   */
  public final transient DefaultInputPort<BlockMetadata.FileBlockMetadata> blockMetadataInput = new DefaultInputPort<BlockMetadata.FileBlockMetadata>()
  {
    @Override
    public void process(BlockMetadata.FileBlockMetadata blockMetadata)
    {
      if (currentWindowId <= windowDataManager.getLargestCompletedWindow()) {
        return;
      }
      blockIdToFilePath.put(blockMetadata.getBlockId(), blockMetadata.getFilePath());
      LOG.debug("received blockId {} for file {} ", blockMetadata.getBlockId(), blockMetadata.getFilePath());
    }
  };

  /**
   * Input port to receive upload file meta data.
   */
  public final transient DefaultInputPort<S3InitiateFileUploadOperator.UploadFileMetadata> uploadMetadataInput = new DefaultInputPort<S3InitiateFileUploadOperator.UploadFileMetadata>()
  {
    @Override
    public void process(S3InitiateFileUploadOperator.UploadFileMetadata tuple)
    {
      processUploadFileMetadata(tuple);
    }
  };

  /**
   * Convert each block of a given file into S3BlockMetaData
   * @param tuple UploadFileMetadata
   */
  protected void processUploadFileMetadata(S3InitiateFileUploadOperator.UploadFileMetadata tuple)
  {
    long[] blocks = tuple.getFileMetadata().getBlockIds();
    String filePath = tuple.getFileMetadata().getFilePath();
    for (int i = 0; i < blocks.length; i++) {
      String blockId = getUniqueBlockIdFromFile(blocks[i], filePath);
      if (blockInfo.get(blockId) != null) {
        break;
      }
      blockInfo.put(blockId, new S3BlockMetaData(tuple.getKeyName(), tuple.getUploadId(), i + 1));
    }
    if (blocks.length > 0) {
      blockInfo.get(getUniqueBlockIdFromFile(blocks[blocks.length - 1], filePath)).setLastBlock(true);
    }
  }

  /**
   * Construct the unique block id from the given block id and file path.
   * @param blockId Id of the block
   * @param filepath given filepath
   * @return unique block id
   */
  public static String getUniqueBlockIdFromFile(long blockId, String filepath)
  {
    return Long.toString(blockId) + "|" + filepath;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    waitingTuples = new ArrayList<>();
    currentWindowRecoveryState = new HashMap<>();
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

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    if (windowId <= windowDataManager.getLargestCompletedWindow()) {
      replay(windowId);
    }
  }

  /**
   * Replay the state.
   * @param windowId replay window Id
   */
  protected void replay(long windowId)
  {
    try {
      @SuppressWarnings("unchecked")
      Map<String, UploadBlockMetadata> recoveredData = (Map<String, UploadBlockMetadata>)windowDataManager.retrieve(windowId);
      if (recoveredData == null) {
        return;
      }
      for (Map.Entry<String, UploadBlockMetadata> uploadBlockMetadata: recoveredData.entrySet()) {
        output.emit(uploadBlockMetadata.getValue());
        blockInfo.remove(uploadBlockMetadata.getKey());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void endWindow()
  {
    if (waitingTuples.size() > 0) {
      processWaitBlocks();
    }

    for (String uniqueblockId : currentWindowRecoveryState.keySet()) {
      long blockId = Long.parseLong(uniqueblockId.substring(0, uniqueblockId.indexOf("|")));
      LOG.debug("Successfully uploaded {} block", blockId);
      blockIdToFilePath.remove(blockId);
      blockInfo.remove(uniqueblockId);
    }

    if (blockIdToFilePath.size() > 0) {
      for (Long blockId : blockIdToFilePath.keySet()) {
        LOG.info("Unable to uploaded {} block", blockId);
      }
      blockIdToFilePath.clear();
    }

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

  /**
   * Process the blocks which are in wait state.
   */
  private void processWaitBlocks()
  {
    Iterator<AbstractBlockReader.ReaderRecord<Slice>> waitIterator = waitingTuples.iterator();

    while (waitIterator.hasNext()) {
      AbstractBlockReader.ReaderRecord<Slice> blockData = waitIterator.next();
      String filePath = blockIdToFilePath.get(blockData.getBlockId());
      if (filePath != null && blockInfo.get(getUniqueBlockIdFromFile(blockData.getBlockId(), filePath)) != null) {
        uploadBlockIntoS3(blockData);
        waitIterator.remove();
      }
    }
  }

  /**
   * Upload the block into S3 bucket.
   * @param tuple block data
   */
  protected void uploadBlockIntoS3(AbstractBlockReader.ReaderRecord<Slice> tuple)
  {
    if (currentWindowId <= windowDataManager.getLargestCompletedWindow()) {
      return;
    }
    // Check whether the block metadata is present for this block
    if (blockIdToFilePath.get(tuple.getBlockId()) == null) {
      if (!waitingTuples.contains(tuple)) {
        waitingTuples.add(tuple);
      }
      return;
    }
    String uniqueBlockId = getUniqueBlockIdFromFile(tuple.getBlockId(), blockIdToFilePath.get(tuple.getBlockId()));
    S3BlockMetaData metaData = blockInfo.get(uniqueBlockId);
    // Check whether the file metadata is received
    if (metaData == null) {
      if (!waitingTuples.contains(tuple)) {
        waitingTuples.add(tuple);
      }
      return;
    }
    long partSize = tuple.getRecord().length;
    PartETag partETag = null;
    ByteArrayInputStream bis = new ByteArrayInputStream(tuple.getRecord().buffer);
    // Check if it is a Single block of a file
    if (metaData.isLastBlock && metaData.partNo == 1) {
      ObjectMetadata omd = createObjectMetadata();
      omd.setContentLength(partSize);
      PutObjectResult result = s3Client.putObject(new PutObjectRequest(bucketName, metaData.getKeyName(), bis, omd));
      partETag = new PartETag(1, result.getETag());
    } else {
      // Else upload use multi-part feature
      try {
        // Create request to upload a part.
        UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(bucketName).withKey(metaData.getKeyName())
            .withUploadId(metaData.getUploadId()).withPartNumber(metaData.getPartNo()).withInputStream(bis).withPartSize(partSize);
        partETag =  s3Client.uploadPart(uploadRequest).getPartETag();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    UploadBlockMetadata uploadmetadata = new UploadBlockMetadata(partETag, metaData.getKeyName());
    output.emit(uploadmetadata);
    currentWindowRecoveryState.put(uniqueBlockId, uploadmetadata);
    try {
      bis.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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

  @Override
  public void handleIdleTime()
  {
    if (waitingTuples.size() > 0) {
      processWaitBlocks();
    }
  }

  /**
   * Upload block metadata consists of partETag and key name.
   */
  public static class UploadBlockMetadata
  {
    @FieldSerializer.Bind(JavaSerializer.class)
    private PartETag partETag;
    private String keyName;

    // For Kryo
    public UploadBlockMetadata()
    {
    }

    public UploadBlockMetadata(PartETag partETag, String keyName)
    {
      this.partETag = partETag;
      this.keyName = keyName;
    }

    /**
     * Get the partETag of the block
     * @return the partETag
     */
    public PartETag getPartETag()
    {
      return partETag;
    }

    /**
     * Return the key name of the file
     * @return key name
     */
    public String getKeyName()
    {
      return keyName;
    }

    @Override
    public int hashCode()
    {
      return keyName.hashCode();
    }
  }

  /**
   * S3 Block meta data consists of keyname, upload Id, part number and whether the block is last block or not.
   */
  public static class S3BlockMetaData
  {
    private String keyName;
    private String uploadId;
    private Integer partNo;
    private boolean isLastBlock;

    // For Kryo Serialization
    public S3BlockMetaData()
    {
    }

    public S3BlockMetaData(String keyName, String uploadId, Integer partNo)
    {
      this.keyName = keyName;
      this.uploadId = uploadId;
      this.partNo = partNo;
      this.isLastBlock = false;
    }

    /**
     * Return the key name of the file
     * @return key name
     */
    public String getKeyName()
    {
      return keyName;
    }

    /**
     * Return the upload id of the block
     * @return the uplaod id
     */
    public String getUploadId()
    {
      return uploadId;
    }

    /**
     * Return the part number of the block
     * @return the part number
     */
    public Integer getPartNo()
    {
      return partNo;
    }

    /**
     * Specifies whether the block is last or not.
     * @return isLastBlock
     */
    public boolean isLastBlock()
    {
      return isLastBlock;
    }

    /**
     * Sets the block is last or not.
     * @param lastBlock Specifies whether the block is last or not
     */
    public void setLastBlock(boolean lastBlock)
    {
      isLastBlock = lastBlock;
    }
  }

  /**
   * Returns the name of the bucket in which to upload the blocks.
   * @return bucket name
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Sets the name of the bucket in which to upload the blocks.
   * @param bucketName bucket name
   */
  public void setBucketName(@NotNull String bucketName)
  {
    this.bucketName = Preconditions.checkNotNull(bucketName);
  }

  /**
   * Return the AWS access key
   * @return access key
   */
  public String getAccessKey()
  {
    return accessKey;
  }

  /**
   * Sets the AWS access key
   * @param accessKey access key
   */
  public void setAccessKey(@NotNull String accessKey)
  {
    this.accessKey = Preconditions.checkNotNull(accessKey);
  }

  /**
   * Return the AWS access key
   * @return access key
   */
  public String getSecretAccessKey()
  {
    return secretAccessKey;
  }

  /**
   * Sets the AWS access key
   * @param secretAccessKey access key
   */
  public void setSecretAccessKey(@NotNull String secretAccessKey)
  {
    this.secretAccessKey = Preconditions.checkNotNull(secretAccessKey);
  }

  /**
   * Return the AWS S3 end point
   * @return S3 end point
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
}
