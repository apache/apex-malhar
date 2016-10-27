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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.block.AbstractFSBlockReader;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.netlet.util.Slice;

@Ignore
public class S3FixedWidthRecordReaderTest
{
  private final String accessKey = "*************";
  private final String secretKey = "*********************";
  private static final int recordLength = 123;
  private static final String FILE_1 = "file1.txt";
  private static final String s3Directory = "input/";

  AbstractFSBlockReader<Slice> getBlockReader(String bucketKey)
  {
    S3RecordReader blockReader = new S3RecordReader();
    blockReader.setAccessKey(accessKey);
    blockReader.setSecretAccessKey(secretKey);
    blockReader.setBucketName(bucketKey);
    blockReader.setRecordLength(recordLength);
    blockReader.setMode("FIXED_WIDTH_RECORD");
    return blockReader;
  }

  class TestMeta extends TestWatcher
  {
    private Context.OperatorContext readerContext;
    private AbstractFSBlockReader<Slice> blockReader;
    private CollectorTestSink<Object> blockMetadataSink;
    private CollectorTestSink<Object> messageSink;
    private List<String[]> messages = Lists.newArrayList();
    private String appId;
    private String dataFilePath;
    private File dataFile;
    private String bucketKey;
    private AmazonS3 client;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      dataFilePath = "src/test/resources/reader_test_data.csv";
      dataFile = new File(dataFilePath);
      bucketKey = new String("target-" + description.getMethodName()).toLowerCase();

      client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
      client.createBucket(bucketKey);

      client.putObject(new PutObjectRequest(bucketKey, s3Directory + FILE_1, dataFile));

      appId = Long.toHexString(System.currentTimeMillis());
      blockReader = getBlockReader(bucketKey);

      Attribute.AttributeMap.DefaultAttributeMap readerAttr = new Attribute.AttributeMap.DefaultAttributeMap();
      readerAttr.put(DAG.APPLICATION_ID, appId);
      readerAttr.put(Context.OperatorContext.SPIN_MILLIS, 10);
      readerContext = new OperatorContextTestHelper.TestIdOperatorContext(1, readerAttr);

      blockReader.setup(readerContext);

      messageSink = new CollectorTestSink<>();
      ((S3RecordReader)blockReader).records.setSink(messageSink);

      blockMetadataSink = new CollectorTestSink<>();
      blockReader.blocksMetadataOutput.setSink(blockMetadataSink);

      BufferedReader reader;
      try {
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(dataFile.getAbsolutePath())));
        String line;
        while ((line = reader.readLine()) != null) {
          messages.add(line.split(","));
        }
        reader.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void finished(Description description)
    {
      deleteBucketAndContent();
      blockReader.teardown();
    }

    public void deleteBucketAndContent()
    {
      //Get the list of objects
      ObjectListing objectListing = client.listObjects(bucketKey);
      for (Iterator<?> iterator = objectListing.getObjectSummaries().iterator(); iterator.hasNext();) {
        S3ObjectSummary objectSummary = (S3ObjectSummary)iterator.next();
        LOG.info("Deleting an object: {}", objectSummary.getKey());
        client.deleteObject(bucketKey, objectSummary.getKey());
      }
      client.deleteBucket(bucketKey);
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  /**
   * The file is processed as a single block
   */
  @Test
  public void testSingleBlock()
  {
    BlockMetadata.FileBlockMetadata block = new BlockMetadata.FileBlockMetadata(s3Directory + FILE_1, 0L, 0L,
        testMeta.dataFile.length(), true, -1, testMeta.dataFile.length());

    testMeta.blockReader.beginWindow(1);
    testMeta.blockReader.blocksMetadataInput.process(block);
    testMeta.blockReader.endWindow();

    List<Object> actualMessages = testMeta.messageSink.collectedTuples;
    Assert.assertEquals("No of records", testMeta.messages.size(), actualMessages.size());

    for (int i = 0; i < actualMessages.size(); i++) {
      byte[] msg = (byte[])actualMessages.get(i);
      /*
       * last character is removed below since the testMeta.messages does not contain '\n'
       *  present in byte[] msg
       */
      Assert.assertTrue("line " + i,
          Arrays.equals(new String(Arrays.copyOf(msg, msg.length - 1)).split(","), testMeta.messages.get(i)));
    }
  }

  /**
   * The file is divided into multiple blocks, blocks are processed
   * consecutively
   */
  @Test
  public void testMultipleBlocks()
  {
    long blockSize = 1000;
    int noOfBlocks = (int)((testMeta.dataFile.length() / blockSize)
        + (((testMeta.dataFile.length() % blockSize) == 0) ? 0 : 1));

    testMeta.blockReader.beginWindow(1);

    for (int i = 0; i < noOfBlocks; i++) {
      BlockMetadata.FileBlockMetadata blockMetadata = new BlockMetadata.FileBlockMetadata(s3Directory + FILE_1, i,
          i * blockSize, i == noOfBlocks - 1 ? testMeta.dataFile.length() : (i + 1) * blockSize, i == noOfBlocks - 1,
          i - 1, testMeta.dataFile.length());
      testMeta.blockReader.blocksMetadataInput.process(blockMetadata);
    }

    testMeta.blockReader.endWindow();

    List<Object> messages = testMeta.messageSink.collectedTuples;
    Assert.assertEquals("No of records", testMeta.messages.size(), messages.size());
    for (int i = 0; i < messages.size(); i++) {

      byte[] msg = (byte[])messages.get(i);
      Assert.assertTrue("line " + i,
          Arrays.equals(new String(Arrays.copyOf(msg, msg.length - 1)).split(","), testMeta.messages.get(i)));
    }
  }

  /**
   * The file is divided into multiple blocks, blocks are processed
   * non-consecutively
   */
  @Test
  public void testNonConsecutiveBlocks()
  {
    long blockSize = 1000;
    int noOfBlocks = (int)((testMeta.dataFile.length() / blockSize)
        + (((testMeta.dataFile.length() % blockSize) == 0) ? 0 : 1));

    testMeta.blockReader.beginWindow(1);

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < Math.ceil(noOfBlocks / 10.0); j++) {
        int blockNo = 10 * j + i;
        if (blockNo >= noOfBlocks) {
          continue;
        }
        BlockMetadata.FileBlockMetadata blockMetadata = new BlockMetadata.FileBlockMetadata(s3Directory + FILE_1,
            blockNo, blockNo * blockSize,
            blockNo == noOfBlocks - 1 ? testMeta.dataFile.length() : (blockNo + 1) * blockSize,
            blockNo == noOfBlocks - 1, blockNo - 1, testMeta.dataFile.length());
        testMeta.blockReader.blocksMetadataInput.process(blockMetadata);
      }
    }

    testMeta.blockReader.endWindow();

    List<Object> messages = testMeta.messageSink.collectedTuples;
    Assert.assertEquals("No of records", testMeta.messages.size(), messages.size());

    Collections.sort(testMeta.messages, new Comparator<String[]>()
    {
      @Override
      public int compare(String[] rec1, String[] rec2)
      {
        return compareStringArrayRecords(rec1, rec2);
      }
    });

    Collections.sort(messages, new Comparator<Object>()
    {
      @Override
      public int compare(Object object1, Object object2)
      {
        String[] rec1 = new String((byte[])object1).split(",");
        String[] rec2 = new String((byte[])object2).split(",");
        return compareStringArrayRecords(rec1, rec2);
      }
    });
    for (int i = 0; i < messages.size(); i++) {
      byte[] msg = (byte[])messages.get(i);
      Assert.assertTrue("line " + i,
          Arrays.equals(new String(Arrays.copyOf(msg, msg.length - 1)).split(","), testMeta.messages.get(i)));
    }
  }

  /**
   * Utility function to compare lexicographically 2 records of string arrays
   *
   * @param rec1
   * @param rec2
   * @return negative if rec1 < rec2, positive if rec1 > rec2, 0 otherwise
   */
  private int compareStringArrayRecords(String[] rec1, String[] rec2)
  {
    for (int i = 0; i < rec1.length && i < rec2.length; i++) {
      if (rec1[i].equals(rec2[i])) {
        continue;
      }
      return rec1[i].compareTo(rec2[i]);
    }
    return 0;
  }

  private static final Logger LOG = LoggerFactory.getLogger(S3FixedWidthRecordReaderTest.class);
}
