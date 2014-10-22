/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.fs;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

public class AbstractBlockReaderTest
{
  public static class TestMeta extends TestWatcher
  {
    String dataFilePath;
    File dataFile;
    Context.OperatorContext readerContext;
    BlockReader blockReader;
    CollectorTestSink<Object> blockMetadataSink;
    CollectorTestSink<Object> messageSink;

    List<String[]> messages = Lists.newArrayList();
    String appId;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.dataFilePath = "src/test/resources/reader_test_data.csv";
      this.dataFile = new File(dataFilePath);
      appId = Long.toHexString(System.currentTimeMillis());
      blockReader = new BlockReader();

      Attribute.AttributeMap.DefaultAttributeMap readerAttr = new Attribute.AttributeMap.DefaultAttributeMap();
      readerAttr.put(DAG.APPLICATION_ID, appId);
      readerAttr.put(Context.OperatorContext.SPIN_MILLIS, 10);
      readerContext = new OperatorContextTestHelper.TestIdOperatorContext(1, readerAttr);

      blockReader.setup(readerContext);

      messageSink = new CollectorTestSink<Object>();
      blockReader.messages.setSink(messageSink);

      blockMetadataSink = new CollectorTestSink<Object>();
      blockReader.blocksMetadataOutput.setSink(blockMetadataSink);

      BufferedReader reader;
      try {
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(this.dataFile.getAbsolutePath())));
        String line;
        while ((line = reader.readLine()) != null) {
          messages.add(line.split(","));
        }
      }
      catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void finished(Description description)
    {
      blockReader.teardown();
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void test()
  {
    FileSplitter.BlockMetadata blockMetadata = new FileSplitter.BlockMetadata(0L, testMeta.dataFile.length(),
      testMeta.dataFile.getAbsolutePath(), 0L, true);

    testMeta.blockReader.beginWindow(1);
    testMeta.blockReader.blocksMetadataInput.process(blockMetadata);
    testMeta.blockReader.endWindow();

    List<Object> messages = testMeta.messageSink.collectedTuples;
    Assert.assertEquals("No of records", testMeta.messages.size(), messages.size());

    for (int i = 0; i < messages.size(); i++) {
      @SuppressWarnings("unchecked")
      AbstractBlockReader.ReaderRecord<String> msg = (AbstractBlockReader.ReaderRecord<String>) messages.get(i);
      Assert.assertTrue("line " + i, Arrays.equals(msg.getRecord().split(","), testMeta.messages.get(i)));
    }
  }

  @Test
  public void testBlockSize()
  {
    long blockSize = 1000;
    int noOfBlocks = (int) ((testMeta.dataFile.length() / blockSize) + (((testMeta.dataFile.length() % blockSize) == 0) ? 0 : 1));

    testMeta.blockReader.beginWindow(1);

    for (int i = 0; i < noOfBlocks; i++) {
      FileSplitter.BlockMetadata blockMetadata = new FileSplitter.BlockMetadata(i * blockSize, i == noOfBlocks - 1 ? testMeta.dataFile.length() : (i + 1) * blockSize,
        testMeta.dataFile.getAbsolutePath(), i, i == noOfBlocks - 1);
      testMeta.blockReader.blocksMetadataInput.process(blockMetadata);
    }

    testMeta.blockReader.endWindow();

    List<Object> messages = testMeta.messageSink.collectedTuples;
    Assert.assertEquals("No of records", testMeta.messages.size(), messages.size());
    for (int i = 0; i < messages.size(); i++) {
      @SuppressWarnings("unchecked")
      AbstractBlockReader.ReaderRecord<String> msg = (AbstractBlockReader.ReaderRecord<String>) messages.get(i);
      Assert.assertTrue("line " + i, Arrays.equals(msg.getRecord().split(","), testMeta.messages.get(i)));
    }
  }

  @Test
  public void testBlocksThreshold()
  {
    testMeta.blockReader.setThreshold(100);
    testMeta.blockReader.setup(testMeta.readerContext);

    long blockSize = 1000;
    int noOfBlocks = (int) ((testMeta.dataFile.length() / blockSize) + (((testMeta.dataFile.length() % blockSize) == 0) ? 0 : 1));

    testMeta.blockReader.beginWindow(1);
    for (int i = 0; i < noOfBlocks; i++) {
      FileSplitter.BlockMetadata blockMetadata = new FileSplitter.BlockMetadata(i * blockSize, i == noOfBlocks - 1 ? testMeta.dataFile.length() : (i + 1) * blockSize,
        testMeta.dataFile.getAbsolutePath(), i, i == noOfBlocks - 1);
      testMeta.blockReader.blocksMetadataInput.process(blockMetadata);
    }
    testMeta.blockReader.endWindow();

    Assert.assertEquals("no of blocks processed", 100, testMeta.blockMetadataSink.collectedTuples.size());

    testMeta.blockReader.beginWindow(2);
    testMeta.blockReader.handleIdleTime();
    testMeta.blockReader.endWindow();

    Assert.assertEquals("no of blocks processed", noOfBlocks, testMeta.blockMetadataSink.collectedTuples.size());

    List<Object> messages = testMeta.messageSink.collectedTuples;
    Assert.assertEquals("No of records", testMeta.messages.size(), messages.size());
    for (int i = 0; i < messages.size(); i++) {
      @SuppressWarnings("unchecked")
      AbstractBlockReader.ReaderRecord<String> msg = (AbstractBlockReader.ReaderRecord<String>) messages.get(i);
      Assert.assertTrue("line " + i, Arrays.equals(msg.getRecord().split(","), testMeta.messages.get(i)));
    }
  }

  public static final class BlockReader extends AbstractBlockReader.AbstractLineReader<String>
  {
    private final Pattern datePattern = Pattern.compile("\\d{2}?/\\d{2}?/\\d{4}?");
    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS");

    @Override
    protected String convertToRecord(byte[] bytes)
    {
      return new String(bytes);
    }

    @Override
    protected boolean isRecordValid(String record)
    {
      String[] parts = record.split(",");
      return parts.length > 0 && datePattern.matcher(parts[0]).find();
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBlockReaderTest.class);
}