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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;

import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Tests for {@link FixedBytesBlockReader}.
 */
public class FixedBytesBlockReaderTest
{
  public static class TestMeta extends TestWatcher
  {
    String output;
    File dataFile;
    Context.OperatorContext readerContext;
    FixedBytesBlockReader blockReader;
    CollectorTestSink<Object> blockMetadataSink;
    CollectorTestSink<Object> messageSink;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      output = "target/" + description.getClassName() + "/" + description.getMethodName();
      try {
        FileUtils.forceMkdir(new File(output));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      dataFile = new File("src/test/resources/reader_test_data.csv");
      blockReader = new FixedBytesBlockReader();

      Attribute.AttributeMap.DefaultAttributeMap readerAttr = new Attribute.AttributeMap.DefaultAttributeMap();
      readerAttr.put(DAG.APPLICATION_ID, Long.toHexString(System.currentTimeMillis()));
      readerAttr.put(Context.OperatorContext.SPIN_MILLIS, 10);
      readerContext = new OperatorContextTestHelper.TestIdOperatorContext(1, readerAttr);

      blockReader.setup(readerContext);

      messageSink = new CollectorTestSink<Object>();
      blockReader.messages.setSink(messageSink);

      blockMetadataSink = new CollectorTestSink<Object>();
      blockReader.blocksMetadataOutput.setSink(blockMetadataSink);
    }

    @Override
    protected void finished(Description description)
    {
      blockReader.teardown();
      try {
        FileUtils.forceDelete(new File(output));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testBytesReceived() throws IOException
  {
    long blockSize = 1500;
    int noOfBlocks = (int) ((testMeta.dataFile.length() / blockSize) + (((testMeta.dataFile.length() % blockSize) == 0) ? 0 : 1));

    testMeta.blockReader.beginWindow(1);

    for (int i = 0; i < noOfBlocks; i++) {
      FileSplitter.BlockMetadata blockMetadata = new FileSplitter.BlockMetadata(i * blockSize, i == noOfBlocks - 1 ? testMeta.dataFile.length() : (i + 1) * blockSize,
        testMeta.dataFile.getAbsolutePath(), i, i == noOfBlocks - 1);
      testMeta.blockReader.blocksMetadataInput.process(blockMetadata);
    }

    testMeta.blockReader.endWindow();

    List<Object> messages = testMeta.messageSink.collectedTuples;
    long totatBytesReceived = 0;

    File outputFile = new File(testMeta.output + "/reader_test_data.csv");
    FileOutputStream outputStream = new FileOutputStream(outputFile);

    for (Object message : messages) {
      @SuppressWarnings("unchecked")
      AbstractBlockReader.ReaderRecord<Slice> msg = (AbstractBlockReader.ReaderRecord<Slice>) message;
      totatBytesReceived += msg.getRecord().length;
      outputStream.write(msg.getRecord().buffer);
    }
    outputStream.close();

    Assert.assertEquals("number of bytes", testMeta.dataFile.length(), totatBytesReceived);
    FileUtils.contentEquals(testMeta.dataFile, outputFile);
  }

}