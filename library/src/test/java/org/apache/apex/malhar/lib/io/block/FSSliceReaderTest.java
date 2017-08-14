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
package org.apache.apex.malhar.lib.io.block;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.netlet.util.Slice;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link FSSliceReader}.
 */
public class FSSliceReaderTest
{
  protected FSSliceReader getBlockReader()
  {
    return new FSSliceReader();
  }

  public class TestMeta extends TestWatcher
  {
    public String output;
    public File dataFile;
    public Context.OperatorContext readerContext;
    public FSSliceReader blockReader;
    public CollectorTestSink<Object> blockMetadataSink;
    public CollectorTestSink<Object> messageSink;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      output = "target/" + description.getClassName() + "/" + description.getMethodName();
      try {
        FileUtils.forceMkdir(new File(output));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      dataFile = new File("src/test/resources/reader_test_data.csv");
      blockReader = getBlockReader();

      Attribute.AttributeMap.DefaultAttributeMap readerAttr = new Attribute.AttributeMap.DefaultAttributeMap();
      readerAttr.put(DAG.APPLICATION_ID, Long.toHexString(System.currentTimeMillis()));
      readerAttr.put(Context.OperatorContext.SPIN_MILLIS, 10);
      readerContext = mockOperatorContext(1, readerAttr);

      blockReader.setup(readerContext);

      messageSink = new CollectorTestSink<>();
      blockReader.messages.setSink(messageSink);

      blockMetadataSink = new CollectorTestSink<>();
      blockReader.blocksMetadataOutput.setSink(blockMetadataSink);
    }

    @Override
    protected void finished(Description description)
    {
      blockReader.teardown();
      try {
        FileUtils.forceDelete(new File("target/" + description.getClassName()));
      } catch (IOException e) {
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
    int noOfBlocks = (int)((testMeta.dataFile.length() / blockSize) + (((testMeta.dataFile.length() % blockSize) == 0) ?
        0 : 1));

    testMeta.blockReader.beginWindow(1);

    for (int i = 0; i < noOfBlocks; i++) {
      BlockMetadata.FileBlockMetadata blockMetadata = new BlockMetadata.FileBlockMetadata(
          testMeta.dataFile.getAbsolutePath(), i, i * blockSize,
          i == noOfBlocks - 1 ? testMeta.dataFile.length() : (i + 1) * blockSize,
          i == noOfBlocks - 1, i - 1);
      testMeta.blockReader.blocksMetadataInput.process(blockMetadata);
    }

    testMeta.blockReader.endWindow();

    List<Object> messages = testMeta.messageSink.collectedTuples;
    long totatBytesReceived = 0;

    File outputFile = new File(testMeta.output + "/reader_test_data.csv");
    FileOutputStream outputStream = new FileOutputStream(outputFile);

    for (Object message : messages) {
      @SuppressWarnings("unchecked")
      AbstractBlockReader.ReaderRecord<Slice> msg = (AbstractBlockReader.ReaderRecord<Slice>)message;
      totatBytesReceived += msg.getRecord().length;
      outputStream.write(msg.getRecord().buffer);
    }
    outputStream.close();

    Assert.assertEquals("number of bytes", testMeta.dataFile.length(), totatBytesReceived);
    FileUtils.contentEquals(testMeta.dataFile, outputFile);
  }

  @Mock
  FileSystem fileSystem;

  public class FSTestReader extends FSSliceReader
  {
    @Override
    protected FileSystem getFSInstance() throws IOException
    {
      return fileSystem;
    }
  }

  @Before
  public void beforeTest()
  {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testBlockSize() throws IOException
  {
    long blockSize = 1000;
    Path path = new Path(testMeta.output);
    when(fileSystem.getDefaultBlockSize(path)).thenReturn(blockSize);
    Attribute.AttributeMap.DefaultAttributeMap readerAttr = new Attribute.AttributeMap.DefaultAttributeMap();
    readerAttr.put(DAG.APPLICATION_ID, Long.toHexString(System.currentTimeMillis()));
    readerAttr.put(Context.OperatorContext.SPIN_MILLIS, 10);

    FSTestReader reader = new FSTestReader();
    reader.setBasePath(testMeta.output);
    reader.setup(mockOperatorContext(1, readerAttr));
    Assert.assertEquals("Block Size", blockSize, (long)((ReaderContext.FixedBytesReaderContext)reader.getReaderContext()).getLength());
  }
}
