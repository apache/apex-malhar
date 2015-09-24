/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.fs;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.*;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

/**
 * Tests for {@link FileSplitterBase}
 */
public class FileSplitterBaseTest
{
  @ClassRule
  public static FileSplitterInputTest.TestClassMeta classTestMeta = new FileSplitterInputTest.TestClassMeta();

  static class BastTestMeta extends TestWatcher
  {
    public String dataDirectory;

    FileSplitterBase fileSplitter;
    CollectorTestSink<FileSplitterInput.FileMetadata> fileMetadataSink;
    CollectorTestSink<BlockMetadata.FileBlockMetadata> blockMetadataSink;
    Set<String> filePaths;
    Context.OperatorContext context;

    @Override
    protected void starting(org.junit.runner.Description description)
    {

      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dataDirectory = "target/" + className + "/" + methodName;
      try {
        filePaths = FileSplitterInputTest.createData(this.dataDirectory);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      fileSplitter = new FileSplitterBase();
      fileSplitter.setFile(this.dataDirectory);

      Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(Context.OperatorContext.SPIN_MILLIS, 500);

      context = new OperatorContextTestHelper.TestIdOperatorContext(0, attributes);
      fileSplitter.setup(context);

      fileMetadataSink = new CollectorTestSink<>();
      TestUtils.setSink(fileSplitter.filesMetadataOutput, fileMetadataSink);

      blockMetadataSink = new CollectorTestSink<>();
      TestUtils.setSink(fileSplitter.blocksMetadataOutput, blockMetadataSink);
    }

    @Override
    protected void finished(Description description)
    {
      this.fileSplitter.teardown();
    }
  }

  @Rule
  public BastTestMeta baseTestMeta = new BastTestMeta();

  @Test
  public void testFileMetadata() throws InterruptedException
  {
    baseTestMeta.fileSplitter.beginWindow(1);
    for (String filePath : baseTestMeta.filePaths) {
      baseTestMeta.fileSplitter.input.process(new FileSplitterInput.FileInfo(null, filePath));
    }
    baseTestMeta.fileSplitter.endWindow();
    Assert.assertEquals("File metadata", 12, baseTestMeta.fileMetadataSink.collectedTuples.size());
    for (Object fileMetadata : baseTestMeta.fileMetadataSink.collectedTuples) {
      FileSplitterInput.FileMetadata metadata = (FileSplitterInput.FileMetadata)fileMetadata;
      Assert.assertTrue("path: " + metadata.getFilePath(), baseTestMeta.filePaths.contains(metadata.getFilePath()));
      Assert.assertNotNull("name: ", metadata.getFileName());
    }

    baseTestMeta.fileMetadataSink.collectedTuples.clear();
  }

  @Test
  public void testBlockMetadataNoSplit() throws InterruptedException
  {
    baseTestMeta.fileSplitter.beginWindow(1);
    for (String filePath : baseTestMeta.filePaths) {
      baseTestMeta.fileSplitter.input.process(new FileSplitterInput.FileInfo(null, filePath));
    }
    Assert.assertEquals("Blocks", 12, baseTestMeta.blockMetadataSink.collectedTuples.size());
    for (Object blockMetadata : baseTestMeta.blockMetadataSink.collectedTuples) {
      BlockMetadata.FileBlockMetadata metadata = (BlockMetadata.FileBlockMetadata)blockMetadata;
      Assert.assertTrue("path: " + metadata.getFilePath(), baseTestMeta.filePaths.contains(metadata.getFilePath()));
    }
  }

  @Test
  public void testBlockMetadataWithSplit() throws InterruptedException
  {
    baseTestMeta.fileSplitter.setBlockSize(2L);
    baseTestMeta.fileSplitter.beginWindow(1);
    for (String filePath : baseTestMeta.filePaths) {
      baseTestMeta.fileSplitter.input.process(new FileSplitterInput.FileInfo(null, filePath));
    }
    Assert.assertEquals("Files", 12, baseTestMeta.fileMetadataSink.collectedTuples.size());

    int noOfBlocks = 0;
    for (int i = 0; i < 12; i++) {
      FileSplitterInput.FileMetadata fm = baseTestMeta.fileMetadataSink.collectedTuples.get(i);
      File testFile = new File(baseTestMeta.dataDirectory, fm.getFileName());
      noOfBlocks += (int)Math.ceil(testFile.length() / (2 * 1.0));
    }
    Assert.assertEquals("Blocks", noOfBlocks, baseTestMeta.blockMetadataSink.collectedTuples.size());
  }

  @Test
  public void testBlocksThreshold() throws InterruptedException
  {
    int noOfBlocks = 0;
    for (int i = 0; i < 12; i++) {
      File testFile = new File(baseTestMeta.dataDirectory, "file" + i + ".txt");
      noOfBlocks += (int)Math.ceil(testFile.length() / (2 * 1.0));
    }

    baseTestMeta.fileSplitter.setBlockSize(2L);
    baseTestMeta.fileSplitter.setBlocksThreshold(10);
    baseTestMeta.fileSplitter.beginWindow(1);

    for (String filePath : baseTestMeta.filePaths) {
      baseTestMeta.fileSplitter.input.process(new FileSplitterInput.FileInfo(null, filePath));
    }
    baseTestMeta.fileSplitter.endWindow();

    Assert.assertEquals("Blocks", 10, baseTestMeta.blockMetadataSink.collectedTuples.size());

    for (int window = 2; window < 8; window++) {
      baseTestMeta.fileSplitter.beginWindow(window);
      baseTestMeta.fileSplitter.handleIdleTime();
      baseTestMeta.fileSplitter.endWindow();
    }

    Assert.assertEquals("Files", 12, baseTestMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("Blocks", noOfBlocks, baseTestMeta.blockMetadataSink.collectedTuples.size());
  }

  @Test
  public void testSplitterInApp() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    SplitterApp app = new SplitterApp();
    lma.prepareDAG(app, new Configuration());
    lma.cloneDAG(); // check serialization
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    app.receiver.latch.await();
    Assert.assertEquals("no. of metadata", 12, app.receiver.count);
    FileUtils.deleteQuietly(new File("target/SplitterInApp"));
  }

  @ApplicationAnnotation(name = "TestApp")
  class SplitterApp implements StreamingApplication
  {
    MockReceiver receiver;

    @Override
    public void populateDAG(DAG dag, Configuration configuration)
    {
      dag.setAttribute(DAG.APPLICATION_PATH, "target/SplitterInApp");
      MockFileInput fileInput = dag.addOperator("Input", new MockFileInput());
      fileInput.filePaths = baseTestMeta.filePaths;

      FileSplitterBase splitter = dag.addOperator("Splitter", new FileSplitterBase());
      splitter.setFile(baseTestMeta.dataDirectory);

      receiver = dag.addOperator("Receiver", new MockReceiver());

      dag.addStream("files", fileInput.files, splitter.input);
      dag.addStream("file-metadata", splitter.filesMetadataOutput, receiver.fileMetadata);
    }
  }

  static class MockReceiver extends BaseOperator implements StatsListener
  {
    @AutoMetric
    int count;

    transient CountDownLatch latch = new CountDownLatch(1);
    public final transient DefaultInputPort<FileSplitterInput.FileMetadata> fileMetadata = new DefaultInputPort<FileSplitterInput.FileMetadata>()
    {
      @Override
      public void process(FileSplitterInput.FileMetadata fileMetadata)
      {
        count++;
        LOG.debug("count {}", count);
      }
    };

    @Override
    public Response processStats(BatchedOperatorStats stats)
    {
      Stats.OperatorStats operatorStats = stats.getLastWindowedStats().get(stats.getLastWindowedStats().size() - 1);
      count = (Integer)operatorStats.metrics.get("count");
      if (count == 12) {
        latch.countDown();
      }
      return null;
    }
  }

  static class MockFileInput extends BaseOperator implements InputOperator
  {

    public final transient DefaultOutputPort<FileSplitterInput.FileInfo> files = new DefaultOutputPort<>();

    protected Set<String> filePaths;

    protected boolean done;

    @Override
    public void emitTuples()
    {
      if (!done) {
        done = true;
        for (String file : filePaths) {
          files.emit(new FileSplitterInput.FileInfo(null, file));
        }
      }
    }
  }

  private static final transient Logger LOG = LoggerFactory.getLogger(FileSplitterBaseTest.class);
}
