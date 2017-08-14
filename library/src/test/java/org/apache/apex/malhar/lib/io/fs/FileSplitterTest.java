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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.io.block.BlockMetadata;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;
import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class FileSplitterTest
{

  public static class TestClassMeta extends TestWatcher
  {
    @Override
    protected void finished(Description description)
    {
      try {
        FileContext.getLocalFSFileContext()
            .delete(new Path(new File("target/" + description.getClassName()).getAbsolutePath()), true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class TestMeta extends TestWatcher
  {
    public String dataDirectory = null;

    FileSplitter fileSplitter;
    CollectorTestSink<FileSplitter.FileMetadata> fileMetadataSink;
    CollectorTestSink<BlockMetadata.FileBlockMetadata> blockMetadataSink;
    Set<String> filePaths = Sets.newHashSet();
    Context.OperatorContext context;

    Exchanger<Integer> exchanger = new Exchanger<>();

    @Override
    protected void starting(org.junit.runner.Description description)
    {

      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dataDirectory = "target/" + className + "/" + methodName;

      try {
        FileContext.getLocalFSFileContext().delete(new Path(new File(dataDirectory).getAbsolutePath()), true);
        HashSet<String> allLines = Sets.newHashSet();
        for (int file = 0; file < 12; file++) {
          HashSet<String> lines = Sets.newHashSet();
          for (int line = 0; line < 2; line++) {
            lines.add("f" + file + "l" + line);
          }
          allLines.addAll(lines);
          File created = new File(this.dataDirectory, "file" + file + ".txt");
          filePaths.add(new Path(this.dataDirectory, created.getName()).toUri().toString());
          FileUtils.write(created, StringUtils.join(lines, '\n'));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      fileSplitter = new FileSplitter();
      fileSplitter.setScanner(new MockScanner(this));
      fileSplitter.scanner.setScanIntervalMillis(500);
      fileSplitter.scanner.setFilePatternRegularExp(".*[.]txt");
      fileSplitter.scanner.setFiles(dataDirectory);
      fileSplitter.setWindowDataManager(new FSWindowDataManager());

      Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(Context.DAGContext.APPLICATION_PATH, dataDirectory);

      context = mockOperatorContext(0, attributes);
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

  @ClassRule
  public static TestClassMeta classTestMeta = new TestClassMeta();

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testFileMetadata() throws InterruptedException
  {
    testMeta.fileSplitter.beginWindow(1);
    testMeta.exchanger.exchange(null);

    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();
    Assert.assertEquals("File metadata", 12, testMeta.fileMetadataSink.collectedTuples.size());
    for (Object fileMetadata : testMeta.fileMetadataSink.collectedTuples) {
      FileSplitter.FileMetadata metadata = (FileSplitter.FileMetadata)fileMetadata;
      Assert.assertTrue("path: " + metadata.getFilePath(), testMeta.filePaths.contains(metadata.getFilePath()));
      Assert.assertNotNull("name: ", metadata.getFileName());
    }

    testMeta.fileMetadataSink.collectedTuples.clear();
  }

  @Test
  public void testBlockMetadataNoSplit() throws InterruptedException
  {
    testMeta.fileSplitter.beginWindow(1);
    testMeta.exchanger.exchange(null);

    testMeta.fileSplitter.emitTuples();
    Assert.assertEquals("Blocks", 12, testMeta.blockMetadataSink.collectedTuples.size());
    for (Object blockMetadata : testMeta.blockMetadataSink.collectedTuples) {
      BlockMetadata.FileBlockMetadata metadata = (BlockMetadata.FileBlockMetadata)blockMetadata;
      Assert.assertTrue("path: " + metadata.getFilePath(), testMeta.filePaths.contains(metadata.getFilePath()));
    }
  }

  @Test
  public void testBlockMetadataWithSplit() throws InterruptedException
  {
    testMeta.fileSplitter.setBlockSize(2L);
    testMeta.fileSplitter.beginWindow(1);
    testMeta.exchanger.exchange(null);

    testMeta.fileSplitter.emitTuples();
    Assert.assertEquals("Files", 12, testMeta.fileMetadataSink.collectedTuples.size());

    int noOfBlocks = 0;
    for (int i = 0; i < 12; i++) {
      FileSplitter.FileMetadata fm = testMeta.fileMetadataSink.collectedTuples.get(i);
      File testFile = new File(testMeta.dataDirectory, fm.getFileName());
      noOfBlocks += (int)Math.ceil(testFile.length() / (2 * 1.0));
    }
    Assert.assertEquals("Blocks", noOfBlocks, testMeta.blockMetadataSink.collectedTuples.size());
  }

  @Test
  public void testIdempotency() throws InterruptedException
  {
    FSWindowDataManager fsWindowDataManager =
        new FSWindowDataManager();
    testMeta.fileSplitter.setWindowDataManager(fsWindowDataManager);

    testMeta.fileSplitter.setup(testMeta.context);
    //will emit window 1 from data directory
    testFileMetadata();
    testMeta.fileMetadataSink.clear();
    testMeta.blockMetadataSink.clear();

    testMeta.fileSplitter.setup(testMeta.context);
    testMeta.fileSplitter.beginWindow(1);
    Assert.assertEquals("Blocks", 12, testMeta.blockMetadataSink.collectedTuples.size());
    for (Object blockMetadata : testMeta.blockMetadataSink.collectedTuples) {
      BlockMetadata.FileBlockMetadata metadata = (BlockMetadata.FileBlockMetadata)blockMetadata;
      Assert.assertTrue("path: " + metadata.getFilePath(), testMeta.filePaths.contains(metadata.getFilePath()));
    }
  }

  @Test
  public void testTimeScan() throws InterruptedException, IOException, TimeoutException
  {
    testFileMetadata();
    testMeta.fileMetadataSink.clear();
    testMeta.blockMetadataSink.clear();

    Thread.sleep(1000);
    //added a new relativeFilePath
    File f13 = new File(testMeta.dataDirectory, "file13" + ".txt");
    HashSet<String> lines = Sets.newHashSet();
    for (int line = 0; line < 2; line++) {
      lines.add("f13" + "l" + line);
    }
    FileUtils.write(f13, StringUtils.join(lines, '\n'));

    //window 2
    testMeta.fileSplitter.beginWindow(2);
    testMeta.exchanger.exchange(null);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();

    Assert.assertEquals("window 2: files", 1, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("window 2: blocks", 1, testMeta.blockMetadataSink.collectedTuples.size());
  }

  @Test
  public void testTrigger() throws InterruptedException, IOException, TimeoutException
  {
    testMeta.fileSplitter.scanner.setScanIntervalMillis(60 * 1000);
    testFileMetadata();
    testMeta.fileMetadataSink.clear();
    testMeta.blockMetadataSink.clear();

    Thread.sleep(1000);
    //added a new relativeFilePath
    File f13 = new File(testMeta.dataDirectory, "file13" + ".txt");
    HashSet<String> lines = Sets.newHashSet();
    for (int line = 0; line < 2; line++) {
      lines.add("f13" + "l" + line);
    }
    FileUtils.write(f13, StringUtils.join(lines, '\n'));
    testMeta.fileSplitter.scanner.setTrigger(true);

    //window 2
    testMeta.fileSplitter.beginWindow(2);
    testMeta.exchanger.exchange(null);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();

    Assert.assertEquals("window 2: files", 1, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("window 2: blocks", 1, testMeta.blockMetadataSink.collectedTuples.size());
  }

  @Test
  public void testBlocksThreshold() throws InterruptedException
  {
    int noOfBlocks = 0;
    for (int i = 0; i < 12; i++) {
      File testFile = new File(testMeta.dataDirectory, "file" + i + ".txt");
      noOfBlocks += (int)Math.ceil(testFile.length() / (2 * 1.0));
    }

    testMeta.fileSplitter.setBlockSize(2L);
    testMeta.fileSplitter.setBlocksThreshold(10);
    testMeta.fileSplitter.beginWindow(1);

    testMeta.exchanger.exchange(null);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();

    Assert.assertEquals("Blocks", 10, testMeta.blockMetadataSink.collectedTuples.size());

    for (int window = 2; window < 8; window++) {
      testMeta.fileSplitter.beginWindow(window);
      testMeta.fileSplitter.emitTuples();
      testMeta.fileSplitter.endWindow();
    }

    Assert.assertEquals("Files", 12, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("Blocks", noOfBlocks, testMeta.blockMetadataSink.collectedTuples.size());
  }

  @Test
  public void testIdempotencyWithBlocksThreshold() throws InterruptedException
  {
    FSWindowDataManager fsWindowDataManager = new FSWindowDataManager();
    testMeta.fileSplitter.setWindowDataManager(fsWindowDataManager);
    testMeta.fileSplitter.setBlocksThreshold(10);
    testMeta.fileSplitter.scanner.setScanIntervalMillis(500);
    testMeta.fileSplitter.setup(testMeta.context);

    testBlocksThreshold();
    testMeta.fileMetadataSink.clear();
    testMeta.blockMetadataSink.clear();

    testMeta.fileSplitter.setup(testMeta.context);
    for (int i = 1; i < 8; i++) {
      testMeta.fileSplitter.beginWindow(i);
    }
    Assert.assertEquals("Files", 12, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("Blocks", 62, testMeta.blockMetadataSink.collectedTuples.size());
  }

  /**
   * {@link FileSplitter} is deprecated because it has issues with recovery which is why disabling the next 2 tests.
   */
  @Ignore
  public void testFirstWindowAfterRecovery() throws IOException, InterruptedException
  {
    testIdempotencyWithBlocksThreshold();
    Thread.sleep(1000);
    HashSet<String> lines = Sets.newHashSet();
    for (int line = 2; line < 4; line++) {
      lines.add("f13" + "l" + line);
    }
    File f13 = new File(testMeta.dataDirectory, "file13" + ".txt");

    FileUtils.writeLines(f13, lines, true);

    testMeta.fileMetadataSink.clear();
    testMeta.blockMetadataSink.clear();

    testMeta.fileSplitter.beginWindow(8);
    testMeta.exchanger.exchange(null);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();

    Assert.assertEquals("Files", 1, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("Blocks", 6, testMeta.blockMetadataSink.collectedTuples.size());
  }

  @Ignore
  public void testRecoveryOfPartialFile() throws InterruptedException
  {
    FSWindowDataManager fsIdempotentStorageManager = new FSWindowDataManager();
    fsIdempotentStorageManager.setStatePath(testMeta.dataDirectory + '/' + "recovery");
    testMeta.fileSplitter.setWindowDataManager(fsIdempotentStorageManager);
    testMeta.fileSplitter.setBlockSize(2L);
    testMeta.fileSplitter.setBlocksThreshold(2);
    testMeta.fileSplitter.scanner.setScanIntervalMillis(500);

    testMeta.fileSplitter.setup(testMeta.context);

    testMeta.fileSplitter.beginWindow(1);
    testMeta.exchanger.exchange(null);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();

    //file0.txt has just 5 blocks. Since blocks threshold is 2, only 2 are emitted.
    Assert.assertEquals("Files", 1, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("Blocks", 2, testMeta.blockMetadataSink.collectedTuples.size());

    testMeta.fileMetadataSink.clear();
    testMeta.blockMetadataSink.clear();

    //there was a failure and the operator was re-deployed
    testMeta.fileSplitter.setup(testMeta.context);
    testMeta.fileSplitter.beginWindow(1);

    Assert.assertEquals("Recovered Files", 1, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("Recovered Blocks", 2, testMeta.blockMetadataSink.collectedTuples.size());

    testMeta.fileSplitter.beginWindow(2);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();

    Assert.assertEquals("Blocks", 4, testMeta.blockMetadataSink.collectedTuples.size());

    String file1 = testMeta.fileMetadataSink.collectedTuples.get(0).getFileName();

    testMeta.fileMetadataSink.clear();
    testMeta.blockMetadataSink.clear();

    testMeta.fileSplitter.beginWindow(3);
    testMeta.exchanger.exchange(null);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();

    Assert.assertEquals("New file", 1, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("Blocks", 2, testMeta.blockMetadataSink.collectedTuples.size());

    String file2 = testMeta.fileMetadataSink.collectedTuples.get(0).getFileName();

    Assert.assertTrue("Block file name 0",
        testMeta.blockMetadataSink.collectedTuples.get(0).getFilePath().endsWith(file1));
    Assert.assertTrue("Block file name 1",
        testMeta.blockMetadataSink.collectedTuples.get(1).getFilePath().endsWith(file2));
  }

  @Test
  public void testSingleFile() throws InterruptedException, IOException
  {
    testMeta.fileSplitter.teardown();
    testMeta.fileSplitter.scanner = new MockScanner(testMeta);
    testMeta.fileSplitter.scanner.regex = null;
    testMeta.fileSplitter.scanner.setFiles(testMeta.dataDirectory + "/file1.txt");

    testMeta.fileSplitter.setup(testMeta.context);
    testMeta.fileSplitter.beginWindow(1);
    testMeta.exchanger.exchange(null);

    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();
    Assert.assertEquals("File metadata count", 1, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("File metadata", new File(testMeta.dataDirectory + "/file1.txt").getAbsolutePath(),
        testMeta.fileMetadataSink.collectedTuples.get(0).getFilePath());
  }

  private static class MockScanner extends FileSplitter.TimeBasedDirectoryScanner
  {
    TestMeta testMeta;

    MockScanner(TestMeta testMeta)
    {
      this.testMeta = testMeta;
    }

    @Override
    protected void scanComplete()
    {
      super.scanComplete();
      try {
        if (discoveredFiles.size() > 0 && discoveredFiles.getLast().lastFileOfScan) {
          LOG.debug("discovered {}", discoveredFiles.size());
          testMeta.exchanger.exchange(discoveredFiles.size());
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(FileSplitterTest.class);
}
