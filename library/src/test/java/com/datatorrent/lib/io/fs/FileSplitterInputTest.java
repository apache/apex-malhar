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
package com.datatorrent.lib.io.fs;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;

import org.junit.*;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

/**
 * Tests for {@link FileSplitterInput}
 */
public class FileSplitterInputTest
{
  static Set<String> createData(String dataDirectory) throws IOException
  {
    Set<String> filePaths = Sets.newHashSet();
    FileContext.getLocalFSFileContext().delete(new Path(new File(dataDirectory).getAbsolutePath()), true);
    HashSet<String> allLines = Sets.newHashSet();
    for (int file = 0; file < 12; file++) {
      HashSet<String> lines = Sets.newHashSet();
      for (int line = 0; line < 2; line++) {
        lines.add("f" + file + "l" + line);
      }
      allLines.addAll(lines);
      File created = new File(dataDirectory, "file" + file + ".txt");
      filePaths.add(new Path(dataDirectory, created.getName()).toUri().toString());
      FileUtils.write(created, StringUtils.join(lines, '\n'));
    }
    return filePaths;
  }

  public static class TestMeta extends TestWatcher
  {
    String dataDirectory;

    FileSplitterInput fileSplitterInput;
    CollectorTestSink<FileSplitterInput.FileMetadata> fileMetadataSink;
    CollectorTestSink<BlockMetadata.FileBlockMetadata> blockMetadataSink;
    Set<String> filePaths;
    Context.OperatorContext context;
    MockScanner scanner;

    @Override
    protected void starting(org.junit.runner.Description description)
    {

      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dataDirectory = "target/" + className + "/" + methodName + "/data";
      try {
        filePaths = createData(this.dataDirectory);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      fileSplitterInput = new FileSplitterInput();
      scanner = new MockScanner();
      fileSplitterInput.setScanner(scanner);
      fileSplitterInput.getScanner().setScanIntervalMillis(500);
      fileSplitterInput.getScanner().setFilePatternRegularExp(".*[.]txt");
      fileSplitterInput.getScanner().setFiles(dataDirectory);
      fileSplitterInput.setIdempotentStorageManager(new IdempotentStorageManager.NoopIdempotentStorageManager());

      Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(Context.DAGContext.APPLICATION_PATH, "target/" + className + "/" + methodName + "/" + Long.toHexString(System.currentTimeMillis()));

      context = new OperatorContextTestHelper.TestIdOperatorContext(0, attributes);
      fileSplitterInput.setup(context);

      fileMetadataSink = new CollectorTestSink<>();
      TestUtils.setSink(fileSplitterInput.filesMetadataOutput, fileMetadataSink);

      blockMetadataSink = new CollectorTestSink<>();
      TestUtils.setSink(fileSplitterInput.blocksMetadataOutput, blockMetadataSink);
    }

    @Override
    protected void finished(Description description)
    {
      this.fileSplitterInput.teardown();
      FileUtils.deleteQuietly(new File("target/" + description.getClassName() + "/" + description.getMethodName()));
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testFileMetadata() throws InterruptedException
  {
    testMeta.fileSplitterInput.beginWindow(1);
    testMeta.scanner.semaphore.acquire();

    testMeta.fileSplitterInput.emitTuples();
    testMeta.fileSplitterInput.endWindow();
    Assert.assertEquals("File metadata", 12, testMeta.fileMetadataSink.collectedTuples.size());
    for (Object fileMetadata : testMeta.fileMetadataSink.collectedTuples) {
      FileSplitterInput.FileMetadata metadata = (FileSplitterInput.FileMetadata)fileMetadata;
      Assert.assertTrue("path: " + metadata.getFilePath(), testMeta.filePaths.contains(metadata.getFilePath()));
      Assert.assertNotNull("name: ", metadata.getFileName());
    }

    testMeta.fileMetadataSink.collectedTuples.clear();
  }

  @Test
  public void testBlockMetadataNoSplit() throws InterruptedException
  {
    testMeta.fileSplitterInput.beginWindow(1);
    testMeta.scanner.semaphore.acquire();

    testMeta.fileSplitterInput.emitTuples();
    Assert.assertEquals("Blocks", 12, testMeta.blockMetadataSink.collectedTuples.size());
    for (Object blockMetadata : testMeta.blockMetadataSink.collectedTuples) {
      BlockMetadata.FileBlockMetadata metadata = (BlockMetadata.FileBlockMetadata)blockMetadata;
      Assert.assertTrue("path: " + metadata.getFilePath(), testMeta.filePaths.contains(metadata.getFilePath()));
    }
  }

  @Test
  public void testBlockMetadataWithSplit() throws InterruptedException
  {
    testMeta.fileSplitterInput.setBlockSize(2L);
    testMeta.fileSplitterInput.beginWindow(1);
    testMeta.scanner.semaphore.acquire();

    testMeta.fileSplitterInput.emitTuples();
    Assert.assertEquals("Files", 12, testMeta.fileMetadataSink.collectedTuples.size());

    int noOfBlocks = 0;
    for (int i = 0; i < 12; i++) {
      FileSplitterInput.FileMetadata fm = testMeta.fileMetadataSink.collectedTuples.get(i);
      File testFile = new File(testMeta.dataDirectory, fm.getFileName());
      noOfBlocks += (int)Math.ceil(testFile.length() / (2 * 1.0));
    }
    Assert.assertEquals("Blocks", noOfBlocks, testMeta.blockMetadataSink.collectedTuples.size());
  }

  @Test
  public void testIdempotency() throws InterruptedException
  {
    IdempotentStorageManager.FSIdempotentStorageManager fsIdempotentStorageManager =
      new IdempotentStorageManager.FSIdempotentStorageManager();
    testMeta.fileSplitterInput.setIdempotentStorageManager(fsIdempotentStorageManager);

    testMeta.fileSplitterInput.setup(testMeta.context);
    //will emit window 1 from data directory
    testFileMetadata();
    testMeta.fileMetadataSink.clear();
    testMeta.blockMetadataSink.clear();

    testMeta.fileSplitterInput.setup(testMeta.context);
    testMeta.fileSplitterInput.beginWindow(1);
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
    testMeta.fileSplitterInput.beginWindow(2);
    testMeta.scanner.semaphore.acquire();
    testMeta.fileSplitterInput.emitTuples();
    testMeta.fileSplitterInput.endWindow();

    Assert.assertEquals("window 2: files", 1, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("window 2: blocks", 1, testMeta.blockMetadataSink.collectedTuples.size());
  }

  @Test
  public void testTrigger() throws InterruptedException, IOException, TimeoutException
  {
    testMeta.fileSplitterInput.getScanner().setScanIntervalMillis(60 * 1000);
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
    testMeta.fileSplitterInput.getScanner().setTrigger(true);

    //window 2
    testMeta.fileSplitterInput.beginWindow(2);
    testMeta.scanner.semaphore.acquire();
    testMeta.fileSplitterInput.emitTuples();
    testMeta.fileSplitterInput.endWindow();

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

    testMeta.fileSplitterInput.setBlockSize(2L);
    testMeta.fileSplitterInput.setBlocksThreshold(10);
    testMeta.fileSplitterInput.beginWindow(1);

    testMeta.scanner.semaphore.acquire();
    testMeta.fileSplitterInput.emitTuples();
    testMeta.fileSplitterInput.endWindow();

    Assert.assertEquals("Blocks", 10, testMeta.blockMetadataSink.collectedTuples.size());

    for (int window = 2; window < 8; window++) {
      testMeta.fileSplitterInput.beginWindow(window);
      testMeta.fileSplitterInput.emitTuples();
      testMeta.fileSplitterInput.endWindow();
    }

    Assert.assertEquals("Files", 12, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("Blocks", noOfBlocks, testMeta.blockMetadataSink.collectedTuples.size());
  }

  @Test
  public void testIdempotencyWithBlocksThreshold() throws InterruptedException
  {
    IdempotentStorageManager.FSIdempotentStorageManager fsIdempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();
    testMeta.fileSplitterInput.setIdempotentStorageManager(fsIdempotentStorageManager);
    testMeta.fileSplitterInput.setBlocksThreshold(10);
    testMeta.fileSplitterInput.getScanner().setScanIntervalMillis(500);
    testMeta.fileSplitterInput.setup(testMeta.context);

    testBlocksThreshold();
    testMeta.fileMetadataSink.clear();
    testMeta.blockMetadataSink.clear();

    testMeta.fileSplitterInput.setup(testMeta.context);
    for (int i = 1; i < 8; i++) {
      testMeta.fileSplitterInput.beginWindow(i);
    }
    Assert.assertEquals("Files", 12, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("Blocks", 62, testMeta.blockMetadataSink.collectedTuples.size());
  }

  @Test
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

    testMeta.fileSplitterInput.beginWindow(8);
    testMeta.scanner.semaphore.acquire();
    testMeta.fileSplitterInput.emitTuples();
    testMeta.fileSplitterInput.endWindow();

    Assert.assertEquals("Files", 1, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("Blocks", 6, testMeta.blockMetadataSink.collectedTuples.size());
  }

  @Test
  public void testRecoveryOfPartialFile() throws InterruptedException
  {
    IdempotentStorageManager.FSIdempotentStorageManager fsIdempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();
    testMeta.fileSplitterInput.setIdempotentStorageManager(fsIdempotentStorageManager);
    testMeta.fileSplitterInput.setBlockSize(2L);
    testMeta.fileSplitterInput.setBlocksThreshold(2);
    testMeta.fileSplitterInput.getScanner().setScanIntervalMillis(500);

    Kryo kryo = new Kryo();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output loutput = new Output(bos);
    kryo.writeObject(loutput, testMeta.fileSplitterInput);
    loutput.close();

    testMeta.fileSplitterInput.setup(testMeta.context);

    testMeta.fileSplitterInput.beginWindow(1);

    ((MockScanner)testMeta.fileSplitterInput.getScanner()).semaphore.acquire();
    testMeta.fileSplitterInput.emitTuples();
    testMeta.fileSplitterInput.endWindow();

    //file0.txt has just 5 blocks. Since blocks threshold is 2, only 2 are emitted.
    Assert.assertEquals("Files", 1, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("Blocks", 2, testMeta.blockMetadataSink.collectedTuples.size());

    testMeta.fileMetadataSink.clear();
    testMeta.blockMetadataSink.clear();

    testMeta.fileSplitterInput.teardown();

    //there was a failure and the operator was re-deployed
    Input lInput = new Input(bos.toByteArray());
    testMeta.fileSplitterInput = kryo.readObject(lInput, testMeta.fileSplitterInput.getClass());
    lInput.close();
    TestUtils.setSink(testMeta.fileSplitterInput.blocksMetadataOutput, testMeta.blockMetadataSink);
    TestUtils.setSink(testMeta.fileSplitterInput.filesMetadataOutput, testMeta.fileMetadataSink);

    testMeta.fileSplitterInput.setup(testMeta.context);
    testMeta.fileSplitterInput.beginWindow(1);

    Assert.assertEquals("Recovered Files", 1, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("Recovered Blocks", 2, testMeta.blockMetadataSink.collectedTuples.size());

    testMeta.fileSplitterInput.beginWindow(2);
    testMeta.fileSplitterInput.emitTuples();
    testMeta.fileSplitterInput.endWindow();

    Assert.assertEquals("Blocks", 4, testMeta.blockMetadataSink.collectedTuples.size());

    String file1 = testMeta.fileMetadataSink.collectedTuples.get(0).getFileName();

    testMeta.fileMetadataSink.clear();
    testMeta.blockMetadataSink.clear();

    testMeta.fileSplitterInput.beginWindow(3);
    ((MockScanner)testMeta.fileSplitterInput.getScanner()).semaphore.acquire();
    testMeta.fileSplitterInput.emitTuples();
    testMeta.fileSplitterInput.endWindow();

    Assert.assertEquals("New file", 1, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("Blocks", 2, testMeta.blockMetadataSink.collectedTuples.size());

    String file2 = testMeta.fileMetadataSink.collectedTuples.get(0).getFileName();

    Assert.assertTrue("Block file name 0", testMeta.blockMetadataSink.collectedTuples.get(0).getFilePath().endsWith(file1));
    Assert.assertTrue("Block file name 1", testMeta.blockMetadataSink.collectedTuples.get(1).getFilePath().endsWith(file2));
  }

  @Test
  public void testRecursive() throws InterruptedException, IOException
  {
    testMeta.fileSplitterInput.getScanner().regex = null;
    testFileMetadata();
    testMeta.fileMetadataSink.clear();
    testMeta.blockMetadataSink.clear();

    Thread.sleep(1000);
    //added a new relativeFilePath
    File f13 = new File(testMeta.dataDirectory + "/child", "file13" + ".txt");
    HashSet<String> lines = Sets.newHashSet();
    for (int line = 0; line < 2; line++) {
      lines.add("f13" + "l" + line);
    }
    FileUtils.write(f13, StringUtils.join(lines, '\n'));

    //window 2
    testMeta.fileSplitterInput.beginWindow(2);
    testMeta.scanner.semaphore.acquire();
    testMeta.fileSplitterInput.emitTuples();
    testMeta.fileSplitterInput.endWindow();

    Assert.assertEquals("window 2: files", 2, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("window 2: blocks", 1, testMeta.blockMetadataSink.collectedTuples.size());
  }

  @Test
  public void testSingleFile() throws InterruptedException, IOException
  {
    testMeta.fileSplitterInput.teardown();
    testMeta.fileSplitterInput.setScanner(new MockScanner());
    testMeta.fileSplitterInput.getScanner().regex = null;
    testMeta.fileSplitterInput.getScanner().setFiles(testMeta.dataDirectory + "/file1.txt");

    testMeta.fileSplitterInput.setup(testMeta.context);
    testMeta.fileSplitterInput.beginWindow(1);
    ((MockScanner)testMeta.fileSplitterInput.getScanner()).semaphore.acquire();

    testMeta.fileSplitterInput.emitTuples();
    testMeta.fileSplitterInput.endWindow();
    Assert.assertEquals("File metadata count", 1, testMeta.fileMetadataSink.collectedTuples.size());
    Assert.assertEquals("File metadata", new File(testMeta.dataDirectory + "/file1.txt").getAbsolutePath(),
      testMeta.fileMetadataSink.collectedTuples.get(0).getFilePath());
  }

  private static class MockScanner extends FileSplitterInput.TimeBasedDirectoryScanner
  {
    transient Semaphore semaphore;

    private MockScanner()
    {
      super();
      this.semaphore = new Semaphore(0);
    }

    @Override
    protected void scanIterationComplete()
    {
      if (getNumDiscoveredPerIteration() > 0) {
        semaphore.release();
      }
      super.scanIterationComplete();
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(FileSplitterInputTest.class);
}
