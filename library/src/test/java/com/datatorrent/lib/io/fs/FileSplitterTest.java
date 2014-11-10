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
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.io.IdempotentStorageManagerTest;
import com.datatorrent.lib.testbench.CollectorTestSink;

public class FileSplitterTest
{

  public static class TestMeta extends TestWatcher
  {
    public String dataDirectory = null;

    public FileSplitter fileSplitter;
    public CollectorTestSink<Object> fileMetadataSink;
    public CollectorTestSink<Object> blockMetadataSink;
    public Set<String> filePaths = Sets.newHashSet();

    @Override
    protected void starting(org.junit.runner.Description description)
    {

      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dataDirectory = "target/" + className + "/" + methodName;

      try {
        FileContext.getLocalFSFileContext().delete(new Path(new File(dataDirectory).getAbsolutePath()), true);
        HashSet<String> allLines = Sets.newHashSet();
        for (int file = 0; file < 2; file++) {
          HashSet<String> lines = Sets.newHashSet();
          for (int line = 0; line < 2; line++) {
            lines.add("f" + file + "l" + line);
          }
          allLines.addAll(lines);
          File created = new File(this.dataDirectory, "file" + file + ".txt");
          filePaths.add("file:" + created.getAbsolutePath());
          FileUtils.write(created, StringUtils.join(lines, '\n'));
        }
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }

      this.fileSplitter = new FileSplitter();

      AbstractFSDirectoryInputOperator.DirectoryScanner scanner = new AbstractFSDirectoryInputOperator.DirectoryScanner();
      scanner.setFilePatternRegexp(".*[.]txt");
      fileSplitter.setScanner(scanner);
      fileSplitter.setDirectory(dataDirectory);
      fileSplitter.setIdempotentStorageManager(new IdempotentStorageManager.NoopIdempotentStorageManager());
      fileSplitter.setup(new OperatorContextTestHelper.TestIdOperatorContext(0));

      fileMetadataSink = new CollectorTestSink<Object>();
      fileSplitter.filesMetadataOutput.setSink(fileMetadataSink);

      blockMetadataSink = new CollectorTestSink<Object>();
      fileSplitter.blocksMetadataOutput.setSink(blockMetadataSink);
    }

    @Override
    protected void finished(Description description)
    {
      this.fileSplitter.teardown();
      try {
        FileUtils.deleteDirectory(new File(this.dataDirectory));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testFileMetadata()
  {
    testMeta.fileSplitter.beginWindow(1);
    testMeta.fileSplitter.emitTuples();
    testMeta.fileSplitter.endWindow();
    Assert.assertEquals("File metadata", 2, testMeta.fileMetadataSink.collectedTuples.size());
    for (Object fileMetada : testMeta.fileMetadataSink.collectedTuples) {
      FileSplitter.FileMetadata metadata = (FileSplitter.FileMetadata) fileMetada;
      Assert.assertTrue("path: " + metadata.getFilePath(), testMeta.filePaths.contains(metadata.getFilePath()));
      Assert.assertNotNull("name: ", metadata.getFileName());
    }
  }

  @Test
  public void testBlockMetadataNoSplit()
  {
    testMeta.fileSplitter.beginWindow(1);
    testMeta.fileSplitter.emitTuples();
    Assert.assertEquals("Blocks", 2, testMeta.blockMetadataSink.collectedTuples.size());
    for (Object blockMetadata : testMeta.blockMetadataSink.collectedTuples) {
      FileSplitter.BlockMetadata metadata = (FileSplitter.BlockMetadata) blockMetadata;
      Assert.assertTrue("path: " + metadata.getFilePath(), testMeta.filePaths.contains(metadata.getFilePath()));
    }
  }

  @Test
  public void testBlockMetadataWithSplit()
  {
    testMeta.fileSplitter.setBlockSize(2L);
    testMeta.fileSplitter.beginWindow(1);
    testMeta.fileSplitter.emitTuples();

    int noOfBlocks = 0;
    for (int file = 0; file < 2; file++) {
      File testFile = new File(testMeta.dataDirectory, "file" + file + ".txt");
      noOfBlocks += (int) Math.ceil(testFile.length() / (2 * 1.0));
    }
    Assert.assertEquals("Blocks", noOfBlocks, testMeta.blockMetadataSink.collectedTuples.size());
  }

  @Test
  public void testIdempotency()
  {
    Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.DAGContext.APPLICATION_ID, "FileSplitterTest");
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(0, attributes);

    IdempotentStorageManager.FSIdempotentStorageManager fsIdempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();
    fsIdempotentStorageManager.setRecoveryPath(testMeta.dataDirectory + '/' + "recovery");
    testMeta.fileSplitter.setIdempotentStorageManager(fsIdempotentStorageManager);

    testMeta.fileSplitter.setup(context);
    //will emit window 1 from data directory
    testFileMetadata();
    testMeta.fileMetadataSink.clear();
    testMeta.blockMetadataSink.clear();

    testMeta.fileSplitter.setup(context);
    testMeta.fileSplitter.beginWindow(1);
    Assert.assertEquals("Blocks", 2, testMeta.blockMetadataSink.collectedTuples.size());
    for (Object blockMetadata : testMeta.blockMetadataSink.collectedTuples) {
      FileSplitter.BlockMetadata metadata = (FileSplitter.BlockMetadata) blockMetadata;
      Assert.assertTrue("path: " + metadata.getFilePath(), testMeta.filePaths.contains(metadata.getFilePath()));
    }
  }
}