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
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.apex.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;
import org.apache.apex.malhar.lib.io.block.BlockWriter;
import org.apache.apex.malhar.lib.io.fs.FileStitcher.BlockNotFoundException;
import org.apache.apex.malhar.lib.io.fs.Synchronizer.OutputFileMetadata;
import org.apache.apex.malhar.lib.io.fs.Synchronizer.StitchBlock;
import org.apache.apex.malhar.lib.io.fs.Synchronizer.StitchBlockMetaData;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link FileMerger}
 */
public class FileMergerTest
{
  private static OperatorContext context;
  private static final long[] blockIds = new long[] {1, 2, 3 };

  private static final String FILE_DATA = "0123456789";
  private static final String[] BLOCKS_DATA = {"0123", "4567", "89" };

  private static final String dummyDir = "dummpDir/anotherDummDir/";
  private static final String dummyFile = "dummy.txt";

  public static class TestFileMerger extends TestWatcher
  {
    public String recoveryDir = "";
    public String baseDir = "";
    public String blocksDir = "";
    public String outputDir = "";
    public String outputFileName = "";

    public File[] blockFiles = new File[blockIds.length];

    public FileMerger underTest;
    @Mock
    public OutputFileMetadata fileMetaDataMock;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String className = description.getClassName();

      this.baseDir = "target" + Path.SEPARATOR + className + Path.SEPARATOR + description.getMethodName()
          + Path.SEPARATOR;
      this.blocksDir = baseDir + Path.SEPARATOR + BlockWriter.DEFAULT_BLOCKS_DIR + Path.SEPARATOR;
      this.recoveryDir = baseDir + Path.SEPARATOR + "recovery";
      this.outputDir = baseDir + Path.SEPARATOR + "output" + Path.SEPARATOR;
      outputFileName = "output.txt";

      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, description.getMethodName());
      attributes.put(DAGContext.APPLICATION_PATH, baseDir);
      context = mockOperatorContext(1, attributes);

      try {
        FileContext.getLocalFSFileContext().delete(new Path(new File(baseDir).getAbsolutePath()), true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      this.underTest = new FileMerger();
      this.underTest.setFilePath(outputDir);
      this.underTest.setup(context);

      MockitoAnnotations.initMocks(this);
      when(fileMetaDataMock.getFileName()).thenReturn(outputFileName);
      when(fileMetaDataMock.getRelativePath()).thenReturn(outputFileName);
      when(fileMetaDataMock.getStitchedFileRelativePath()).thenReturn(outputFileName);
      when(fileMetaDataMock.getNumberOfBlocks()).thenReturn(3);
      when(fileMetaDataMock.getBlockIds()).thenReturn(new long[] {1, 2, 3 });
      when(fileMetaDataMock.isDirectory()).thenReturn(false);
      when(fileMetaDataMock.getNumberOfBlocks()).thenReturn(blockIds.length);

      List<StitchBlock> outputBlockMetaDataList = Lists.newArrayList();
      try {
        for (int i = 0; i < blockIds.length; i++) {
          blockFiles[i] = new File(blocksDir + blockIds[i]);
          FileUtils.write(blockFiles[i], BLOCKS_DATA[i]);
          FileBlockMetadata fmd = new FileBlockMetadata(blockFiles[i].getPath(), blockIds[i], 0,
              BLOCKS_DATA[i].length(), (i == blockIds.length - 1), -1);
          StitchBlockMetaData outputFileBlockMetaData = new StitchBlockMetaData(fmd, outputFileName,
              (i == blockIds.length - 1));
          outputBlockMetaDataList.add(outputFileBlockMetaData);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      when(fileMetaDataMock.getStitchBlocksList()).thenReturn(outputBlockMetaDataList);

    }

    @Override
    protected void finished(Description description)
    {
      this.underTest.teardown();
      try {
        FileUtils.deleteDirectory(new File("target" + Path.SEPARATOR + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

  }

  @AfterClass
  public static void cleanup()
  {
    try {
      FileUtils.deleteDirectory(new File("target" + Path.SEPARATOR + FileMergerTest.class.getName()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Rule
  public TestFileMerger testFM = new TestFileMerger();

  @Test
  public void testMergeFile() throws IOException
  {
    testFM.underTest.mergeOutputFile(testFM.fileMetaDataMock);
    Assert.assertEquals("File size differes", FILE_DATA.length(),
        FileUtils.sizeOf(new File(testFM.outputDir, testFM.outputFileName)));
  }

  @Test
  public void testBlocksPath()
  {
    Assert.assertEquals("Blocks path not initialized in application context",
        context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + BlockWriter.DEFAULT_BLOCKS_DIR + Path.SEPARATOR,
        testFM.blocksDir);
  }

  @Test
  public void testOverwriteFlag() throws IOException, InterruptedException
  {
    FileUtils.write(new File(testFM.outputDir, testFM.outputFileName), "");
    long modTime = testFM.underTest.outputFS.getFileStatus(new Path(testFM.outputDir, testFM.outputFileName))
        .getModificationTime();
    when(testFM.fileMetaDataMock.getNumberOfBlocks()).thenReturn(0);
    when(testFM.fileMetaDataMock.isDirectory()).thenReturn(false);
    when(testFM.fileMetaDataMock.getBlockIds()).thenReturn(new long[] {});

    Thread.sleep(1000);
    testFM.underTest.setOverwriteOnConflict(true);
    testFM.underTest.processCommittedData(testFM.fileMetaDataMock);
    FileStatus fileStatus = testFM.underTest.outputFS.getFileStatus(new Path(testFM.outputDir, testFM.outputFileName));
    Assert.assertTrue(fileStatus.getModificationTime() > modTime);
  }

  // Using a bit of reconciler during testing, so using committed call explicitly
  @Test
  public void testOverwriteFlagForDirectory() throws IOException, InterruptedException
  {
    FileUtils.forceMkdir(new File(testFM.outputDir + dummyDir));
    when(testFM.fileMetaDataMock.isDirectory()).thenReturn(true);
    when(testFM.fileMetaDataMock.getStitchedFileRelativePath()).thenReturn(dummyDir);
    testFM.underTest.setOverwriteOnConflict(true);

    testFM.underTest.beginWindow(1L);
    testFM.underTest.input.process(testFM.fileMetaDataMock);
    testFM.underTest.endWindow();
    testFM.underTest.checkpointed(1);
    testFM.underTest.committed(1);
    Thread.sleep(1000);

    File statsFile = new File(testFM.outputDir, dummyDir);
    Assert.assertTrue(statsFile.exists() && statsFile.isDirectory());
  }

  @Test(expected = BlockNotFoundException.class)
  public void testMissingBlock() throws IOException, BlockNotFoundException
  {
    FileUtils.deleteQuietly(testFM.blockFiles[2]);
    testFM.underTest.tempOutFilePath = new Path(testFM.baseDir, testFM.fileMetaDataMock.getStitchedFileRelativePath()
        + '.' + System.currentTimeMillis() + FileStitcher.PART_FILE_EXTENTION);
    testFM.underTest.writeTempOutputFile(testFM.fileMetaDataMock);
    fail("Failed when one block missing.");
  }

  @Test
  public void testDirectory() throws IOException
  {
    when(testFM.fileMetaDataMock.getFileName()).thenReturn(dummyDir);
    when(testFM.fileMetaDataMock.getRelativePath()).thenReturn(dummyDir);
    when(testFM.fileMetaDataMock.getStitchedFileRelativePath()).thenReturn(dummyDir);
    when(testFM.fileMetaDataMock.isDirectory()).thenReturn(true); // is a directory
    when(testFM.fileMetaDataMock.getNumberOfBlocks()).thenReturn(0);
    when(testFM.fileMetaDataMock.getBlockIds()).thenReturn(new long[] {});

    testFM.underTest.mergeOutputFile(testFM.fileMetaDataMock);
    File statsFile = new File(testFM.outputDir, dummyDir);
    Assert.assertTrue(statsFile.exists() && statsFile.isDirectory());
  }

  @Test
  public void testFileWithRelativePath() throws IOException
  {
    FileUtils.write(new File(testFM.outputDir, dummyDir + dummyFile), FILE_DATA);
    when(testFM.fileMetaDataMock.getFileName()).thenReturn(dummyDir + dummyFile);
    when(testFM.fileMetaDataMock.getRelativePath()).thenReturn(dummyDir + dummyFile);
    when(testFM.fileMetaDataMock.getStitchedFileRelativePath()).thenReturn(dummyDir + dummyFile);

    testFM.underTest.mergeOutputFile(testFM.fileMetaDataMock);
    File statsFile = new File(testFM.outputDir, dummyDir + dummyFile);
    Assert.assertTrue(statsFile.exists() && !statsFile.isDirectory());
    Assert.assertEquals("File size differes", FILE_DATA.length(),
        FileUtils.sizeOf(new File(testFM.outputDir, dummyDir + dummyFile)));
  }
}
