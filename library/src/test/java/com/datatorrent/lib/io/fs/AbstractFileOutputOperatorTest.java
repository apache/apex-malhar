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
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.zip.GZIPInputStream;

import javax.annotation.Nonnull;
import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import javax.validation.ConstraintViolationException;

import com.google.common.collect.Maps;
import com.google.common.io.LimitInputStream;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.RandomWordGenerator;
import com.datatorrent.lib.util.TestUtils.TestInfo;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

import com.datatorrent.common.util.DTThrowable;

public class AbstractFileOutputOperatorTest
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileOutputOperatorTest.class);

  private static final String SINGLE_FILE = "single.txt";
  private static final String EVEN_FILE = "even.txt";
  private static final String ODD_FILE = "odd.txt";

  @Rule public TestInfo testMeta = new FSTestWatcher();

  public static OperatorContextTestHelper.TestIdOperatorContext testOperatorContext =
                new OperatorContextTestHelper.TestIdOperatorContext(0);

  public static class FSTestWatcher extends TestInfo
  {
    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      new File(getDir()).mkdir();
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      FileUtils.deleteQuietly(new File(getDir()));
    }
  }

  /**
   * Simple writer which writes to two files.
   */
  private static class EvenOddHDFSExactlyOnceWriter extends AbstractFileOutputOperator<Integer>
  {
    @Override
    protected FileSystem getFSInstance() throws IOException
    {
      return FileSystem.getLocal(new Configuration()).getRaw();
    }

    @Override
    protected String getFileName(Integer tuple)
    {
      if(tuple % 2 == 0)
      {
        return EVEN_FILE;
      }
      else
      {
        return ODD_FILE;
      }
    }

    @Override
    protected byte[] getBytesForTuple(Integer tuple)
    {
      return (tuple.toString() + "\n").getBytes();
    }
  }

  /**
   * Simple writer which writes to one file.
   */
  private static class SingleHDFSExactlyOnceWriter extends AbstractFileOutputOperator<Integer>
  {
    @Override
    protected FileSystem getFSInstance() throws IOException
    {
      return FileSystem.getLocal(new Configuration()).getRaw();
    }

    @Override
    protected String getFileName(Integer tuple)
    {
      return SINGLE_FILE;
    }

    @Override
    protected byte[] getBytesForTuple(Integer tuple)
    {
      return (tuple.toString() + "\n").getBytes();
    }
  }

  /**
   * Simple writer which writes byte array tuples to one file.
   */
  private static class SingleHDFSByteExactlyOnceWriter extends AbstractFileOutputOperator<byte[]>
  {
    SingleHDFSByteExactlyOnceWriter()
    {
    }

    @Override
    protected FileSystem getFSInstance() throws IOException
    {
      return FileSystem.getLocal(new Configuration()).getRaw();
    }

    @Override
    protected String getFileName(byte[] tuple)
    {
      return SINGLE_FILE;
    }

    @Override
    protected byte[] getBytesForTuple(byte[] tuple)
    {
      return tuple;
    }
  }

  /**
   * Dummy writer to store checkpointed state
   */
  @SuppressWarnings("rawtypes")
  public static class CheckPointWriter extends AbstractFileOutputOperator
  {
    @Override
    protected FileSystem getFSInstance() throws IOException
    {
      return FileSystem.getLocal(new Configuration()).getRaw();
    }

    @Override
    protected String getFileName(Object tuple)
    {
      //This is a dummy operator for checkpointing
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    protected byte[] getBytesForTuple(Object tuple)
    {
      //This is a dummy operator for checkpointing
      throw new UnsupportedOperationException("Not supported.");
    }
  }

  /**
   * This is a test app to make sure that the operator validation works properly.
   */

  private static class ValidationTestApp implements StreamingApplication
  {
    private final File testDir;
    private final Long maxLength;
    private final AbstractFileOutputOperator<byte[]> fsWriter;

    ValidationTestApp(File testDir,
                      Long maxLength,
                      AbstractFileOutputOperator<byte[]> fsWriter)
    {
      this.testDir = testDir;
      this.maxLength = maxLength;
      this.fsWriter = fsWriter;
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      RandomWordGenerator randomWordGenerator = new RandomWordGenerator();
      randomWordGenerator.setTuplesPerWindow(2);

      dag.addOperator("random", randomWordGenerator);

      if(maxLength != null) {
        fsWriter.setMaxLength(maxLength);
      }

      fsWriter.setFilePath(testDir.getPath());
      dag.addOperator("fswriter",
                      fsWriter);

      dag.addStream("fswriterstream",
                    randomWordGenerator.output,
                    fsWriter.input);
    }
  }

  private void populateFile(String fileName,
                            String contents)
  {
    File testFile = new File(testMeta.getDir() +
                             "/" +
                             fileName);

    try {
      testFile.createNewFile();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    FileWriter fileWriter;

    try {
      fileWriter = new FileWriter(testFile);
      fileWriter.write(contents);
      fileWriter.close();
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
  }

  /**
   * This method checkpoints the given writer.
   * @param writer The writer to checkpoint.
   * @return Checkpointed writer.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static CheckPointWriter checkpoint(AbstractFileOutputOperator writer)
  {
    CheckPointWriter checkPointWriter = new CheckPointWriter();
    checkPointWriter.counts = Maps.newHashMap();

    for(Object keys: writer.counts.keySet()) {
      checkPointWriter.counts.put(keys,
                                  new MutableLong(((MutableLong) writer.counts.get(keys)).longValue()));
    }

    checkPointWriter.endOffsets = Maps.newHashMap();

    for(Object keys: writer.endOffsets.keySet()) {
      checkPointWriter.endOffsets.put(keys, new MutableLong(((MutableLong) writer.endOffsets.get(keys)).longValue()));
    }

    checkPointWriter.openPart = Maps.newHashMap();

    for(Object keys: writer.openPart.keySet()) {
      checkPointWriter.openPart.put(keys,
                                    new MutableInt(((MutableInt) writer.openPart.get(keys)).intValue()));
    }

    checkPointWriter.filePath = writer.filePath;
    checkPointWriter.maxOpenFiles = writer.maxOpenFiles;
    checkPointWriter.replication = writer.replication;
    checkPointWriter.totalBytesWritten = writer.totalBytesWritten;
    checkPointWriter.maxLength = writer.maxLength;
    checkPointWriter.rollingFile = writer.rollingFile;

    return checkPointWriter;
  }

  /**
   * Restores the checkpointed writer.
   * @param checkPointWriter The checkpointed writer.
   * @param writer The writer to restore state into.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static void restoreCheckPoint(CheckPointWriter checkPointWriter,
                                       AbstractFileOutputOperator writer)
  {
    writer.counts = checkPointWriter.counts;
    writer.endOffsets = checkPointWriter.endOffsets;
    writer.openPart = checkPointWriter.openPart;
    writer.filePath = checkPointWriter.filePath;
    writer.maxOpenFiles = checkPointWriter.maxOpenFiles;
    writer.replication = checkPointWriter.replication;
    writer.totalBytesWritten = checkPointWriter.totalBytesWritten;
    writer.maxLength = checkPointWriter.maxLength;
    writer.rollingFile = checkPointWriter.rollingFile;
  }

  public static void checkOutput(int fileCount,
                                 String baseFilePath,
                                 String expectedOutput)
  {
    if(fileCount >= 0) {
      baseFilePath += "." + fileCount;
    }

    File file = new File(baseFilePath);

    String fileContents = null;

    try {
      fileContents = FileUtils.readFileToString(file);
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }

    Assert.assertEquals("Single file " + fileCount +
                        " output contents",
                        expectedOutput,
                        fileContents);
  }

  @Test
  public void testSingleFileCompletedWrite()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();

    testSingleFileCompletedWriteHelper(writer);

    String singleFileName = testMeta.getDir() + File.separator + SINGLE_FILE;

    String correctContents = "0\n" +
                             "1\n" +
                             "2\n" +
                             "3\n";

    checkOutput(-1,
                singleFileName,
                correctContents);
  }

  @Test
  public void testSingleFileCompletedWriteOverwriteInitial()
  {
    populateFile(SINGLE_FILE,
                 "0\n" +
                 "1\n" +
                 "2\n");

    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();

    testSingleFileCompletedWriteHelper(writer);

    String singleFileName = testMeta.getDir() + File.separator + SINGLE_FILE;

    String correctContents = "0\n" +
                             "1\n" +
                             "2\n" +
                             "3\n";

    checkOutput(-1,
                singleFileName,
                correctContents);
  }

  private void testSingleFileCompletedWriteHelper(SingleHDFSExactlyOnceWriter writer)
  {
    writer.setFilePath(testMeta.getDir());
    writer.setup(testOperatorContext);

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.endWindow();

    writer.beginWindow(1);
    writer.input.put(2);
    writer.input.put(3);
    writer.endWindow();

    writer.teardown();
  }

  @Test
  public void testSingleFileFailedWrite()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();

    testSingleFileFailedWriteHelper(writer);

    String singleFileName = testMeta.getDir() + File.separator + SINGLE_FILE;

    String correctContents = "0\n" +
                             "1\n" +
                             "4\n" +
                             "5\n" +
                             "6\n" +
                             "7\n";

    checkOutput(-1,
                singleFileName,
                correctContents);
  }

  @Test
  public void testSingleFileFailedWriteOverwriteInitial()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    populateFile(SINGLE_FILE,
                 "0\n" +
                 "1\n" +
                 "2\n");

    testSingleFileFailedWriteHelper(writer);

    String singleFileName = testMeta.getDir() + File.separator + SINGLE_FILE;

    String correctContents = "0\n" +
                             "1\n" +
                             "4\n" +
                             "5\n" +
                             "6\n" +
                             "7\n";

    checkOutput(-1,
                singleFileName,
                correctContents);
  }

  private void testSingleFileFailedWriteHelper(SingleHDFSExactlyOnceWriter writer)
  {
    File meta = new File(testMeta.getDir());
    writer.setFilePath(meta.getAbsolutePath());
    writer.setup(testOperatorContext);

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.endWindow();

    CheckPointWriter checkPointWriter = checkpoint(writer);

    writer.beginWindow(1);
    writer.input.put(2);

    writer.teardown();

    restoreCheckPoint(checkPointWriter,
                      writer);
    writer.setup(testOperatorContext);

    writer.beginWindow(1);
    writer.input.put(4);
    writer.input.put(5);
    writer.endWindow();

    writer.beginWindow(2);
    writer.input.put(6);
    writer.input.put(7);
    writer.endWindow();

    writer.teardown();
  }

  @Test
  public void testMultiFileCompletedWrite()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();

    testMultiFileCompletedWriteHelper(writer);

    String evenFileName = testMeta.getDir() + File.separator + EVEN_FILE;

    String correctContents = "0\n" +
                             "2\n" +
                             "4\n" +
                             "6\n";

    checkOutput(-1,
                evenFileName,
                correctContents);

    String oddFileName = testMeta.getDir() + File.separator + ODD_FILE;

    correctContents = "1\n" +
                      "3\n" +
                      "5\n" +
                      "7\n";

    checkOutput(-1,
                oddFileName,
                correctContents);
  }

  //@Ignore
  @Test
  public void testMultiFileCompletedWriteCache1()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setMaxOpenFiles(1);

    testMultiFileCompletedWriteHelper(writer);

    String evenFileName = testMeta.getDir() + File.separator + EVEN_FILE;

    String correctContents = "0\n" +
                             "2\n" +
                             "4\n" +
                             "6\n";

    checkOutput(-1,
                evenFileName,
                correctContents);

    String oddFileName = testMeta.getDir() + File.separator + ODD_FILE;

    correctContents = "1\n" +
                      "3\n" +
                      "5\n" +
                      "7\n";

    checkOutput(-1,
                oddFileName,
                correctContents);
  }

  //@Ignore
  @Test
  public void testMultiFileCompletedWriteOverwriteInitial()
  {
    populateFile(EVEN_FILE,
                 "0\n" +
                 "2\n");

    populateFile(ODD_FILE,
                 "1\n" +
                 "3\n");

    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();

    testMultiFileCompletedWriteHelper(writer);

    String evenFileName = testMeta.getDir() + File.separator + EVEN_FILE;

    String correctContents = "0\n" +
                             "2\n" +
                             "4\n" +
                             "6\n";

    checkOutput(-1,
                evenFileName,
                correctContents);

    String oddFileName = testMeta.getDir() + File.separator + ODD_FILE;

    correctContents = "1\n" +
                      "3\n" +
                      "5\n" +
                      "7\n";

    checkOutput(-1,
                oddFileName,
                correctContents);
  }

  @Test
  public void testMultiFileCompletedWriteOverwriteCache1Initial()
  {
    populateFile(EVEN_FILE,
                 "0\n" +
                 "2\n");

    populateFile(ODD_FILE,
                 "1\n" +
                 "3\n");

    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setMaxOpenFiles(1);

    testMultiFileCompletedWriteHelperCache1(writer);

    String evenFileName = testMeta.getDir() + File.separator + EVEN_FILE;

    String correctContents = "0\n" +
                             "2\n" +
                             "4\n" +
                             "6\n";

    checkOutput(-1,
                evenFileName,
                correctContents);

    String oddFileName = testMeta.getDir() + File.separator + ODD_FILE;

    correctContents = "1\n" +
                      "3\n" +
                      "5\n" +
                      "7\n";

    checkOutput(-1,
                oddFileName,
                correctContents);
  }

  private void testMultiFileCompletedWriteHelperCache1(EvenOddHDFSExactlyOnceWriter writer)
  {
    File meta = new File(testMeta.getDir());
    writer.setFilePath(meta.getAbsolutePath());

    writer.setup(testOperatorContext);

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.input.put(2);
    writer.input.put(3);
    writer.endWindow();

    writer.beginWindow(1);
    writer.input.put(4);
    writer.input.put(5);
    writer.input.put(6);
    writer.input.put(7);
    writer.endWindow();
  }

  private void testMultiFileCompletedWriteHelper(EvenOddHDFSExactlyOnceWriter writer)
  {
    File meta = new File(testMeta.getDir());
    writer.setFilePath(meta.getAbsolutePath());

    writer.setup(testOperatorContext);

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.input.put(2);
    writer.input.put(3);
    writer.endWindow();

    writer.beginWindow(1);
    writer.input.put(4);
    writer.input.put(5);
    writer.input.put(6);
    writer.input.put(7);
    writer.endWindow();
  }

  @Test
  public void testMultiFileFailedWrite()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();

    testMultiFileFailedWriteHelper(writer);

    String evenFileName = testMeta.getDir() + File.separator + EVEN_FILE;

    String correctContents = "0\n" +
                             "2\n" +
                             "6\n" +
                             "8\n";

    checkOutput(-1,
                evenFileName,
                correctContents);

    String oddFileName = testMeta.getDir() + File.separator + ODD_FILE;

    correctContents = "1\n" +
                      "3\n" +
                      "7\n" +
                      "9\n";

    checkOutput(-1,
                oddFileName,
                correctContents);
  }

  @Test
  public void testMultiFileFailedWriteCache1()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setMaxOpenFiles(1);

    testMultiFileFailedWriteHelper(writer);

    String evenFileName = testMeta.getDir() + File.separator + EVEN_FILE;

    String correctContents = "0\n" +
                             "2\n" +
                             "6\n" +
                             "8\n";

    checkOutput(-1,
                evenFileName,
                correctContents);

    String oddFileName = testMeta.getDir() + File.separator + ODD_FILE;

    correctContents = "1\n" +
                      "3\n" +
                      "7\n" +
                      "9\n";

    checkOutput(-1,
                oddFileName,
                correctContents);
  }

  private void testMultiFileFailedWriteHelper(EvenOddHDFSExactlyOnceWriter writer)
  {
    File meta = new File(testMeta.getDir());
    writer.setFilePath(meta.getAbsolutePath());

    writer.setup(testOperatorContext);

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.input.put(2);
    writer.input.put(3);
    writer.endWindow();

    CheckPointWriter checkPointWriter = checkpoint(writer);

    writer.beginWindow(1);
    writer.input.put(4);
    writer.input.put(5);
    writer.endWindow();
    writer.teardown();

    restoreCheckPoint(checkPointWriter,
                      writer);
    writer.setup(testOperatorContext);

    writer.beginWindow(2);
    writer.input.put(6);
    writer.input.put(7);
    writer.input.put(8);
    writer.input.put(9);
    writer.endWindow();
  }

  @Test
  public void testSingleRollingFileCompletedWrite()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();

    testSingleRollingFileCompletedWriteHelper(writer);

    //Rolling file 0

    String singleFileName = testMeta.getDir() + File.separator + SINGLE_FILE;

    String correctContents = "0\n" +
                             "1\n" +
                             "2\n";

    checkOutput(0,
                singleFileName,
                correctContents);

    //Rolling file 1

    correctContents = "3\n" +
                      "4\n" +
                      "5\n";

    checkOutput(1,
                singleFileName,
                correctContents);
  }

  @Test
  public void testSingleRollingFileCompletedWriteOverwriteInitial()
  {
    populateFile(SINGLE_FILE + ".0",
                 "0\n" +
                 "1\n" +
                 "2\n");

    populateFile(SINGLE_FILE + ".1",
                 "0\n" +
                 "1\n" +
                 "2\n");


    populateFile(SINGLE_FILE + ".2",
                 "0\n" +
                 "1\n" +
                 "2\n");

    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();

    testSingleRollingFileCompletedWriteHelper(writer);

    //Rolling file 0

    String singleFileName = testMeta.getDir() + File.separator + SINGLE_FILE;

    String correctContents = "0\n" +
                             "1\n" +
                             "2\n";

    checkOutput(0,
                singleFileName,
                correctContents);

    //Rolling file 1

    correctContents = "3\n" +
                      "4\n" +
                      "5\n";

    checkOutput(1,
                singleFileName,
                correctContents);
  }

  private void testSingleRollingFileCompletedWriteHelper(SingleHDFSExactlyOnceWriter writer)
  {
    writer.setFilePath(testMeta.getDir());
    writer.setMaxLength(4);

    writer.setup(testOperatorContext);

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.input.put(2);
    writer.endWindow();

    writer.beginWindow(1);
    writer.input.put(3);
    writer.input.put(4);
    writer.input.put(5);
    writer.endWindow();

    writer.teardown();
  }

  @Test
  public void testSingleRollingFileFailedWrite()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();

    testSingleRollingFileFailedWriteHelper(writer);

    //Rolling file 0

    String singleFileName = testMeta.getDir() + File.separator + SINGLE_FILE;

    String correctContents = "0\n" +
                             "1\n" +
                             "2\n";

    checkOutput(0,
                singleFileName,
                correctContents);

    //Rolling file 1

    correctContents = "3\n" +
                      "4\n" +
                      "5\n";

    checkOutput(1,
                singleFileName,
                correctContents);

    //Rolling file 2

    correctContents = "6\n" +
                      "7\n" +
                      "8\n";

    checkOutput(2,
                singleFileName,
                correctContents);
  }

  private void testSingleRollingFileFailedWriteHelper(SingleHDFSExactlyOnceWriter writer)
  {
    writer.setMaxLength(4);
    writer.setFilePath(testMeta.getDir());
    writer.setup(testOperatorContext);

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.input.put(2);
    writer.endWindow();

    CheckPointWriter checkPointWriter = checkpoint(writer);

    writer.beginWindow(1);
    writer.input.put(3);
    writer.input.put(4);

    writer.teardown();

    restoreCheckPoint(checkPointWriter,
                      writer);
    writer.setup(testOperatorContext);

    writer.beginWindow(1);
    writer.input.put(3);
    writer.input.put(4);
    writer.input.put(5);
    writer.endWindow();

    writer.beginWindow(2);
    writer.input.put(6);
    writer.input.put(7);
    writer.input.put(8);
    writer.endWindow();

    writer.teardown();
  }

  @Test
  public void testSingleRollingFileFailedWrite1()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setFilePath(testMeta.getDir());
    writer.setMaxLength(4);

    writer.setup(testOperatorContext);

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.input.put(2);
    writer.endWindow();

    writer.beginWindow(1);
    writer.input.put(3);
    writer.input.put(4);
    writer.endWindow();

    CheckPointWriter checkPointWriter = checkpoint(writer);
    CheckPointWriter checkPointWriter1 = checkpoint(writer);

    LOG.debug("Checkpoint endOffsets={}", checkPointWriter.endOffsets);

    writer.beginWindow(2);
    writer.input.put(5);
    writer.teardown();

    restoreCheckPoint(checkPointWriter,
                      writer);
    LOG.debug("Checkpoint endOffsets={}", checkPointWriter.endOffsets);
    writer.setup(testOperatorContext);

    writer.beginWindow(2);
    writer.input.put(5);
    writer.endWindow();

    writer.beginWindow(3);
    writer.input.put(6);
    writer.input.put(7);
    writer.input.put(8);
    writer.endWindow();

    writer.teardown();

    restoreCheckPoint(checkPointWriter1,
                      writer);
    writer.setup(testOperatorContext);

    String singleFilePath = testMeta.getDir() + File.separator + SINGLE_FILE;

    //Rolling file 0

    String correctContents = "0\n" +
                             "1\n" +
                             "2\n";
    checkOutput(0,
                singleFilePath,
                correctContents);

    //Rolling file 1

    correctContents = "3\n" +
                      "4\n";

    checkOutput(1,
                singleFilePath,
                correctContents);
  }

  //@Ignore
  @Test
  public void testMultiRollingFileCompletedWrite()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();

    testMultiRollingFileCompletedWriteHelper(writer);
  }

  @Test
  public void testMultiRollingFileCompletedWriteCache1()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setMaxOpenFiles(1);

    testMultiRollingFileCompletedWriteHelper(writer);
  }

  @Test
  public void testMultiRollingFileCompletedWriteOverwrite()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();

    testMultiRollingFileCompletedWriteHelper(writer);
  }

  @Test
  public void testMultiRollingFileCompletedWriteOverwriteCache1()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setMaxOpenFiles(1);

    testMultiRollingFileCompletedWriteHelperCache1(writer);
  }

  private void testMultiRollingFileCompletedWriteHelperCache1(EvenOddHDFSExactlyOnceWriter writer)
  {
    writer.setMaxLength(4);
    File meta = new File(testMeta.getDir());
    writer.setFilePath(meta.getAbsolutePath());

    writer.setup(testOperatorContext);

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.input.put(2);
    writer.input.put(3);
    writer.input.put(4);
    writer.input.put(5);
    writer.endWindow();

    writer.beginWindow(1);
    writer.input.put(6);
    writer.input.put(7);
    writer.input.put(8);
    writer.input.put(9);
    writer.input.put(6);
    writer.input.put(7);
    writer.endWindow();

    //Even file

    String evenFileName = testMeta.getDir() + File.separator + EVEN_FILE;

    String correctContents = "0\n" +
                             "2\n" +
                             "4\n";

    checkOutput(0,
                evenFileName,
                correctContents);

    correctContents = "6\n" +
                      "8\n" +
                      "6\n";

    checkOutput(1,
                evenFileName,
                correctContents);

    //Odd file

    String oddFileName = testMeta.getDir() + File.separator + ODD_FILE;

    correctContents = "1\n" +
                      "3\n" +
                      "5\n";

    checkOutput(0,
                oddFileName,
                correctContents);

    correctContents = "7\n" +
                      "9\n" +
                      "7\n";

    checkOutput(1,
                oddFileName,
                correctContents);
  }

  private void testMultiRollingFileCompletedWriteHelper(EvenOddHDFSExactlyOnceWriter writer)
  {
    writer.setMaxLength(4);
    File meta = new File(testMeta.getDir());
    writer.setFilePath(meta.getAbsolutePath());

    writer.setup(testOperatorContext);

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.input.put(2);
    writer.input.put(3);
    writer.input.put(4);
    writer.input.put(5);
    writer.endWindow();

    writer.beginWindow(1);
    writer.input.put(6);
    writer.input.put(7);
    writer.input.put(8);
    writer.input.put(9);
    writer.input.put(6);
    writer.input.put(7);
    writer.endWindow();

    //Even file

    String evenFileName = testMeta.getDir() + File.separator + EVEN_FILE;

    String correctContents = "0\n" +
                             "2\n" +
                             "4\n";

    checkOutput(0,
                evenFileName,
                correctContents);

    correctContents = "6\n" +
                      "8\n" +
                      "6\n";

    checkOutput(1,
                evenFileName,
                correctContents);

    //Odd file

    String oddFileName = testMeta.getDir() + File.separator + ODD_FILE;

    correctContents = "1\n" +
                      "3\n" +
                      "5\n";

    checkOutput(0,
                oddFileName,
                correctContents);

    correctContents = "7\n" +
                      "9\n" +
                      "7\n";

    checkOutput(1,
                oddFileName,
                correctContents);
  }

  @Test
  public void testMultiRollingFileFailedWrite()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();

    testMultiRollingFileFailedWriteHelperHelper(writer);
  }

  @Test
  public void testMultiRollingFileFailedWriteCache1()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setMaxOpenFiles(1);

    testMultiRollingFileFailedWriteHelperHelper(writer);
  }

  private void testMultiRollingFileFailedWriteHelperHelper(EvenOddHDFSExactlyOnceWriter writer)
  {
    testMultiRollingFileFailedWriteHelper(writer);

    //Even file

    String evenFileName = testMeta.getDir() + File.separator + EVEN_FILE;

    String correctContents = "0\n" +
                             "2\n" +
                             "4\n";

    checkOutput(0,
                evenFileName,
                correctContents);

    correctContents = "6\n" +
                      "8\n" +
                      "6\n";

    checkOutput(1,
                evenFileName,
                correctContents);

    //Odd file

    String oddFileName = testMeta.getDir() + File.separator + ODD_FILE;

    correctContents = "1\n" +
                      "3\n" +
                      "5\n";

    checkOutput(0,
                oddFileName,
                correctContents);

    correctContents = "7\n" +
                      "9\n" +
                      "7\n";

    checkOutput(1,
                oddFileName,
                correctContents);
  }

  private void testMultiRollingFileFailedWriteHelper(EvenOddHDFSExactlyOnceWriter writer)
  {
    File meta = new File(testMeta.getDir());
    writer.setFilePath(meta.getAbsolutePath());
    writer.setMaxLength(4);

    writer.setup(testOperatorContext);

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.endWindow();

    CheckPointWriter checkPointWriter = checkpoint(writer);

    writer.beginWindow(1);
    writer.input.put(2);
    writer.input.put(3);
    writer.teardown();

    restoreCheckPoint(checkPointWriter,
                      writer);
    writer.setup(testOperatorContext);

    writer.beginWindow(1);
    writer.input.put(2);
    writer.input.put(3);
    writer.endWindow();

    writer.beginWindow(2);
    writer.input.put(4);
    writer.input.put(5);
    writer.endWindow();

    writer.beginWindow(3);
    writer.input.put(6);
    writer.input.put(7);
    writer.input.put(8);
    writer.input.put(9);
    writer.input.put(6);
    writer.input.put(7);
    writer.endWindow();
  }

  @Test
  public void testMultiRollingFileFailedWriteOverwrite()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();

    testMultiRollingFileFailedWriteOverwriteHelper(writer);
  }

  @Test
  public void testMultiRollingFileFailedWriteOverwriteCache1()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setMaxOpenFiles(1);


    String evenFileName = testMeta.getDir() + File.separator + EVEN_FILE;
    String oddFileName = testMeta.getDir() + File.separator + ODD_FILE;

    populateFile(EVEN_FILE + ".0", "0\n" +
                                      "2\n" +
                                      "4\n");
    populateFile(ODD_FILE + ".0", "1\n" +
                                     "3\n" +
                                     "5\n");

    testMultiRollingFileFailedWriteOverwriteHelperCache1(writer);


    //Even file

    String correctContents = "0\n" +
                             "4\n" +
                             "6\n";
    checkOutput(0,
                evenFileName,
                correctContents);

    correctContents = "8\n" +
                      "6\n";
    checkOutput(1,
                evenFileName,
                correctContents);

    //Odd file

    correctContents = "1\n" +
                      "5\n" +
                      "7\n";
    checkOutput(0,
                oddFileName,
                correctContents);

    correctContents = "9\n" +
                      "7\n";
    checkOutput(1,
                oddFileName,
                correctContents);
  }

  private void testMultiRollingFileFailedWriteOverwriteHelperCache1(EvenOddHDFSExactlyOnceWriter writer)
  {
    File meta = new File(testMeta.getDir());
    writer.setFilePath(meta.getAbsolutePath());
    writer.setMaxLength(4);

    writer.setup(testOperatorContext);

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.endWindow();

    CheckPointWriter checkPointWriter = checkpoint(writer);

    writer.beginWindow(1);
    writer.input.put(2);
    writer.input.put(3);
    writer.teardown();

    restoreCheckPoint(checkPointWriter,
                      writer);
    writer.setup(testOperatorContext);

    writer.beginWindow(1);
    writer.input.put(4);
    writer.input.put(5);
    writer.endWindow();

    writer.beginWindow(2);
    writer.input.put(6);
    writer.input.put(7);
    writer.input.put(8);
    writer.input.put(9);
    writer.input.put(6);
    writer.input.put(7);
    writer.endWindow();
  }

  private void testMultiRollingFileFailedWriteOverwriteHelper(EvenOddHDFSExactlyOnceWriter writer)
  {
    String evenFileName = testMeta.getDir() + File.separator + EVEN_FILE;
    String oddFileName = testMeta.getDir() + File.separator + ODD_FILE;

    populateFile(EVEN_FILE + ".0", "0\n" +
                                      "2\n" +
                                      "4\n");
    populateFile(ODD_FILE + ".0", "1\n" +
                                     "3\n" +
                                     "5\n");

    File meta = new File(testMeta.getDir());
    writer.setFilePath(meta.getAbsolutePath());
    writer.setMaxLength(4);

    writer.setup(testOperatorContext);

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.endWindow();

    CheckPointWriter checkPointWriter = checkpoint(writer);

    writer.beginWindow(1);
    writer.input.put(2);
    writer.input.put(3);
    writer.teardown();

    restoreCheckPoint(checkPointWriter,
                      writer);
    writer.setup(testOperatorContext);

    writer.beginWindow(1);
    writer.input.put(4);
    writer.input.put(5);
    writer.endWindow();

    writer.beginWindow(2);
    writer.input.put(6);
    writer.input.put(7);
    writer.input.put(8);
    writer.input.put(9);
    writer.input.put(6);
    writer.input.put(7);
    writer.endWindow();

    //Even file

    String correctContents = "0\n" +
                             "4\n" +
                             "6\n";

    checkOutput(0,
                evenFileName,
                correctContents);

    correctContents = "8\n" +
                      "6\n";

    checkOutput(1,
                evenFileName,
                correctContents);

    //Odd file

    correctContents = "1\n" +
                      "5\n" +
                      "7\n";

    checkOutput(0,
                oddFileName,
                correctContents);

    correctContents = "9\n" +
                      "7\n";

    checkOutput(1,
                oddFileName,
                correctContents);
  }

  @Test
  public void singleFileMultiRollingFailure()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    File meta = new File(testMeta.getDir());
    writer.setFilePath(meta.getAbsolutePath());
    writer.setMaxLength(4);

    singleFileMultiRollingFailureHelper(writer);

    String singleFilePath = testMeta.getDir() + File.separator + SINGLE_FILE;

    //Rolling file 0

    String correctContents = "0\n" +
                             "1\n" +
                             "2\n";
    checkOutput(0,
                singleFilePath,
                correctContents);

    //Rolling file 1

    correctContents = "3\n" +
                      "4\n" +
                      "0\n";

    checkOutput(1,
                singleFilePath,
                correctContents);

    //Rolling file 2

    correctContents = "1\n" +
                      "2\n" +
                      "3\n";

    checkOutput(2,
                singleFilePath,
                correctContents);

    //Rolling file 3

    correctContents = "4\n" +
                      "5\n";

    checkOutput(3,
                singleFilePath,
                correctContents);
  }

  private void singleFileMultiRollingFailureHelper(SingleHDFSExactlyOnceWriter writer)
  {
    writer.setup(testOperatorContext);

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.input.put(2);
    writer.endWindow();

    writer.beginWindow(1);
    writer.input.put(3);
    writer.input.put(4);

    CheckPointWriter checkPointWriter = checkpoint(writer);

    writer.input.put(3);
    writer.input.put(4);
    writer.input.put(5);
    writer.endWindow();

    writer.beginWindow(2);
    writer.input.put(6);
    writer.input.put(7);
    writer.input.put(8);
    writer.endWindow();
    writer.teardown();

    restoreCheckPoint(checkPointWriter,
                      writer);
    writer.setup(testOperatorContext);

    writer.beginWindow(1);
    writer.input.put(0);
    writer.input.put(1);
    writer.input.put(2);
    writer.endWindow();

    writer.beginWindow(2);
    writer.input.put(3);
    writer.input.put(4);
    writer.input.put(5);
    writer.endWindow();
  }

  @Test
  public void validateNothingWrongTest()
  {
    ValidationTestApp validationTestApp = new ValidationTestApp(new File(testMeta.getDir()),
                                                                null,
                                                                new SingleHDFSByteExactlyOnceWriter());

    LocalMode.runApp(validationTestApp, 1);
  }

  @Test
  public void validateNegativeMaxLengthTest()
  {
    ValidationTestApp validationTestApp = new ValidationTestApp(new File(testMeta.getDir()),
                                                                -1L,
                                                                new SingleHDFSByteExactlyOnceWriter());

    boolean error = false;

    try {
      LocalMode.runApp(validationTestApp, 1);
    }
    catch(RuntimeException e) {
      if(e.getCause() instanceof ConstraintViolationException) {
        error = true;
      }
    }

    Assert.assertEquals("Max length validation not thrown with -1 max length", true, error);
  }
  
  @Test
  public void testPeriodicRotation()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    File dir = new File(testMeta.getDir());
    writer.setFilePath(testMeta.getDir());
    writer.setRotationWindows(30);
    writer.setup(testOperatorContext);

    // Check that rotation doesn't happen prematurely
    for (int i = 0; i < 30; ++i) {
      writer.beginWindow(i);
      for (int j = 0; j < i; ++j) {
        writer.input.put(2 * j + 1);
      }
      writer.endWindow();      
    }
    Set<String> fileNames = new TreeSet<String>();
    fileNames.add(ODD_FILE + ".0");
    Collection<File> files = FileUtils.listFiles(dir, null, false);
    Assert.assertEquals("Number of part files", 1, files.size());
    Assert.assertEquals("Part file names", fileNames, getFileNames(files));

    // Check that rotation is happening consistently and for all files
    for (int i = 30; i < 120; ++i) {
      writer.beginWindow(i);
      for (int j = 0; j < i; ++j) {
        writer.input.put(j);
      }
      writer.endWindow();
    }
    files = FileUtils.listFiles(dir, null, false);
    Assert.assertEquals("Number of part files", 7, files.size());
    for (int i = 0; i < 3; ++i) {
      fileNames.add(EVEN_FILE + "." + i);
    }
    for (int i = 1; i < 4; ++i) {
      fileNames.add(ODD_FILE + "." + i);
    }
    Assert.assertEquals("Part file names", fileNames, getFileNames(files));

    // Check that rotation doesn't happen for files that don't have data during the rotation period
    for (int i = 120; i < 180; ++i) {
      writer.beginWindow(i);
      for (int j = 0; j < i; ++j) {
        writer.input.put(j * 2);
      }
      writer.endWindow();
    }
    files = FileUtils.listFiles(dir, null, false);
    Assert.assertEquals("Number of part files", 9, files.size());
    for (int i = 3; i < 5; ++i) {
      fileNames.add(EVEN_FILE + "." + i);
    }
    Assert.assertEquals("Part file names", fileNames, getFileNames(files));
    writer.teardown();
  }

  @Test
  public void testCompression() throws IOException
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setFilterStreamProvider(new FilterStreamCodec.GZipFilterStreamProvider());

    File evenFile = new File(testMeta.getDir(), EVEN_FILE);
    File oddFile = new File(testMeta.getDir(), ODD_FILE);

    // To get around the multi member gzip issue with openjdk
    // http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4691425
    List<Long> evenOffsets = new ArrayList<Long>();
    List<Long> oddOffsets = new ArrayList<Long>();
    
    writer.setFilePath(testMeta.getDir());
    writer.setup(testOperatorContext);

    for (int i = 0; i < 10; ++i) {
      writer.beginWindow(i);
      for (int j = 0; j < 1000; ++j) {
        writer.input.put(i);
      }
      writer.endWindow();
      evenOffsets.add(evenFile.length());
      oddOffsets.add(oddFile.length());
    }

    writer.teardown();

    checkCompressedFile(evenFile, evenOffsets, 0, 5, 1000, null, null);
    checkCompressedFile(oddFile, oddOffsets, 1, 5, 1000, null, null);
  }

  private void checkCompressedFile(File file, List<Long> offsets, int startVal, int totalWindows, int totalRecords, SecretKey secretKey, byte[] iv) throws IOException
  {
    FileInputStream fis;
    InputStream gss = null;
    GZIPInputStream gis = null;
    BufferedReader br = null;

    Cipher cipher = null;
    if (secretKey != null) {
      try {
        cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        IvParameterSpec ivps = new IvParameterSpec(iv);
        cipher.init(Cipher.DECRYPT_MODE, secretKey, ivps);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    
    int numWindows = 0;
    try {
      fis = new FileInputStream(file);
      //fis.skip(startOffset);
      gss = fis;
      if (secretKey != null) {
        try {
            /*
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            IvParameterSpec ivps = new IvParameterSpec(iv);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, ivps);
            */
          gss = new CipherInputStream(fis, cipher);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      
      long startOffset = 0;
      for (long offset : offsets) {
        // Skip initial case in case file is not yet created
        if (offset == 0) {
          continue;
        }
        long limit = offset - startOffset;
        LimitInputStream lis = new LimitInputStream(gss, limit);

        //gis = new GZIPInputStream(fis);
        gis = new GZIPInputStream(lis);
        br = new BufferedReader(new InputStreamReader(gis));
        //br = new BufferedReader(new InputStreamReader(gss));
        String eline = "" + (startVal + numWindows * 2);
        int count = 0;
        String line;
        while ((line = br.readLine()) != null) {
          Assert.assertEquals("File line", eline, line);
          ++count;
          //System.out.println("line " + line + " " + count);
          if ((count % totalRecords) == 0) {
            ++numWindows;
            //System.out.println("numWindows " + numWindows);
            eline = "" + (startVal + numWindows * 2);
          }
        }
        /*
        if (count > 0) {
          Assert.assertEquals("Event count", 1000, count);
          ++numWindows;
        }
        */
        startOffset = offset;
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        br.close();
      } else {
        if (gis != null) {
          gis.close();
        } else if (gss != null) {
          gss.close();
        }
      }
    }
    Assert.assertEquals("Total", totalWindows, numWindows);
  }

  @Test
  public void testChainFilters() throws NoSuchAlgorithmException, IOException
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    KeyGenerator keygen = KeyGenerator.getInstance("AES");
    keygen.init(128);
    final SecretKey secretKey = keygen.generateKey();
    byte[] iv = "TestParam16bytes".getBytes();
    final IvParameterSpec ivps = new IvParameterSpec(iv);
    FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream> chainStreamProvider
            = new FilterStreamProvider.FilterChainStreamProvider<FilterOutputStream, OutputStream>();
    chainStreamProvider.addStreamProvider(new FilterStreamCodec.GZipFilterStreamProvider());

    // The filter is to keep track of the offsets to handle multi member gzip issue with openjdk
    // http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4691425
    final CounterFilterStreamContext evenCounterContext = new CounterFilterStreamContext();
    final CounterFilterStreamContext oddCounterContext = new CounterFilterStreamContext();
    chainStreamProvider.addStreamProvider(new FilterStreamProvider.SimpleFilterReusableStreamProvider<CounterFilterOutputStream, OutputStream>()
    {
      @Override
      protected FilterStreamContext<CounterFilterOutputStream> createFilterStreamContext(OutputStream outputStream) throws IOException
      {
        if (evenCounterContext.isDoInit()) {
          evenCounterContext.init(outputStream);
          return evenCounterContext;
        } else {
          oddCounterContext.init(outputStream);
          return oddCounterContext;
        }
      }
    });
    chainStreamProvider.addStreamProvider(new FilterStreamProvider.SimpleFilterReusableStreamProvider<CipherOutputStream, OutputStream>()
    {
      @Override
      protected FilterStreamContext<CipherOutputStream> createFilterStreamContext(OutputStream outputStream) throws IOException
      {
        try {
          Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
          cipher.init(Cipher.ENCRYPT_MODE, secretKey, ivps);
          return new FilterStreamCodec.CipherFilterStreamContext(outputStream, cipher);
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    });
    writer.setFilterStreamProvider(chainStreamProvider);

    File evenFile = new File(testMeta.getDir(), EVEN_FILE);
    File oddFile = new File(testMeta.getDir(), ODD_FILE);

    List<Long> evenOffsets = new ArrayList<Long>();
    List<Long> oddOffsets = new ArrayList<Long>();

    writer.setFilePath(testMeta.getDir());
    writer.setup(testOperatorContext);

    for (int i = 0; i < 10; ++i) {
      writer.beginWindow(i);
      for (int j = 0; j < 1000; ++j) {
        writer.input.put(i);
      }
      writer.endWindow();
      evenOffsets.add(evenCounterContext.getCounter());
      oddOffsets.add(oddCounterContext.getCounter());
      //evenOffsets.add(evenFile.length());
      //oddOffsets.add(oddFile.length());
    }

    writer.teardown();

    /*
    evenOffsets.add(evenFile.length());
    oddOffsets.add(oddFile.length());
    */

    checkCompressedFile(evenFile, evenOffsets, 0, 5, 1000, secretKey, iv);
    checkCompressedFile(oddFile, oddOffsets, 1, 5, 1000, secretKey, iv);
  }

  private Set<String> getFileNames(Collection<File> files)
  {
    Set<String> filesNames = new TreeSet<String>();
    for (File file : files) {
      filesNames.add(file.getName());
    }
    return filesNames;
  }

  private static class CounterFilterStreamContext implements FilterStreamContext<CounterFilterOutputStream>
  {

    private CounterFilterOutputStream counterStream;
    
    public void init(OutputStream outputStream) {
      counterStream = new CounterFilterOutputStream(outputStream);
    }
    
    public boolean isDoInit() 
    {
      return (counterStream == null);
    }

    @Override
    public CounterFilterOutputStream getFilterStream()
    {
      return counterStream;
    }

    @Override
    public void finalizeContext() throws IOException
    {

    }
    
    public long getCounter() {
      if (isDoInit()) {
        return 0;
      } else {
        return counterStream.getCounter();
      }
      
    }
  }
  
  private static class CounterFilterOutputStream extends FilterOutputStream
  {
    long counter;
    int refCount;
    
    public CounterFilterOutputStream(OutputStream out)
    {
      super(out);
    }
    
    @Override
    public void write(int b) throws IOException
    {
      ++refCount;
      super.write(b);
      --refCount;
      if (refCount == 0) {
        counter += 1;
      }
    }

    @Override
    public void write(@Nonnull byte[] b) throws IOException
    {
      ++refCount;
      super.write(b);
      --refCount;
      if (refCount == 0) {
        counter += b.length;
      }
    }

    @Override
    public void write(@Nonnull byte[] b, int off, int len) throws IOException
    {
      ++refCount;
      super.write(b, off, len);
      --refCount;
      if (refCount == 0) {
        counter += len;
      }
    }

    public long getCounter()
    {
      return counter;
    }
  }
  
}
