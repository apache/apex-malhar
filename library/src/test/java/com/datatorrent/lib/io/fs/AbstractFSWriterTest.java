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

import com.datatorrent.api.*;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.RandomWordGenerator;
import com.datatorrent.lib.util.TestUtils.TestInfo;
import com.google.common.collect.Maps;
import java.io.*;
import javax.validation.ConstraintViolationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.*;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractFSWriterTest
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFSWriterTest.class);

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
  private static class EvenOddHDFSExactlyOnceWriter extends AbstractFSWriter<Integer>
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
  private static class SingleHDFSExactlyOnceWriter extends AbstractFSWriter<Integer>
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
  private static class SingleHDFSByteExactlyOnceWriter extends AbstractFSWriter<byte[]>
  {
    public SingleHDFSByteExactlyOnceWriter()
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
  public static class CheckPointWriter extends AbstractFSWriter
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
    @SuppressWarnings("rawtypes")
    private final AbstractFSWriter<byte[]> fsWriter;

    public ValidationTestApp(File testDir,
                             Long maxLength,
                             AbstractFSWriter<byte[]> fsWriter)
    {
      this.testDir = testDir;
      this.maxLength = maxLength;
      this.fsWriter = fsWriter;
    }

    @Override
    @SuppressWarnings("unchecked")
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
      DTThrowable.rethrow(ex);
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
  public static CheckPointWriter checkpoint(AbstractFSWriter writer)
  {
    CheckPointWriter checkPointWriter = new CheckPointWriter();
    checkPointWriter.append = writer.append;
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
                                       AbstractFSWriter writer)
  {
    writer.append = checkPointWriter.append;
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

  @SuppressWarnings("deprecation")
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

  //@Ignore
  @Test
  public void testSingleFileCompletedWrite()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testSingleFileCompletedWriteHelper(writer,
                                       ProcessingMode.EXACTLY_ONCE);

    String singleFileName = testMeta.getDir() + File.separator + SINGLE_FILE;

    String correctContents = "0\n" +
                             "1\n" +
                             "2\n" +
                             "3\n";

    checkOutput(-1,
                singleFileName,
                correctContents);
  }

  //@Ignore
  @Test
  public void testSingleFileCompletedWriteInitial()
  {
    populateFile(SINGLE_FILE,
                 "0\n" +
                 "1\n" +
                 "2\n");

    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testSingleFileCompletedWriteHelper(writer,
                                       ProcessingMode.EXACTLY_ONCE);

    String singleFileName = testMeta.getDir() + File.separator + SINGLE_FILE;

    String correctContents = "0\n" +
                             "1\n" +
                             "2\n" +
                             "0\n" +
                             "1\n" +
                             "2\n" +
                             "3\n";

    checkOutput(-1,
                singleFileName,
                correctContents);
  }

  //@Ignore
  @Test
  public void testSingleFileCompletedWriteOverwrite()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(false);

    testSingleFileCompletedWriteHelper(writer,
                                       ProcessingMode.EXACTLY_ONCE);

    String singleFileName = testMeta.getDir() + File.separator + SINGLE_FILE;

    String correctContents = "0\n" +
                             "1\n" +
                             "2\n" +
                             "3\n";

    checkOutput(-1,
                singleFileName,
                correctContents);
  }

  //@Ignore
  @Test
  public void testSingleFileCompletedWriteOverwriteInitial()
  {
    populateFile(SINGLE_FILE,
                 "0\n" +
                 "1\n" +
                 "2\n");

    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(false);

    testSingleFileCompletedWriteHelper(writer,
                                       ProcessingMode.EXACTLY_ONCE);

    String singleFileName = testMeta.getDir() + File.separator + SINGLE_FILE;

    String correctContents = "0\n" +
                             "1\n" +
                             "2\n" +
                             "3\n";

    checkOutput(-1,
                singleFileName,
                correctContents);
  }

  private void testSingleFileCompletedWriteHelper(SingleHDFSExactlyOnceWriter writer,
                                                  ProcessingMode mode)
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

  //@Ignore
  @Test
  public void testSingleFileFailedWrite()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testSingleFileFailedWriteHelper(writer,
                                    ProcessingMode.EXACTLY_ONCE);

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

 //@Ignore
  @Test
  public void testSingleFileFailedWriteInitial()
  {
    populateFile(SINGLE_FILE,
                 "0\n" +
                 "1\n" +
                 "2\n");

    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testSingleFileFailedWriteHelper(writer,
                                    ProcessingMode.EXACTLY_ONCE);

    String singleFileName = testMeta.getDir() + File.separator + SINGLE_FILE;

    String correctContents = "0\n" +
                             "1\n" +
                             "2\n" +
                             "0\n" +
                             "1\n" +
                             "4\n" +
                             "5\n" +
                             "6\n" +
                             "7\n";

    checkOutput(-1,
                singleFileName,
                correctContents);
  }

  //@Ignore
  @Test
  public void testSingleFileFailedWriteOverwrite()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(false);

    testSingleFileFailedWriteHelper(writer,
                                    ProcessingMode.EXACTLY_ONCE);

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

  //@Ignore
  @Test
  public void testSingleFileFailedWriteOverwriteInitial()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    populateFile(SINGLE_FILE,
                 "0\n" +
                 "1\n" +
                 "2\n");

    writer.setAppend(false);
    testSingleFileFailedWriteHelper(writer,
                                    ProcessingMode.EXACTLY_ONCE);

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

  private void testSingleFileFailedWriteHelper(SingleHDFSExactlyOnceWriter writer,
                                               ProcessingMode mode)
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

  //@Ignore
  @Test
  public void testMultiFileCompletedWrite()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testMultiFileCompletedWriteHelper(writer,
                                      ProcessingMode.EXACTLY_ONCE);

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
  public void testMultiFileCompletedWriteInitial()
  {
    populateFile(EVEN_FILE,
                 "0\n" +
                 "2\n");

    populateFile(ODD_FILE,
                 "1\n" +
                 "3\n");

    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testMultiFileCompletedWriteHelper(writer,
                                      ProcessingMode.EXACTLY_ONCE);

    String evenFileName = testMeta.getDir() + File.separator + EVEN_FILE;

    String correctContents = "0\n" +
                             "2\n" +
                             "0\n" +
                             "2\n" +
                             "4\n" +
                             "6\n";

    checkOutput(-1,
                evenFileName,
                correctContents);

    String oddFileName = testMeta.getDir() + File.separator + ODD_FILE;

    correctContents = "1\n" +
                      "3\n" +
                      "1\n" +
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
    writer.setAppend(true);
    writer.setMaxOpenFiles(1);

    testMultiFileCompletedWriteHelper(writer,
                                      ProcessingMode.EXACTLY_ONCE);

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
  public void testMultiFileCompletedWriteCache1Initial()
  {
    populateFile(EVEN_FILE,
                 "0\n" +
                 "2\n");

    populateFile(ODD_FILE,
                 "1\n" +
                 "3\n");

    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(true);
    writer.setMaxOpenFiles(1);

    testMultiFileCompletedWriteHelper(writer,
                                      ProcessingMode.EXACTLY_ONCE);

    String evenFileName = testMeta.getDir() + File.separator + EVEN_FILE;

    String correctContents = "0\n" +
                             "2\n" +
                             "0\n" +
                             "2\n" +
                             "4\n" +
                             "6\n";

    checkOutput(-1,
                evenFileName,
                correctContents);

    String oddFileName = testMeta.getDir() + File.separator + ODD_FILE;

    correctContents = "1\n" +
                      "3\n" +
                      "1\n" +
                      "3\n" +
                      "5\n" +
                      "7\n";

    checkOutput(-1,
                oddFileName,
                correctContents);
  }

  //@Ignore
  @Test
  public void testMultiFileCompletedWriteOverwrite()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(false);

    testMultiFileCompletedWriteHelper(writer,
                                      ProcessingMode.EXACTLY_ONCE);

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
    writer.setAppend(false);

    testMultiFileCompletedWriteHelper(writer,
                                      ProcessingMode.EXACTLY_ONCE);

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
  public void testMultiFileCompletedWriteOverwriteCache1()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(false);
    writer.setMaxOpenFiles(1);

    testMultiFileCompletedWriteHelperCache1(writer,
                                            ProcessingMode.EXACTLY_ONCE);

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
  public void testMultiFileCompletedWriteOverwriteCache1Initial()
  {
    populateFile(EVEN_FILE,
                 "0\n" +
                 "2\n");

    populateFile(ODD_FILE,
                 "1\n" +
                 "3\n");

    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(false);
    writer.setMaxOpenFiles(1);

    testMultiFileCompletedWriteHelperCache1(writer,
                                            ProcessingMode.EXACTLY_ONCE);

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

  private void testMultiFileCompletedWriteHelperCache1(EvenOddHDFSExactlyOnceWriter writer,
                                                       ProcessingMode mode)
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

  private void testMultiFileCompletedWriteHelper(EvenOddHDFSExactlyOnceWriter writer,
                                                 ProcessingMode mode)
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

  //@Ignore
  @Test
  public void testMultiFileFailedWrite()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testMultiFileFailedWriteHelper(writer, ProcessingMode.EXACTLY_ONCE);

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

  //@Ignore
  @Test
  public void testMultiFileFailedWriteCache1()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(true);
    writer.setMaxOpenFiles(1);

    testMultiFileFailedWriteHelper(writer, ProcessingMode.EXACTLY_ONCE);

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

  //@Ignore
  @Test
  public void testMultiFileFailedWriteOverwrite()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(false);

    testMultiFileFailedWriteHelper(writer, ProcessingMode.EXACTLY_ONCE);

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

  //@Ignore
  @Test
  public void testMultiFileFailedWriteOverwriteCache1()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(false);
    writer.setMaxOpenFiles(1);

    testMultiFileFailedWriteHelper(writer, ProcessingMode.EXACTLY_ONCE);

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

  private void testMultiFileFailedWriteHelper(EvenOddHDFSExactlyOnceWriter writer,
                                              ProcessingMode mode)
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

  //@Ignore
  @Test
  public void testSingleRollingFileCompletedWrite()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testSingleRollingFileCompletedWriteHelper(writer,
                                              ProcessingMode.EXACTLY_ONCE);

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

  //@Ignore
  @Test
  public void testSingleRollingFileCompletedWriteInitial()
  {
    populateFile(SINGLE_FILE + ".0",
                 "0\n");

    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testSingleRollingFileCompletedWriteHelper(writer,
                                              ProcessingMode.EXACTLY_ONCE);

    String singleFileName = testMeta.getDir() + File.separator + SINGLE_FILE;

    //Rolling file 0

    String correctContents = "0\n" +
                             "0\n" +
                             "1\n";

    checkOutput(0,
                singleFileName,
                correctContents);

    //Rolling file 0

    correctContents = "2\n" +
                      "3\n" +
                      "4\n";

    checkOutput(1,
                singleFileName,
                correctContents);

    //Rolling file 1

    correctContents = "5\n";

    checkOutput(2,
                singleFileName,
                correctContents);
  }

  //@Ignore
  @Test
  public void testSingleRollingFileCompletedWriteInitialMax()
  {
    populateFile(SINGLE_FILE + ".0",
                 "0\n" +
                 "1\n" +
                 "2\n");

    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testSingleRollingFileCompletedWriteHelper(writer,
                                              ProcessingMode.EXACTLY_ONCE);

    String singleFileName = testMeta.getDir() + File.separator + SINGLE_FILE;

    //Rolling file 0

    String correctContents = "0\n" +
                             "1\n" +
                             "2\n";

    checkOutput(0,
                singleFileName,
                correctContents);

    //Rolling file 0

    correctContents = "0\n" +
                      "1\n" +
                      "2\n";

    checkOutput(1,
                singleFileName,
                correctContents);

    //Rolling file 1

    correctContents = "3\n" +
                      "4\n" +
                      "5\n";

    checkOutput(2,
                singleFileName,
                correctContents);
  }

  //@Ignore
  @Test
  public void testSingleRollingFileCompletedWriteOverwrite()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(false);

    testSingleRollingFileCompletedWriteHelper(writer,
                                              ProcessingMode.EXACTLY_ONCE);

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

  //@Ignore
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
    writer.setAppend(false);

    testSingleRollingFileCompletedWriteHelper(writer,
                                              ProcessingMode.EXACTLY_ONCE);

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

  private void testSingleRollingFileCompletedWriteHelper(SingleHDFSExactlyOnceWriter writer,
                                                         ProcessingMode mode)
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

  //@Ignore
  @Test
  public void testSingleRollingFileFailedWrite()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testSingleRollingFileFailedWriteHelper(writer,
                                           ProcessingMode.EXACTLY_ONCE);

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

  private void testSingleRollingFileFailedWriteHelper(SingleHDFSExactlyOnceWriter writer,
                                                      ProcessingMode mode)
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

  //@Ignore
  @Test
  public void testSingleRollingFileFailedWriteOverwrite()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(false);

    testSingleRollingFileFailedWriteOverwriteHelper(writer,
                                                    ProcessingMode.EXACTLY_ONCE);

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
                      "3\n";

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

  private void testSingleRollingFileFailedWriteOverwriteHelper(SingleHDFSExactlyOnceWriter writer,
                                                               ProcessingMode mode)
  {
    writer.setMaxLength(4);
    writer.setFilePath(testMeta.getDir());
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

    writer.teardown();

    restoreCheckPoint(checkPointWriter,
                      writer);
    writer.setup(testOperatorContext);

    writer.beginWindow(1);
    writer.input.put(3);
    writer.endWindow();

    writer.beginWindow(2);
    writer.input.put(6);
    writer.input.put(7);
    writer.input.put(8);
    writer.endWindow();

    writer.teardown();
  }

  //@Ignore
  @Test
  public void testSingleRollingFileFailedWrite1()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(true);
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
    writer.setAppend(true);

    testMultiRollingFileCompletedWriteHelper(writer, ProcessingMode.EXACTLY_ONCE);
  }

  //@Ignore
  @Test
  public void testMultiRollingFileCompletedWriteCache1()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(true);
    writer.setMaxOpenFiles(1);

    testMultiRollingFileCompletedWriteHelper(writer, ProcessingMode.EXACTLY_ONCE);
  }

  //@Ignore
  @Test
  public void testMultiRollingFileCompletedWriteOverwrite()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(false);

    testMultiRollingFileCompletedWriteHelper(writer, ProcessingMode.EXACTLY_ONCE);
  }

  //@Ignore
  @Test
  public void testMultiRollingFileCompletedWriteOverwriteCache1()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(false);
    writer.setMaxOpenFiles(1);

    testMultiRollingFileCompletedWriteHelperCache1(writer, ProcessingMode.EXACTLY_ONCE);
  }

  private void testMultiRollingFileCompletedWriteHelperCache1(EvenOddHDFSExactlyOnceWriter writer,
                                                              ProcessingMode mode)
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

  private void testMultiRollingFileCompletedWriteHelper(EvenOddHDFSExactlyOnceWriter writer,
                                                        ProcessingMode mode)
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

  //@Ignore
  @Test
  public void testMultiRollingFileFailedWrite()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testMultiRollingFileFailedWriteHelperHelper(writer,
                                                ProcessingMode.EXACTLY_ONCE);
  }

  //@Ignore
  @Test
  public void testMultiRollingFileFailedWriteCache1()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(true);
    writer.setMaxOpenFiles(1);

    testMultiRollingFileFailedWriteHelperHelper(writer,
                                                ProcessingMode.EXACTLY_ONCE);
  }

  private void testMultiRollingFileFailedWriteHelperHelper(EvenOddHDFSExactlyOnceWriter writer,
                                                           ProcessingMode mode)
  {
    testMultiRollingFileFailedWriteHelper(writer,
                                          mode);

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

  private void testMultiRollingFileFailedWriteHelper(EvenOddHDFSExactlyOnceWriter writer,
                                                     ProcessingMode mode)
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

  //@Ignore
  @Test
  public void testMultiRollingFileFailedWriteOverwrite()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(false);

    testMultiRollingFileFailedWriteOverwriteHelper(writer,
                                                   ProcessingMode.EXACTLY_ONCE);
  }

  //@Ignore
  @Test
  public void testMultiRollingFileFailedWriteOverwriteCache1()
  {
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(false);
    writer.setMaxOpenFiles(1);

    testMultiRollingFileFailedWriteOverwriteHelperCache1(writer,
                                                         ProcessingMode.EXACTLY_ONCE);


    //Even file

    String evenFileName = testMeta.getDir() + File.separator + EVEN_FILE;
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

    String oddFileName = testMeta.getDir() + File.separator + ODD_FILE;

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

  private void testMultiRollingFileFailedWriteOverwriteHelperCache1(EvenOddHDFSExactlyOnceWriter writer,
                                                                    ProcessingMode mode)
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

  private void testMultiRollingFileFailedWriteOverwriteHelper(EvenOddHDFSExactlyOnceWriter writer,
                                                              ProcessingMode mode)
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

    //Even file

    String evenFileName = testMeta.getDir() + File.separator + EVEN_FILE;

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

    String oddFileName = testMeta.getDir() + File.separator + ODD_FILE;

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

  //@Ignore
  @Test
  public void singleFileMultiRollingFailure()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    File meta = new File(testMeta.getDir());
    writer.setFilePath(meta.getAbsolutePath());
    writer.setAppend(true);
    writer.setMaxLength(4);

    singleFileMultiRollingFailureHelper(writer,
                                        ProcessingMode.EXACTLY_ONCE);

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

  //@Ignore
  @Test
  public void singleFileMultiRollingFailureOverwrite()
  {
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    File meta = new File(testMeta.getDir());
    writer.setFilePath(meta.getAbsolutePath());
    writer.setAppend(false);
    writer.setMaxLength(4);

    singleFileMultiRollingFailureHelper(writer,
                                        ProcessingMode.EXACTLY_ONCE);

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

    //Rolling file 2

    correctContents = "4\n" +
                      "5\n";

    checkOutput(3,
                singleFilePath,
                correctContents);
  }

  private void singleFileMultiRollingFailureHelper(SingleHDFSExactlyOnceWriter writer,
                                                   ProcessingMode mode)
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

  //@Ignore
  @Test
  public void validateNothingWrongTest()
  {
    ValidationTestApp validationTestApp = new ValidationTestApp(new File(testMeta.getDir()),
                                                                null,
                                                                new SingleHDFSByteExactlyOnceWriter());

    LocalMode.runApp(validationTestApp, 1);
  }

  //@Ignore
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
}
