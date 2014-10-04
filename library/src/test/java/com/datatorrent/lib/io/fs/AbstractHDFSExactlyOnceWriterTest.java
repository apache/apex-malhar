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

import com.datatorrent.common.util.DTThrowable;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractHDFSExactlyOnceWriterTest
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractHDFSExactlyOnceWriterTest.class);

  private static final String SINGLE_FILE = "single.txt";
  private static final String EVEN_FILE = "even.txt";
  private static final String ODD_FILE = "odd.txt";

  @Rule public IOTestHelper testMeta = new IOTestHelper();

  /**
   * Simple writer which writes to two files.
   */
  private static class EvenOddHDFSExactlyOnceWriter extends AbstractHDFSExactlyOnceWriter<Integer, Integer>
  {
    @Override
    protected FileSystem getFSInstance() throws IOException
    {
      return FileSystem.getLocal(new Configuration()).getRaw();
    }

    @Override
    protected Integer convert(Integer tuple)
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
  private static class SingleHDFSExactlyOnceWriter extends AbstractHDFSExactlyOnceWriter<Integer, Integer>
  {
    @Override
    protected FileSystem getFSInstance() throws IOException
    {
      return FileSystem.getLocal(new Configuration()).getRaw();
    }

    @Override
    protected Integer convert(Integer tuple)
    {
      return tuple;
      //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
   * Dummy writer to store checkpointed state
   */
  private static class CheckPointWriter extends AbstractHDFSExactlyOnceWriter<Integer, Integer>
  {
    @Override
    protected FileSystem getFSInstance() throws IOException
    {
      return FileSystem.getLocal(new Configuration()).getRaw();
    }

    @Override
    protected Integer convert(Integer tuple)
    {
      //This is a dummy operator for checkpointing
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected String getFileName(Integer tuple)
    {
      //This is a dummy operator for checkpointing
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected byte[] getBytesForTuple(Integer tuple)
    {
      //This is a dummy operator for checkpointing
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }

  private void prepareTest()
  {
    File testDir = new File(testMeta.dir);
    FileUtils.deleteQuietly(testDir);

    testDir.mkdir();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private CheckPointWriter checkpoint(AbstractHDFSExactlyOnceWriter<Integer, Integer> writer)
  {
    CheckPointWriter checkPointWriter = new CheckPointWriter();
    checkPointWriter.append = writer.append;
    checkPointWriter.counts = Maps.newHashMap();

    for(String keys: writer.counts.keySet()) {
      checkPointWriter.counts.put(keys,
                                  new MutableLong(writer.counts.get(keys).longValue()));
    }

    checkPointWriter.endOffsets = Maps.newHashMap();

    for(String keys: writer.endOffsets.keySet()) {
      checkPointWriter.endOffsets.put(keys, new MutableLong(writer.endOffsets.get(keys).longValue()));
    }

    checkPointWriter.openPart = Maps.newHashMap();

    for(String keys: writer.openPart.keySet()) {
      checkPointWriter.openPart.put(keys,
                                    new MutableInt(writer.openPart.get(keys).intValue()));
    }

    checkPointWriter.filePath = writer.filePath;
    checkPointWriter.maxOpenFiles = writer.maxOpenFiles;
    checkPointWriter.replication = writer.replication;
    checkPointWriter.totalBytesWritten = writer.totalBytesWritten;
    checkPointWriter.maxLength = writer.maxLength;
    checkPointWriter.rollingFile = writer.rollingFile;

    return checkPointWriter;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void restoreCheckPoint(CheckPointWriter checkPointWriter,
                                 AbstractHDFSExactlyOnceWriter<Integer, Integer> writer)
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
                        "output contents",
                        expectedOutput,
                        fileContents);
  }

  @Test
  public void testSingleFileCompletedWrite()
  {
    prepareTest();
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testSingleFileCompletedWriteHelper(writer);
  }

  @Test
  public void testSingleFileCompletedWriteOverwrite()
  {
    prepareTest();
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(false);

    testSingleFileCompletedWriteHelper(writer);
  }

  private void testSingleFileCompletedWriteHelper(SingleHDFSExactlyOnceWriter writer)
  {
    writer.setFilePath(testMeta.dir);

    writer.setup(new DummyContext(0));

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.endWindow();

    writer.beginWindow(1);
    writer.input.put(2);
    writer.input.put(3);
    writer.endWindow();

    writer.teardown();

    String singleFileName = testMeta.dir + File.separator + SINGLE_FILE;

    String correctContents = "0\n" +
                             "1\n" +
                             "2\n" +
                             "3\n";

    checkOutput(-1,
                singleFileName,
                correctContents);
  }

  @Test
  public void testSingleFileFailedWrite()
  {
    prepareTest();
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testSingleFileFailedWriteHelper(writer);

    String singleFileName = testMeta.dir + File.separator + SINGLE_FILE;

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
  public void testSingleFileFailedWriteOverwrite()
  {
    prepareTest();
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(false);

    testSingleFileFailedWriteHelper(writer);

    String singleFileName = testMeta.dir + File.separator + SINGLE_FILE;

    String correctContents = "4\n" +
                             "5\n" +
                             "6\n" +
                             "7\n";

    checkOutput(-1,
                singleFileName,
                correctContents);
  }

  private void testSingleFileFailedWriteHelper(SingleHDFSExactlyOnceWriter writer)
  {
    File meta = new File(testMeta.dir);
    writer.setFilePath(meta.getAbsolutePath());

    writer.setup(new DummyContext(0));

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
    writer.setup(new DummyContext(0));

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
    prepareTest();
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testMultiFileCompletedWriteHelper(writer);
  }

  @Test
  public void testMultiFileCompletedWriteOverwrite()
  {
    prepareTest();
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(false);

    testMultiFileCompletedWriteHelper(writer);
  }

  private void testMultiFileCompletedWriteHelper(EvenOddHDFSExactlyOnceWriter writer)
  {
    File meta = new File(testMeta.dir);
    writer.setFilePath(meta.getAbsolutePath());

    writer.setup(new DummyContext(0));

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

    String evenFileName = testMeta.dir + File.separator + EVEN_FILE;

    String correctContents = "0\n" +
                             "2\n" +
                             "4\n" +
                             "6\n";

    checkOutput(-1,
                evenFileName,
                correctContents);

    String oddFileName = testMeta.dir + File.separator + ODD_FILE;

    correctContents = "1\n" +
                      "3\n" +
                      "5\n" +
                      "7\n";

    checkOutput(-1,
                oddFileName,
                correctContents);
  }

  @Test
  public void testMultiFileFailedWrite()
  {
    prepareTest();
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testMultiFileFailedWriteHelper(writer);

    String evenFileName = testMeta.dir + File.separator + EVEN_FILE;

    String correctContents = "0\n" +
                             "2\n" +
                             "6\n" +
                             "8\n";

    checkOutput(-1,
                evenFileName,
                correctContents);

    String oddFileName = testMeta.dir + File.separator + ODD_FILE;

    correctContents = "1\n" +
                      "3\n" +
                      "7\n" +
                      "9\n";

    checkOutput(-1,
                oddFileName,
                correctContents);
  }

  @Test
  public void testMultiFileFailedWriteOverwrite()
  {
    prepareTest();
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(false);

    testMultiFileFailedWriteHelper(writer);

    String evenFileName = testMeta.dir + File.separator + EVEN_FILE;

    String correctContents = "6\n" +
                             "8\n";

    checkOutput(-1,
                evenFileName,
                correctContents);

    String oddFileName = testMeta.dir + File.separator + ODD_FILE;

    correctContents = "7\n" +
                      "9\n";

    checkOutput(-1,
                oddFileName,
                correctContents);
  }

  private void testMultiFileFailedWriteHelper(EvenOddHDFSExactlyOnceWriter writer)
  {
    File meta = new File(testMeta.dir);
    writer.setFilePath(meta.getAbsolutePath());

    writer.setup(new DummyContext(0));

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
    writer.setup(new DummyContext(0));

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
    prepareTest();
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(true);

    testSingleRollingFileCompletedWriteHelper(writer);

    //Rolling file 0

    String singleFileName = testMeta.dir + File.separator + SINGLE_FILE;

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
  public void testSingleRollingFileCompletedWriteOverwrite()
  {
    prepareTest();
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(false);

    testSingleRollingFileCompletedWriteHelper(writer);

    //Rolling file 0

    String singleFileName = testMeta.dir + File.separator + SINGLE_FILE;

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
    writer.setFilePath(testMeta.dir);
    writer.setMaxLength(4);

    writer.setup(new DummyContext(0));

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
    prepareTest();
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(true);
    writer.setMaxLength(4);

    writer.setFilePath(testMeta.dir);

    writer.setup(new DummyContext(0));

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
    writer.setup(new DummyContext(0));

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

    //Rolling file 0

    String singleFileName = testMeta.dir + File.separator + SINGLE_FILE;

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

  @Test
  public void testSingleRollingFileFailedWriteOverwrite()
  {
    prepareTest();
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(false);
    writer.setMaxLength(4);

    writer.setFilePath(testMeta.dir);

    writer.setup(new DummyContext(0));

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
    writer.setup(new DummyContext(0));

    writer.beginWindow(1);
    writer.input.put(3);
    writer.endWindow();

    writer.beginWindow(2);
    writer.input.put(6);
    writer.input.put(7);
    writer.input.put(8);
    writer.endWindow();

    writer.teardown();

    //Rolling file 0

    String singleFileName = testMeta.dir + File.separator + SINGLE_FILE;

    String correctContents = "0\n" +
                             "1\n" +
                             "2\n";


    checkOutput(0,
                singleFileName,
                correctContents);

    //Rolling file 1

    correctContents = "3\n" +
                      "6\n" +
                      "7\n";

    checkOutput(1,
                singleFileName,
                correctContents);

    //Rolling file 2

    correctContents = "8\n";

    checkOutput(2,
                singleFileName,
                correctContents);
  }

  @Test
  public void testSingleRollingFileFailedWrite1()
  {
    prepareTest();
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(true);
    writer.setFilePath(testMeta.dir);
    writer.setMaxLength(4);

    writer.setup(new DummyContext(0));

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

    LOG.debug("Checkpoint endOffsets={}", checkPointWriter.endOffsets);

    writer.beginWindow(2);
    writer.input.put(5);
    writer.teardown();

    restoreCheckPoint(checkPointWriter,
                      writer);
    LOG.debug("Checkpoint endOffsets={}", checkPointWriter.endOffsets);
    writer.setup(new DummyContext(0));

    writer.beginWindow(2);
    writer.input.put(5);
    writer.endWindow();

    writer.beginWindow(3);
    writer.input.put(6);
    writer.input.put(7);
    writer.input.put(8);
    writer.endWindow();

    writer.teardown();

    restoreCheckPoint(checkPointWriter,
                      writer);
    writer.setup(new DummyContext(0));

    String singleFilePath = testMeta.dir + File.separator + SINGLE_FILE;

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
                      "5\n";

    checkOutput(1,
                singleFilePath,
                correctContents);

    //Rolling file 2

    correctContents = "6\n" +
                      "7\n" +
                      "8\n";

    checkOutput(2,
                singleFilePath,
                correctContents);
  }

  @Test
  public void testMultiRollingFileCompletedWrite()
  {
    prepareTest();
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setMaxLength(4);
    writer.setAppend(true);

    testMultiRollingFileCompletedWriteHelper(writer);
  }

  @Test
  public void testMultiRollingFileCompletedWriteOverwrite()
  {
    prepareTest();
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setMaxLength(4);
    writer.setAppend(false);

    testMultiRollingFileCompletedWriteHelper(writer);
  }

  private void testMultiRollingFileCompletedWriteHelper(EvenOddHDFSExactlyOnceWriter writer)
  {
    File meta = new File(testMeta.dir);
    writer.setFilePath(meta.getAbsolutePath());

    writer.setup(new DummyContext(0));

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

    String evenFileName = testMeta.dir + File.separator + EVEN_FILE;

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

    String oddFileName = testMeta.dir + File.separator + ODD_FILE;

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
    prepareTest();
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(true);
    File meta = new File(testMeta.dir);
    writer.setFilePath(meta.getAbsolutePath());
    writer.setMaxLength(4);

    writer.setup(new DummyContext(0));

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
    writer.setup(new DummyContext(0));

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

    //Even file

    String evenFileName = testMeta.dir + File.separator + EVEN_FILE;

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

    String oddFileName = testMeta.dir + File.separator + ODD_FILE;

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
  public void testMultiRollingFileFailedWriteOverwrite()
  {
    prepareTest();
    EvenOddHDFSExactlyOnceWriter writer = new EvenOddHDFSExactlyOnceWriter();
    writer.setAppend(false);
    File meta = new File(testMeta.dir);
    writer.setFilePath(meta.getAbsolutePath());
    writer.setMaxLength(4);

    writer.setup(new DummyContext(0));

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
    writer.setup(new DummyContext(0));

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

    String evenFileName = testMeta.dir + File.separator + EVEN_FILE;

    String correctContents = "4\n" +
                             "6\n" +
                             "8\n";

    checkOutput(0,
                evenFileName,
                correctContents);

    correctContents = "6\n";

    checkOutput(1,
                evenFileName,
                correctContents);

    //Odd file

    String oddFileName = testMeta.dir + File.separator + ODD_FILE;

    correctContents = "5\n" +
                      "7\n" +
                      "9\n";

    checkOutput(0,
                oddFileName,
                correctContents);

    correctContents = "7\n";

    checkOutput(1,
                oddFileName,
                correctContents);
  }
}
