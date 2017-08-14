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

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.io.fs.AbstractFileOutputOperatorTest.FSTestWatcher;
import org.apache.apex.malhar.lib.util.TestUtils.TestInfo;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context.OperatorContext;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class AbstractSingleFileOutputOperatorTest
{
  private static final String SINGLE_FILE = "single.txt";

  @Rule
  public TestInfo testMeta = new PrivateTestWatcher();

  public static OperatorContext testOperatorContext = mockOperatorContext(0);

  private static SimpleFileOutputOperator writer;

  private static class PrivateTestWatcher extends FSTestWatcher
  {
    @Override
    public void starting(Description description)
    {
      super.starting(description);
      writer = new SimpleFileOutputOperator();
      writer.setOutputFileName(SINGLE_FILE);

      writer.setFilePath(getDir());
      writer.setAlwaysWriteToTmp(false);
      writer.setup(testOperatorContext);
    }
  }

  /**
   * Dummy writer to store checkpointed state
   */
  private static class CheckPointOutputOperator extends AbstractSingleFileOutputOperator<Integer>
  {
    @Override
    protected byte[] getBytesForTuple(Integer tuple)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  /**
   * Simple writer which writes to one file.
   */
  private static class SimpleFileOutputOperator extends AbstractSingleFileOutputOperator<Integer>
  {
    @Override
    protected FileSystem getFSInstance() throws IOException
    {
      return FileSystem.getLocal(new Configuration()).getRaw();
    }

    @Override
    protected byte[] getBytesForTuple(Integer tuple)
    {
      return (tuple.toString() + "\n").getBytes();
    }
  }

  private CheckPointOutputOperator checkpoint(AbstractSingleFileOutputOperator<Integer> writer)
  {
    CheckPointOutputOperator checkPointWriter = new CheckPointOutputOperator();
    checkPointWriter.counts = Maps.newHashMap();

    for (String keys : writer.counts.keySet()) {
      checkPointWriter.counts.put(keys, new MutableLong(writer.counts.get(keys).longValue()));
    }

    checkPointWriter.endOffsets = Maps.newHashMap();

    for (String keys : writer.endOffsets.keySet()) {
      checkPointWriter.endOffsets.put(keys, new MutableLong(writer.endOffsets.get(keys).longValue()));
    }

    checkPointWriter.openPart = Maps.newHashMap();

    for (String keys : writer.openPart.keySet()) {
      checkPointWriter.openPart.put(keys, new MutableInt(writer.openPart.get(keys).intValue()));
    }

    checkPointWriter.filePath = writer.filePath;
    checkPointWriter.maxOpenFiles = writer.maxOpenFiles;
    checkPointWriter.replication = writer.replication;
    checkPointWriter.totalBytesWritten = writer.totalBytesWritten;
    checkPointWriter.maxLength = writer.maxLength;
    checkPointWriter.rollingFile = writer.rollingFile;
    checkPointWriter.outputFileName = writer.outputFileName;

    return checkPointWriter;
  }

  private void restoreCheckPoint(CheckPointOutputOperator checkPointWriter,
      AbstractSingleFileOutputOperator<Integer> writer)
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
    writer.outputFileName = checkPointWriter.outputFileName;
  }

  @Test
  public void testSingleFileCompletedWrite()
  {
    writer.setOutputFileName(SINGLE_FILE);
    writer.setPartitionedFileNameformat(null);

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

    String singleFileName = testMeta.getDir() + File.separator + SINGLE_FILE;

    String correctContents = "0\n" + "1\n" + "2\n" + "3\n";
    AbstractFileOutputOperatorTest.checkOutput(-1, singleFileName, correctContents);
  }

  @Test
  public void testSingleFileFailedWrite()
  {
    writer.setOutputFileName(SINGLE_FILE);
    writer.setPartitionedFileNameformat("");

    File meta = new File(testMeta.getDir());
    writer.setFilePath(meta.getAbsolutePath());

    writer.setup(testOperatorContext);

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.endWindow();

    CheckPointOutputOperator checkPointWriter = checkpoint(writer);

    writer.beginWindow(1);
    writer.input.put(2);

    writer.teardown();

    restoreCheckPoint(checkPointWriter, writer);
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

    String singleFileName = testMeta.getDir() + File.separator + SINGLE_FILE;

    String correctContents = "0\n" + "1\n" + "4\n" + "5\n" + "6\n" + "7\n";
    AbstractFileOutputOperatorTest.checkOutput(-1, singleFileName, correctContents);
  }
}
