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
package org.apache.apex.malhar.lib.wal;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.KryoCloneUtils;
import org.apache.apex.malhar.lib.utils.FileContextUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.netlet.util.Slice;

public class FileSystemWALTest
{
  private static final Random RAND = new Random();

  private static Slice getRandomSlice(int len)
  {
    byte[] val = new byte[len];
    RAND.nextBytes(val);
    return new Slice(val);
  }

  private class TestMeta extends TestWatcher
  {
    private String targetDir;
    FileSystemWAL fsWAL = new FileSystemWAL();
    Configuration conf = new Configuration();
    FileSystem fs;

    @Override
    protected void starting(Description description)
    {
      targetDir = "target/" + description.getClassName() + "/" + description.getMethodName();

      try {
        fs = FileSystem.get(new URI(targetDir + "/WAL"), conf);
        fs.delete(new Path(targetDir), true);
      } catch (IOException | URISyntaxException e) {
        throw new RuntimeException(e);
      }

      fsWAL = new FileSystemWAL();
      fsWAL.setFilePath(targetDir + "/WAL");
    }

    @Override
    protected void finished(Description description)
    {
      try {
        fs.delete(new Path(targetDir), true);
      } catch (IOException e) {
        throw  new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testSerde() throws IOException
  {
    FileSystemWAL deserialized = KryoCloneUtils.cloneObject(testMeta.fsWAL);
    Assert.assertNotNull("File System WAL", deserialized);
  }

  /**
   * - Write some data to WAL
   * - Read the data back. The amount of data read should be
   *   same as amount of data written.
   * @throws IOException
   */
  @Test
  public void testWalWriteAndRead() throws IOException
  {
    testMeta.fsWAL.setup();
    int numTuples = 100;

    FileSystemWAL.FileSystemWALWriter fsWALWriter = testMeta.fsWAL.getWriter();
    for (int i = 0; i < numTuples; i++) {
      int len = RAND.nextInt(100);
      fsWALWriter.append(getRandomSlice(len));
    }
    fsWALWriter.rotate(true);
    testMeta.fsWAL.beforeCheckpoint(0);
    testMeta.fsWAL.committed(0);
    Path walFile = new Path(testMeta.fsWAL.getPartFilePath(0));
    Assert.assertEquals("WAL file created ", true, testMeta.fs.isFile(walFile));

    FileSystemWAL.FileSystemWALReader fsWALReader = testMeta.fsWAL.getReader();
    assertNumTuplesRead(fsWALReader, numTuples);

    testMeta.fsWAL.teardown();
  }

  /**
   * Read WAL from middle of the file by seeking to known valid
   * offset and start reading from that point till the end.
   */
  @Test
  public void testWalSkip() throws IOException
  {
    testMeta.fsWAL.setup();

    FileSystemWAL.FileSystemWALWriter fsWALWriter = testMeta.fsWAL.getWriter();

    int totalTuples = 100;
    int totalBytes = 0;
    long offset = 0;
    for (int i = 0; i < totalTuples; i++) {
      if (i == 30) {
        offset = totalBytes;
      }
      totalBytes += fsWALWriter.append(getRandomSlice(100));
    }

    fsWALWriter.rotate(true);
    testMeta.fsWAL.beforeCheckpoint(0);
    testMeta.fsWAL.committed(0);

    FileSystemWAL.FileSystemWALReader fsWALReader = testMeta.fsWAL.getReader();

    fsWALReader.seek(new FileSystemWAL.FileSystemWALPointer(0, offset));
    assertNumTuplesRead(fsWALReader, totalTuples - 30);

    testMeta.fsWAL.teardown();
  }

  /**
   * Test WAL rolling functionality, set segment size to 32 * 1024.
   * Write some data which will go over WAL segment size. multiple files
   * should be created.
   * @throws IOException
   */
  @Test
  public void testWalRolling() throws IOException
  {

    testMeta.fsWAL.setMaxLength(32 * 1024);
    testMeta.fsWAL.setup();

    FileSystemWAL.FileSystemWALWriter fsWALWriter = testMeta.fsWAL.getWriter();

    /* write each record of size 1K, each file will have 32 records, last file will have
     * (count % 32) records. */
    int numRecords = 100;
    write1KRecords(fsWALWriter, numRecords);

    testMeta.fsWAL.beforeCheckpoint(0);
    testMeta.fsWAL.committed(0);

    /** Read from start till the end */
    FileSystemWAL.FileSystemWALReader reader = testMeta.fsWAL.getReader();
    assertNumTuplesRead(reader, numRecords);

    /* skip first file for reading, and then read other entries till the end */
    reader.seek(new FileSystemWAL.FileSystemWALPointer(1, 0));
    assertNumTuplesRead(reader, numRecords - 32);

    /* skip first file and few records from the next file, and read till the end */
    reader.seek(new FileSystemWAL.FileSystemWALPointer(1, 16 * 1024));
    assertNumTuplesRead(reader, numRecords - (32 + 16));

    testMeta.fsWAL.teardown();
  }

  @Test
  public void testWalRollingWithPartialFiles() throws IOException
  {
    testMeta.fsWAL.setMaxLength(1024);
    testMeta.fsWAL.setup();

    FileSystemWAL.FileSystemWALWriter fsWALWriter = testMeta.fsWAL.getWriter();

    Slice first = getRandomSlice(1000);
    fsWALWriter.append(first);

    Slice second = getRandomSlice(1000);
    fsWALWriter.append(second);

    testMeta.fsWAL.beforeCheckpoint(0);

    /** Read from start till the end */
    FileSystemWAL.FileSystemWALReader reader = testMeta.fsWAL.getReader();
    assertNumTuplesRead(reader, 2);

    testMeta.fsWAL.teardown();
  }

  @Test
  public void testFinalizeAfterDelay() throws IOException
  {
    testMeta.fsWAL.setMaxLength(32 * 1024);
    testMeta.fsWAL.setup();

    FileSystemWAL.FileSystemWALWriter fsWALWriter = testMeta.fsWAL.getWriter();

    // write 32 records of size 1K
    write1KRecords(fsWALWriter, 32);
    testMeta.fsWAL.beforeCheckpoint(0);

    write1KRecords(fsWALWriter, 32);
    testMeta.fsWAL.beforeCheckpoint(1);

    write1KRecords(fsWALWriter, 32);
    testMeta.fsWAL.beforeCheckpoint(2);

    testMeta.fsWAL.committed(2);

    /** Read from start till the end */
    FileSystemWAL.FileSystemWALReader reader = testMeta.fsWAL.getReader();
    assertNumTuplesRead(reader, 32 * 3);
  }

  @Test
  public void testRecovery() throws IOException
  {
    testMeta.fsWAL.setMaxLength(2 * 1024);
    testMeta.fsWAL.setup();

    FileSystemWAL.FileSystemWALWriter fsWALWriter = testMeta.fsWAL.getWriter();

    /* Write each record of size 1K, each complete file will have 2 records.
     * If we write 3 records the first tmp file will get rotated with 2 records but the next one will just have 1
     * entry.
     */
    write1KRecords(fsWALWriter, 3);

    testMeta.fsWAL.beforeCheckpoint(0);
    testMeta.fsWAL.committed(0);

    FileSystemWAL.FileSystemWALReader fsWALReader = testMeta.fsWAL.getReader();
    assertNumTuplesRead(fsWALReader, 3);

    // a failure occurred
    fsWALWriter.close();
    testMeta.fsWAL.setup();
    write1KRecords(fsWALWriter, 1); //2nd part is completed and rotated
    testMeta.fsWAL.beforeCheckpoint(1);
    testMeta.fsWAL.committed(1);

    fsWALReader.seek(new FileSystemWAL.FileSystemWALPointer(1, 0));
    assertNumTuplesRead(fsWALReader, 2);

    testMeta.fsWAL.teardown();
  }

  @Test
  public void testDeleteOfTmpFiles() throws IOException
  {
    testMeta.fsWAL.setMaxLength(2 * 1024);
    testMeta.fsWAL.setup();

    FileSystemWAL.FileSystemWALWriter fsWALWriter = testMeta.fsWAL.getWriter();

    write1KRecords(fsWALWriter, 2);
    testMeta.fsWAL.beforeCheckpoint(0);
    FileSystemWAL fsWalCheckpointed = KryoCloneUtils.cloneObject(testMeta.fsWAL);

    write1KRecords(fsWALWriter, 2);
    testMeta.fsWAL.beforeCheckpoint(1);

    // a failure occurred
    fsWALWriter.close();

    fsWalCheckpointed.setup();
    fsWALWriter = fsWalCheckpointed.getWriter();
    write1KRecords(fsWALWriter, 2);
    fsWalCheckpointed.beforeCheckpoint(1);
    fsWalCheckpointed.committed(1);

    FileSystemWAL.FileSystemWALReader fsWALReader = fsWalCheckpointed.getReader();
    assertNumTuplesRead(fsWALReader, 4);

    fsWalCheckpointed.teardown();
  }

  @Test
  public void testReadWithInterceptingFinalize() throws IOException
  {
    testMeta.fsWAL.setMaxLength(2 * 1024);
    testMeta.fsWAL.setup();

    FileSystemWAL.FileSystemWALWriter fsWALWriter = testMeta.fsWAL.getWriter();

    write1KRecords(fsWALWriter, 1);
    testMeta.fsWAL.beforeCheckpoint(0);

    FileSystemWAL.FileSystemWALReader fsWALReader = testMeta.fsWAL.getReader();
    Assert.assertNotNull("one entry", fsWALReader.next());

    write1KRecords(fsWALWriter, 1);
    testMeta.fsWAL.beforeCheckpoint(1);
    testMeta.fsWAL.committed(1);
    assertNumTuplesRead(fsWALReader, 1);

    testMeta.fsWAL.teardown();
  }

  @Test
  public void testDeleteEverything() throws IOException
  {
    testMeta.fsWAL.setMaxLength(2 * 1024);
    testMeta.fsWAL.setup();

    FileSystemWAL.FileSystemWALWriter fsWALWriter = testMeta.fsWAL.getWriter();

    write1KRecords(fsWALWriter, 10);
    testMeta.fsWAL.beforeCheckpoint(0);
    testMeta.fsWAL.committed(0);

    FileSystemWAL.FileSystemWALReader fsWALReader = testMeta.fsWAL.getReader();
    assertNumTuplesRead(fsWALReader, 10);

    FileSystemWAL.FileSystemWALPointer writerPointer = fsWALWriter.getPointer();
    fsWALWriter.delete(writerPointer);

    FileContext fileContext = FileContextUtils.getFileContext(testMeta.fsWAL.getFilePath());
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue("part exists " + i,
          !fileContext.util().exists(new Path(testMeta.fsWAL.getPartFilePath(i))));
    }

    testMeta.fsWAL.teardown();
  }

  @Test
  public void testDeleteFullParts() throws IOException
  {
    testMeta.fsWAL.setMaxLength(2 * 1024);
    testMeta.fsWAL.setup();

    FileSystemWAL.FileSystemWALWriter fsWALWriter = testMeta.fsWAL.getWriter();

    write1KRecords(fsWALWriter, 10);
    testMeta.fsWAL.beforeCheckpoint(0);
    testMeta.fsWAL.committed(0);

    FileSystemWAL.FileSystemWALReader fsWALReader = testMeta.fsWAL.getReader();
    assertNumTuplesRead(fsWALReader, 10);

    fsWALWriter.delete(new FileSystemWAL.FileSystemWALPointer(3, 0));
    fsWALReader.seek(fsWALReader.getStartPointer());

    assertNumTuplesRead(fsWALReader, 4);
  }

  @Test
  public void testDeletePartialParts() throws IOException
  {
    testMeta.fsWAL.setMaxLength(2 * 1024);
    testMeta.fsWAL.setup();

    FileSystemWAL.FileSystemWALWriter fsWALWriter = testMeta.fsWAL.getWriter();

    write1KRecords(fsWALWriter, 4);
    testMeta.fsWAL.beforeCheckpoint(0);
    testMeta.fsWAL.committed(0);

    FileSystemWAL.FileSystemWALReader fsWALReader = testMeta.fsWAL.getReader();
    assertNumTuplesRead(fsWALReader, 4);

    fsWALWriter.delete(new FileSystemWAL.FileSystemWALPointer(1, 1024));

    fsWALReader.seek(fsWALReader.getStartPointer());
    assertNumTuplesRead(fsWALReader, 1);
  }

  @Test
  public void testFinalizeWithDelete() throws IOException
  {
    testMeta.fsWAL.setMaxLength(2 * 1024);
    testMeta.fsWAL.setup();

    FileSystemWAL.FileSystemWALWriter fsWALWriter = testMeta.fsWAL.getWriter();

    write1KRecords(fsWALWriter, 2);
    testMeta.fsWAL.beforeCheckpoint(0);

    write1KRecords(fsWALWriter, 2);
    testMeta.fsWAL.beforeCheckpoint(1);

    write1KRecords(fsWALWriter, 2);
    testMeta.fsWAL.beforeCheckpoint(2);

    FileSystemWAL.FileSystemWALReader fsWALReader = testMeta.fsWAL.getReader();
    assertNumTuplesRead(fsWALReader, 6);

    testMeta.fsWAL.committed(0);

    fsWALWriter.delete(new FileSystemWAL.FileSystemWALPointer(2, 0));

    FileContext fileContext = FileContextUtils.getFileContext(testMeta.fsWAL.getFilePath());
    Assert.assertTrue("part 0 exists ", !fileContext.util().exists(new Path(testMeta.fsWAL.getPartFilePath(0))));

    testMeta.fsWAL.committed(1);
    Assert.assertTrue("part 1 exists ", !fileContext.util().exists(new Path(testMeta.fsWAL.getPartFilePath(1))));

    fsWALReader.seek(fsWALReader.getStartPointer());
    assertNumTuplesRead(fsWALReader, 2);
  }

  @Test
  public void testPointerComparisons()
  {
    FileSystemWAL.FileSystemWALPointer pointer1 = new FileSystemWAL.FileSystemWALPointer(0, 10);
    FileSystemWAL.FileSystemWALPointer pointer2 = new FileSystemWAL.FileSystemWALPointer(0, 10);

    Assert.assertTrue("equal", pointer1.compareTo(pointer2) == 0);

    pointer1 = new FileSystemWAL.FileSystemWALPointer(0, 11);
    Assert.assertTrue("offset greater", pointer1.compareTo(pointer2) == 1);

    pointer1 = new FileSystemWAL.FileSystemWALPointer(0, 3);
    Assert.assertTrue("offset smaller", pointer1.compareTo(pointer2) == -1);

    pointer1 = new FileSystemWAL.FileSystemWALPointer(1, 10);
    Assert.assertTrue("part greater", pointer1.compareTo(pointer2) == 1);

    pointer1 = new FileSystemWAL.FileSystemWALPointer(0, 10);
    pointer2 = new FileSystemWAL.FileSystemWALPointer(3, 10);
    Assert.assertTrue("part smaller", pointer1.compareTo(pointer2) == -1);
  }

  private static void assertNumTuplesRead(FileSystemWAL.FileSystemWALReader reader, int expectedTuples)
      throws IOException
  {
    int tuples = 0;
    while (reader.next() != null) {
      tuples++;
    }
    Assert.assertEquals("num tuples", expectedTuples, tuples);
  }

  private static void write1KRecords(FileSystemWAL.FileSystemWALWriter writer, int numRecords) throws IOException
  {
    for (int i = 0; i < numRecords; i++) {
      writer.append(getRandomSlice(1020));
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemWALTest.class);

}
