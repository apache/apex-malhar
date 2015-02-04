/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.hdht;

import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.HDFSWalReader;
import com.datatorrent.contrib.hdht.HDFSWalWriter;
import com.datatorrent.contrib.hdht.HDHTFileAccessFSImpl;
import com.datatorrent.contrib.hdht.HDHTWriter;
import com.datatorrent.contrib.hdht.MutableKeyValue;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.google.common.util.concurrent.MoreExecutors;

import org.junit.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class WALTest
{
  static final Random rand = new Random();

  File file = new File("target/hds");

  static byte[] genRandomByteArray(int len) {
    byte[] val = new byte[len];
    rand.nextBytes(val);
    return val;
  }

  static Slice genRandomKey(int len) {
    byte[] val = new byte[len];
    rand.nextBytes(val);
    return new Slice(val);
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
    FileUtils.deleteDirectory(file);
    HDHTFileAccessFSImpl bfs = new MockFileAccess();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();

    int keySize = 100;
    int valSize = 100;
    int numTuples = 100;

    HDFSWalWriter wWriter = new HDFSWalWriter(bfs, 1, "WAL-0");
    for (int i = 0; i < numTuples; i++) {
      wWriter.append(genRandomKey(keySize), genRandomByteArray(valSize));
    }
    wWriter.close();

    File wal0 = new File(file.getAbsoluteFile().toString() + "/1/WAL-0");
    Assert.assertEquals("WAL file created ", true, wal0.exists());

    HDFSWalReader wReader = new HDFSWalReader(bfs, 1, "WAL-0");
    int read = 0;
    while (wReader.advance()) {
      read++;
      MutableKeyValue keyVal = wReader.get();
      Assert.assertEquals("Key size ", keySize, keyVal.getKey().length);
      Assert.assertEquals("Value size ", valSize, keyVal.getValue().length);
    }
    wReader.close();
    Assert.assertEquals("Write and read same number of tuples ", numTuples, read);
  }

  /**
   * Read WAL from middle of the file by seeking to known valid
   * offset and start reading from that point till the end.
   */
  @Test
  public void testWalSkip() throws IOException
  {
    FileUtils.deleteDirectory(file);
    HDHTFileAccessFSImpl bfs = new MockFileAccess();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();

    long offset = 0;

    HDFSWalWriter wWriter = new HDFSWalWriter(bfs, 1, "WAL-0");
    int totalTuples = 100;
    int recoveryTuples = 30;
    for (int i = 0; i < totalTuples; i++) {
      wWriter.append(genRandomKey(100), genRandomByteArray(100));
      if (i == recoveryTuples)
        offset = wWriter.logSize();
    }
    logger.info("total file size is " + wWriter.logSize() + " recovery offset is " + offset);
    wWriter.close();

    HDFSWalReader wReader = new HDFSWalReader(bfs, 1, "WAL-0");
    wReader.seek(offset);
    int read = 0;
    while (wReader.advance()) {
      read++;
      wReader.get();
    }
    wReader.close();

    Assert.assertEquals("Number of tuples read after skipping", read, (totalTuples - recoveryTuples - 1));
  }

  /**
   * Test WAL rolling functionality, set maximumWal size to 1024.
   * Write some data which will go over WAL size.
   * call endWindow
   * Write some more data.
   * Two files should be created.
   * @throws IOException
   */
  @Test
  public void testWalRolling() throws IOException
  {
    File file = new File("target/hds");
    FileUtils.deleteDirectory(file);
    final long BUCKET1 = 1L;

    HDHTFileAccessFSImpl bfs = new MockFileAccess();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();

    HDHTWriter hds = new HDHTWriter();
    hds.setFileStore(bfs);
    hds.setKeyComparator(new HDHTWriterTest.SequenceComparator());
    hds.setFlushIntervalCount(5);
    hds.setFlushSize(1000);
    hds.setMaxWalFileSize(1024);
    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor();

    hds.beginWindow(0);
    hds.put(BUCKET1, genRandomKey(500), genRandomByteArray(500));
    hds.put(BUCKET1, genRandomKey(500), genRandomByteArray(500));
    hds.endWindow();

    hds.beginWindow(1);
    hds.put(BUCKET1, genRandomKey(500), genRandomByteArray(500));
    hds.put(BUCKET1, genRandomKey(500), genRandomByteArray(500));
    hds.endWindow();
    hds.forceWal();

    File wal0 = new File(file.getAbsoluteFile().toString() + "/1/_WAL-0");
    Assert.assertEquals("New Wal-0 created ", wal0.exists(), true);

    File wal1 = new File(file.getAbsoluteFile().toString() + "/1/_WAL-1");
    Assert.assertEquals("New Wal-1 created ", wal1.exists(), true);
  }

  /**
   * Rest recovery of operator cache. Steps
   * - Add some tuples
   * - Flush data to disk.
   * - Add some more tuples, which are not flushed to data, but flushed to WAL.
   * - Save WAL state (operator checkpoint)
   * - Add a tuple to start recovery from tuples.
   * @throws IOException
   */
  @Test
  public void testWalRecovery() throws IOException
  {
    File file = new File("target/hds");
    FileUtils.deleteDirectory(file);

    FileUtils.deleteDirectory(file);
    HDHTFileAccessFSImpl bfs = new MockFileAccess();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();

    HDHTWriter hds = new HDHTWriter();
    hds.setFileStore(bfs);
    hds.setKeyComparator(new HDHTWriterTest.SequenceComparator());
    hds.setFlushSize(1);
    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor();

    hds.beginWindow(1);
    hds.put(1, genRandomKey(500), genRandomByteArray(500));
    hds.put(1, genRandomKey(500), genRandomByteArray(500));
    hds.endWindow();
    hds.checkpointed(1);

    hds.beginWindow(2);
    hds.put(1, genRandomKey(500), genRandomByteArray(500));
    hds.put(1, genRandomKey(500), genRandomByteArray(500));
    hds.endWindow();
    hds.checkpointed(2);
    hds.committed(2);

    // Tuples added till this point is written to data files,
    // Tuples being added in this window, will not be written to data files
    // but will be saved in WAL. These should get recovered when bucket
    // is initialized for use next time.
    hds.beginWindow(3);
    hds.put(1, genRandomKey(500), genRandomByteArray(500));
    hds.put(1, genRandomKey(500), genRandomByteArray(500));
    hds.endWindow();
    hds.checkpointed(3);

    hds.forceWal();
    hds.teardown();

    /* Get a check-pointed state of the WAL */
    HDHTWriter newOperator = TestUtils.clone(new Kryo(), hds);

    newOperator.setKeyComparator(new HDHTWriterTest.SequenceComparator());
    newOperator.setFlushIntervalCount(1);
    newOperator.setFlushSize(3);
    newOperator.setup(null);
    newOperator.writeExecutor = MoreExecutors.sameThreadExecutor();

    newOperator.setFileStore(bfs);
    newOperator.setup(null);

    // This should run recovery, as first tuple is added in bucket
    newOperator.beginWindow(4);
    newOperator.put(1, genRandomKey(500), genRandomByteArray(500));

    // current tuple, being added is put into write cache.
    Assert.assertEquals("Number of tuples in write cache ", 1, newOperator.unflushedDataSize(1));

    // two tuples are put in to committed write cache.
    Assert.assertEquals("Number of tuples in committed cache ", 2, newOperator.committedDataSize(1));

    newOperator.put(1, genRandomKey(500), genRandomByteArray(500));
    newOperator.put(1, genRandomKey(500), genRandomByteArray(500));
    newOperator.put(1, genRandomKey(500), genRandomByteArray(500));
    newOperator.endWindow();
    newOperator.forceWal();

    File wal1 = new File(file.getAbsoluteFile().toString() + "/1/_WAL-1");
    Assert.assertEquals("New Wal-1 created ", wal1.exists(), true);
  }


  /**
   * Test WAL cleanup functionality, WAL file is deleted, once data
   * from it is written to data files.
   * @throws IOException
   */
  @Test
  public void testOldWalCleanup() throws IOException
  {
    File file = new File("target/hds");
    FileUtils.deleteDirectory(file);
    final long BUCKET1 = 1L;

    HDHTFileAccessFSImpl bfs = new MockFileAccess();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();

    HDHTWriter hds = new HDHTWriter();
    hds.setFileStore(bfs);
    hds.setKeyComparator(new HDHTWriterTest.SequenceComparator());
    // Flush at every window.
    hds.setFlushIntervalCount(2);
    hds.setFlushSize(1000);
    hds.setMaxWalFileSize(4000);
    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor();

    hds.beginWindow(1);
    hds.put(BUCKET1, genRandomKey(500), genRandomByteArray(500));
    hds.put(BUCKET1, genRandomKey(500), genRandomByteArray(500));
    hds.endWindow();

    hds.beginWindow(2);
    hds.put(BUCKET1, genRandomKey(500), genRandomByteArray(500));
    hds.put(BUCKET1, genRandomKey(500), genRandomByteArray(500));
    // log file will roll at this point because of limit on WAL file size,
    hds.endWindow();

    File wal0 = new File(file.getAbsoluteFile().toString() + "/1/_WAL-0");
    Assert.assertEquals("New Wal-0 created ", wal0.exists(), true);

    hds.beginWindow(3);
    hds.put(BUCKET1, genRandomKey(500), genRandomByteArray(500));
    hds.put(BUCKET1, genRandomKey(500), genRandomByteArray(500));
    hds.endWindow();
    hds.checkpointed(3);
    hds.committed(3);
    // Data till this point is committed to disk, and old WAL file WAL-0
    // is deleted, as all data from that file is committed.
    hds.forceWal();

    wal0 = new File(file.getAbsoluteFile().toString() + "/1/_WAL-0");
    Assert.assertEquals("New Wal-0 deleted ", wal0.exists(), false);

    File wal1 = new File(file.getAbsoluteFile().toString() + "/1/_WAL-1");
    Assert.assertEquals("New Wal-1 created ", wal1.exists(), true);
  }

  static Slice getLongByteArray(long key)
  {
    ByteBuffer bb = ByteBuffer.allocate(8);
    bb.putLong(key);
    return new Slice(bb.array());
  }

    /**
   * checkpointed(1)  1 -> 10
   * checkpointed(2)  1 -> 20
   * checkpointed(3)  1 -> 30
   * checkpointed(4)  1 -> 40
   * committed(2)
   * checkpointed(5)
   *
   * restore from 3rd checkpoint.
   * do a get and value should be 30.
   */
  @Test
  public void testWalRecoveryValues() throws IOException
  {
    File file = new File("target/hds");
    FileUtils.deleteDirectory(file);

    HDHTFileAccessFSImpl bfs = new MockFileAccess();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();
    ((MockFileAccess)bfs).disableChecksum();

    HDHTWriter hds = new HDHTWriter();
    hds.setFileStore(bfs);
    hds.setFlushSize(1);
    hds.setFlushIntervalCount(1);
    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor();

    hds.beginWindow(1);
    hds.put(1, getLongByteArray(1), getLongByteArray(10).toByteArray());
    hds.endWindow();
    hds.checkpointed(1);

    hds.beginWindow(2);
    hds.put(1, getLongByteArray(1), getLongByteArray(20).toByteArray());
    hds.endWindow();
    hds.checkpointed(2);

    hds.beginWindow(3);
    hds.put(1, getLongByteArray(1), getLongByteArray(30).toByteArray());
    hds.endWindow();
    hds.checkpointed(3);

    // Commit window id 2
    hds.committed(2);
    // use checkpoint after window 3 for recovery.
    HDHTWriter newOperator = TestUtils.clone(new Kryo(), hds);

    hds.beginWindow(4);
    hds.put(1, getLongByteArray(1), getLongByteArray(40).toByteArray());
    hds.put(1, getLongByteArray(2), getLongByteArray(200).toByteArray());
    hds.endWindow();
    hds.checkpointed(4);

    hds.beginWindow(5);
    hds.put(1, getLongByteArray(1), getLongByteArray(50).toByteArray());
    hds.put(1, getLongByteArray(2), getLongByteArray(210).toByteArray());
    hds.endWindow();
    hds.checkpointed(5);
    hds.forceWal();

    /* Simulate recovery after failure, checkpoint is restored to after
       processing of window 3.
     */
    newOperator.setFlushIntervalCount(1);
    newOperator.setFileStore(bfs);
    newOperator.setFlushSize(1);
    newOperator.setup(null);
    newOperator.writeExecutor = MoreExecutors.sameThreadExecutor();

    // This should run recovery, as first tuple is added in bucket
    newOperator.beginWindow(4);
    newOperator.put(1, getLongByteArray(1), getLongByteArray(40).toByteArray());
    newOperator.put(1, getLongByteArray(2), getLongByteArray(200).toByteArray());
    // current tuple, being added is put into write cache.
    Assert.assertEquals("Number of tuples in write cache ", 2, newOperator.unflushedDataSize(1));
    // one tuples are put in to committed write cache.
    Assert.assertEquals("Number of tuples in committed cache ", 1, newOperator.committedDataSize(1));
    newOperator.endWindow();
    newOperator.checkpointed(4);

    /* The latest value is recovered from WAL */
    ByteBuffer bb = ByteBuffer.wrap(newOperator.getUncommitted(1, getLongByteArray(1)));
    long l = bb.getLong();
    Assert.assertEquals("Value of 1 is recovered from WAL", 40, l);

    newOperator.committed(3);

    bb = ByteBuffer.wrap(newOperator.get(1, getLongByteArray(1)));
    l = bb.getLong();
    Assert.assertEquals("Value is persisted ", 30, l);

    newOperator.committed(4);
    bb = ByteBuffer.wrap(newOperator.get(1, getLongByteArray(1)));
    l = bb.getLong();
    Assert.assertEquals("Value is persisted ", 40, l);
  }


  /**
   * checkpointed(1)  1 -> 10
   * checkpointed(2)  1 -> 20
   * committed(2)
   * checkpointed(3)  1 -> 30
   * committed(3)
   * checkpointed(4)
   * committed(4)
   *
   * no null pointer exception should occure.
   */
  @Test
  public void testIssue4008() throws IOException
  {
    File file = new File("target/hds");
    FileUtils.deleteDirectory(file);

    HDHTFileAccessFSImpl bfs = new MockFileAccess();
    bfs.setBasePath(file.getAbsolutePath());
    bfs.init();
    ((MockFileAccess)bfs).disableChecksum();

    HDHTWriter hds = new HDHTWriter();
    hds.setFileStore(bfs);
    hds.setFlushSize(2);
    hds.setFlushIntervalCount(1);
    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor();

    hds.beginWindow(1);
    hds.put(1, getLongByteArray(1), getLongByteArray(10).toByteArray());
    hds.endWindow();
    hds.checkpointed(1);

    hds.beginWindow(2);
    hds.put(1, getLongByteArray(1), getLongByteArray(20).toByteArray());
    hds.endWindow();
    hds.checkpointed(2);
    hds.committed(2);

    hds.beginWindow(3);
    hds.put(1, getLongByteArray(1), getLongByteArray(30).toByteArray());
    hds.endWindow();
    hds.checkpointed(3);
    hds.committed(3);

    hds.beginWindow(4);
    hds.endWindow();
    hds.checkpointed(4);
    hds.committed(4);

    /* The latest value is recovered from WAL */
    ByteBuffer bb = ByteBuffer.wrap(hds.get(1, getLongByteArray(1)));
    long l = bb.getLong();
    Assert.assertEquals("Value of 1 is recovered from WAL", 30, l);

  }


  private static final Logger logger = LoggerFactory.getLogger(WALTest.class);

}
