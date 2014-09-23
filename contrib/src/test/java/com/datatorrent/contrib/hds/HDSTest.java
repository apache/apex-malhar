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
package com.datatorrent.contrib.hds;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hds.HDSFileAccess.HDSFileReader;
import com.datatorrent.contrib.hds.HDSReader.HDSQuery;
import com.datatorrent.contrib.hds.hfile.HFileImpl;
import com.datatorrent.contrib.hds.tfile.TFileImpl;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Tests for bucket management
 */
public class HDSTest
{
  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  public static long getBucketKey(Slice key)
  {
    return readLong(key.buffer, 8);
  }

  public static class SequenceComparator implements Comparator<Slice>
  {
    @Override
    public int compare(Slice o1, Slice o2)
    {
      long t1 = readLong(o1.buffer, o1.offset);
      long t2 = readLong(o2.buffer, o2.offset);
      if (t1 == t2) {
        long b1 = readLong(o1.buffer, o1.offset + 8);
        long b2 = readLong(o2.buffer, o2.offset + 8);
        return b1 == b2 ? 0 : (b1 > b2) ? 1 : -1;
      } else {
        return t1 > t2 ? 1 : -1;
      }
    }
  }

  public static Slice newKey(long bucketId, long sequenceId)
  {
    byte[] bytes = ByteBuffer.allocate(16).putLong(sequenceId).putLong(bucketId).array();
    return new Slice(bytes, 0, bytes.length);
  }

  private static long readLong(byte[] bytes, int offset)
  {
    long r = 0;
    for (int i = offset; i < offset + 8; i++) {
      r = r << 8;
      r += bytes[i];
    }
    return r;
  }

  private TreeMap<Slice, byte[]> readFile(HDSWriter bm, long bucketKey, String fileName) throws IOException
  {
    TreeMap<Slice, byte[]> data = Maps.newTreeMap(bm.getKeyComparator());
    HDSFileReader reader = bm.getFileStore().getReader(bucketKey, fileName);
    reader.readFully(data);
    reader.close();
    return data;
  }

  private void testHDSFileAccess(HDSFileAccessFSImpl bfs) throws Exception
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);
    final long BUCKET1 = 1L;

    File bucket1Dir = new File(file, Long.toString(BUCKET1));
    File bucket1WalFile = new File(bucket1Dir, HDSWalManager.WAL_FILE_PREFIX + 0);
    RegexFileFilter dataFileFilter = new RegexFileFilter("\\d+.*");

    bfs.setBasePath(file.getAbsolutePath());

    HDSWriter hds = new HDSWriter();
    hds.setFileStore(bfs);
    //hds.setKeyComparator(new SequenceComparator());
    hds.setMaxFileSize(1); // limit to single entry per file
    hds.setFlushSize(0); // flush after every key

    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor(); // synchronous flush on endWindow

    hds.beginWindow(10);
    Assert.assertFalse("exists " + bucket1WalFile, bucket1WalFile.exists());

    Slice key1 = newKey(BUCKET1, 1);
    String data1 = "data01bucket1";

    hds.put(BUCKET1, key1, data1.getBytes());

    HDSQuery q = new HDSQuery();
    q.bucketKey = BUCKET1;
    q.key = new Slice(key1.buffer, key1.offset, key1.length); // check key equality;

    hds.processQuery(q); // write cache
    Assert.assertArrayEquals("uncommitted get1 " + key1, data1.getBytes(), q.result);

    Assert.assertTrue("exists " + bucket1Dir, bucket1Dir.exists() && bucket1Dir.isDirectory());
    Assert.assertTrue("exists " + bucket1WalFile, bucket1WalFile.exists() && bucket1WalFile.isFile());

    hds.endWindow();
    String[] files = bucket1Dir.list(dataFileFilter);
    Assert.assertEquals("" + Arrays.asList(files), 1, files.length);
    files = bucket1Dir.list(dataFileFilter);
    Assert.assertEquals("" + Arrays.asList(files), 1, files.length);

    // replace value
    String data1Updated = data1 + "-update1";
    hds.put(BUCKET1, key1, data1Updated.getBytes());

    hds.processQuery(q); // write cache
    Assert.assertArrayEquals("uncommitted get2 " + key1, data1Updated.getBytes(), q.result);

    hds.endWindow();
    files = bucket1Dir.list(dataFileFilter);
    Assert.assertEquals("" + Arrays.asList(files), 1, files.length);
    Assert.assertArrayEquals("cold read key=" + key1, data1Updated.getBytes(), readFile(hds, BUCKET1, "1-1").get(key1));

    Slice key12 = newKey(BUCKET1, 2);
    String data12 = "data02bucket1";

    Assert.assertEquals(BUCKET1, getBucketKey(key12));
    hds.put(getBucketKey(key12), key12, data12.getBytes()); // key 2, bucket 1

    // new key added to existing range, due to size limit 2 data files will be written
    hds.endWindow();
    File metaFile = new File(bucket1Dir, HDSWriter.FNAME_META);
    Assert.assertTrue("exists " + metaFile, metaFile.exists());

    files = bucket1Dir.list(dataFileFilter);
    Assert.assertEquals("" + Arrays.asList(files), 2, files.length);
    Assert.assertArrayEquals("cold read key=" + key1, data1Updated.getBytes(), readFile(hds, BUCKET1, "1-2").get(key1));
    Assert.assertArrayEquals("cold read key=" + key12, data12.getBytes(), readFile(hds, BUCKET1, "1-3").get(key12));
    Assert.assertTrue("exists " + bucket1WalFile, bucket1WalFile.exists() && bucket1WalFile.isFile());

    hds.committed(1);
    Assert.assertTrue("exists " + metaFile, metaFile.exists() && metaFile.isFile());
    bfs.close();

  }

  @Test
  public void testGet() throws Exception
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    Slice key = newKey(1, 1);
    String data = "data1";

    HDSFileAccessFSImpl fa = new MockFileAccess();
    fa.setBasePath(file.getAbsolutePath());
    HDSWriter hds = new HDSWriter();
    hds.setFileStore(fa);
    hds.setFlushSize(0); // flush after every key

    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor(); // synchronous flush on endWindow
    hds.beginWindow(1);

    hds.put(getBucketKey(key), key, data.getBytes());
    byte[] val = hds.getUncommitted(getBucketKey(key), key);
    Assert.assertArrayEquals("getUncommitted", data.getBytes(), val);

    hds.endWindow();

    val = hds.getUncommitted(getBucketKey(key), key);
    Assert.assertNull("getUncommitted", val);

    hds.teardown();

    // get fresh instance w/o cached readers
    hds = TestUtils.clone(new Kryo(), hds);
    hds.setup(null);
    hds.beginWindow(1);
    val = hds.get(getBucketKey(key), key);
    hds.endWindow();
    hds.teardown();
    Assert.assertArrayEquals("get", data.getBytes(), val);
  }

  @Test
  public void testRandomWrite() throws Exception
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    HDSFileAccessFSImpl fa = new MockFileAccess();
    fa.setBasePath(file.getAbsolutePath());
    HDSWriter hds = new HDSWriter();
    hds.setFileStore(fa);
    hds.setFlushIntervalCount(0); // flush after every window

    long BUCKETKEY = 1;

    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor(); // synchronous flush on endWindow

    hds.beginWindow(1);
    long[] seqArray = { 5L, 1L, 3L, 4L, 2L };
    for (long seq : seqArray) {
      Slice key = newKey(BUCKETKEY, seq);
      hds.put(BUCKETKEY, key, ("data"+seq).getBytes());
    }
    hds.endWindow();

    hds.teardown();

    HDSFileReader reader = fa.getReader(BUCKETKEY, "1-0");
    Slice key = new Slice(null, 0, 0);
    Slice value = new Slice(null, 0, 0);
    long seq = 0;
    while (reader.next(key, value)) {
      seq++;
      Assert.assertArrayEquals(("data"+seq).getBytes(), value.buffer);
    }
    Assert.assertEquals(5, seq);
  }

  @Test
  public void testWriteError() throws Exception
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    final RuntimeException writeError = new RuntimeException("failure simulation");
    final CountDownLatch endWindowComplete = new CountDownLatch(1);
    final CountDownLatch writerActive = new CountDownLatch(1);

    HDSFileAccessFSImpl fa = new MockFileAccess() {
      @Override
      public HDSFileWriter getWriter(long bucketKey, String fileName) throws IOException
      {
        writerActive.countDown();
        try {
          if (endWindowComplete.await(10, TimeUnit.SECONDS)) {
            throw writeError;
          }
        } catch (InterruptedException e) {
        }
        return super.getWriter(bucketKey, fileName);
      }
    };
    fa.setBasePath(file.getAbsolutePath());
    HDSWriter hds = new HDSWriter();
    hds.setFileStore(fa);
    hds.setFlushIntervalCount(0); // flush after every window

    long BUCKETKEY = 1;

    hds.setup(null);
    //hds.writeExecutor = new ScheduledThreadPoolExecutor(1);

    hds.beginWindow(1);
    long[] seqArray = { 5L, 1L, 3L, 4L, 2L };
    for (long seq : seqArray) {
      Slice key = newKey(BUCKETKEY, seq);
      hds.put(BUCKETKEY, key, ("data"+seq).getBytes());
    }
    hds.endWindow();
    endWindowComplete.countDown();

    try {
      Assert.assertTrue(writerActive.await(10, TimeUnit.SECONDS));
      hds.writeExecutor.shutdown();
      hds.writeExecutor.awaitTermination(10, TimeUnit.SECONDS);
      hds.beginWindow(2);
      hds.endWindow();
      Assert.fail("exception not raised");
    } catch (Exception e) {
      Assert.assertSame(writeError, e.getCause());
    }

    hds.teardown();
  }

  @Test
  public void testDefaultHDSFileAccess() throws Exception
  {
    // Create default HDSFileAccessImpl
    HDSFileAccessFSImpl bfs = new MockFileAccess();
    testHDSFileAccess(bfs);
  }


  @Test
  public void testDefaultTFileHDSFileAccess() throws Exception
  {
    //Create DefaultTFileImpl
    TFileImpl timpl = new TFileImpl.DefaultTFileImpl();
    testHDSFileAccess(timpl);
  }

  @Test
  public void testDTFileHDSFileAccess() throws Exception
  {
    //Create DefaultTFileImpl
    TFileImpl timpl = new TFileImpl.DTFileImpl();
    testHDSFileAccess(timpl);
  }

  @Test
  public void testHFileHDSFileAccess() throws Exception
  {
    //Create HfileImpl
    HFileImpl hfi = new HFileImpl();
    hfi.setComparator(new HDSWriter.DefaultKeyComparator());
    testHDSFileAccess(hfi);
  }

}

