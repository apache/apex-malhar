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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.getopt.util.hash.MurmurHash;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.contrib.hds.tfile.TFileImpl;
import com.google.common.util.concurrent.MoreExecutors;
/**
 *
 */
public class HDSTest
{


  public static class MyDataKey
  {
    public byte[] bytes;

    public long getBucketKey()
    {
      return readLong(this.bytes, 0);
    }

    @Override
    public int hashCode()
    {
      return MurmurHash.hash(this.bytes, 31);
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      MyDataKey other = (MyDataKey) obj;
      return Arrays.equals(this.bytes, other.bytes);
    }

    public byte[] getBytes()
    {
      return bytes;
    }

    public static class SequenceComparator implements Comparator<byte[]>
    {
      @Override
      public int compare(byte[] o1, byte[] o2)
      {
        long t1 = readLong(o1, 8);
        long t2 = readLong(o2, 8);
        if (t1 == t2) {
          long b1 = readLong(o1, 0);
          long b2 = readLong(o2, 0);
          return b1 == b2 ? 0 : (b1 > b2) ? 1 : -1;
        } else {
          return t1 > t2 ? 1 : -1;
        }
      }
    }

    public static MyDataKey newKey(long bucketId, long sequenceId)
    {
      MyDataKey key = new MyDataKey();
      key.bytes = ByteBuffer.allocate(16).putLong(bucketId).putLong(sequenceId).array();
      return key;
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

  }

  private void testHDSFileAccess(HDSFileAccessFSImpl bfs) throws Exception
  {
    File file = new File("target/hds");
    FileUtils.deleteDirectory(file);
    final long BUCKET1 = 1L;

    File bucket1Dir = new File(file, Long.toString(BUCKET1));
    File bucket1WalFile = new File(bucket1Dir, HDSBucketManager.FNAME_WAL);
    RegexFileFilter dataFileFilter = new RegexFileFilter("\\d+.*");

    bfs.setBasePath(file.getAbsolutePath());

    HDSBucketManager hds = new HDSBucketManager();
    hds.setFileStore(bfs);
    hds.setKeyComparator(new MyDataKey.SequenceComparator());
    hds.setMaxFileSize(1); // limit to single entry per file
    hds.setFlushSize(0); // flush after every key

    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor(); // execute synchronously on endWindow

    hds.beginWindow(10);
    Assert.assertFalse("exists " + bucket1WalFile, bucket1WalFile.exists());

    MyDataKey key1 = MyDataKey.newKey(BUCKET1, 1);
    String data1 = "data01bucket1";

    hds.put(BUCKET1, key1.getBytes(), data1.getBytes());
    Assert.assertArrayEquals("uncommitted get1 " + key1, data1.getBytes(), hds.get(BUCKET1, key1.getBytes()));

    Assert.assertTrue("exists " + bucket1Dir, bucket1Dir.exists() && bucket1Dir.isDirectory());
    Assert.assertTrue("exists " + bucket1WalFile, bucket1WalFile.exists() && bucket1WalFile.isFile());

    hds.endWindow();
    String[] files = bucket1Dir.list(dataFileFilter);
    Assert.assertEquals("" + Arrays.asList(files), 1, files.length);
    files = bucket1Dir.list(dataFileFilter);
    Assert.assertEquals("" + Arrays.asList(files), 1, files.length);

    // replace value
    String data1Updated = data1 + "-update1";
    hds.put(BUCKET1, key1.getBytes(), data1Updated.getBytes());
    Assert.assertArrayEquals("uncommitted get2 " + key1, data1Updated.getBytes(), hds.get(BUCKET1, key1.getBytes()));

    hds.endWindow();
    files = bucket1Dir.list(dataFileFilter);
    Assert.assertEquals("" + Arrays.asList(files), 1, files.length);
    Assert.assertArrayEquals("cold read key=" + key1, data1Updated.getBytes(), hds.readFile(BUCKET1, "1-1").get(key1.getBytes()));

    MyDataKey key12 = MyDataKey.newKey(BUCKET1, 2);
    String data12 = "data02bucket1";

    Assert.assertEquals(BUCKET1, key12.getBucketKey());

    hds.put(key12.getBucketKey(), key12.bytes, data12.getBytes()); // key 2, bucket 1

    // new key added to existing range, due to size limit 2 data files will be written
    hds.endWindow();
    File metaFile = new File(bucket1Dir, HDSBucketManager.FNAME_META);
    Assert.assertTrue("exists " + metaFile, metaFile.exists());

    files = bucket1Dir.list(dataFileFilter);
    Assert.assertEquals("" + Arrays.asList(files), 2, files.length);
    Assert.assertArrayEquals("cold read key=" + key1, data1Updated.getBytes(), hds.readFile(BUCKET1, "1-2").get(key1.getBytes()));
    Assert.assertArrayEquals("cold read key=" + key12, data12.getBytes(), hds.readFile(BUCKET1, "1-3").get(key12.getBytes()));
    Assert.assertTrue("exists " + bucket1WalFile, bucket1WalFile.exists() && bucket1WalFile.isFile());

    hds.committed(1);

    Assert.assertTrue("exists " + metaFile, metaFile.exists() && metaFile.isFile());

    bfs.close();

  }

  @Test
  public void testDefaultHDSFileAccess() throws Exception
  {

    // Create default HDSFileAccessImpl
    HDSFileAccessFSImpl bfs = new HDSFileAccessFSImpl();

    testHDSFileAccess(bfs);

  }


  @Test
  public void testTFileHDSFileAccess() throws Exception
  {

    //Create TFileImpl
    TFileImpl timpl = new TFileImpl();

    testHDSFileAccess(timpl);

  }

}
