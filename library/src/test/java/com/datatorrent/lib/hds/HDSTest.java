/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.hds;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.getopt.util.hash.MurmurHash;
import org.junit.Test;

import com.datatorrent.lib.hds.HDS.DataKey;
import com.datatorrent.lib.util.KeyValPair;

/**
 *
 */
public class HDSTest
{
  public static class MyDataKey implements DataKey
  {
    public byte[] bytes;

    @Override
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

    @Override
    public byte[] getBytes()
    {
      return bytes;
    }

    public static class SequenceComparator implements Comparator<MyDataKey>
    {
      @Override
      public int compare(MyDataKey o1, MyDataKey o2)
      {
        long t1 = readLong(o1.bytes, 8);
        long t2 = readLong(o2.bytes, 8);
        if (t1 == t2) {
          long b1 = readLong(o1.bytes, 0);
          long b2 = readLong(o2.bytes, 0);
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
      for (int i = offset; i < bytes.length; i++) {
          r = r << 8;
          r += bytes[i];
      }
      return r;
    }

  }

  @Test
  public void test() throws Exception
  {
    File file = new File("target/hds");
    FileUtils.deleteDirectory(file);
    final long BUCKET1 = 1L;

    FileSystem fs = FileSystem.getLocal(new Configuration(false)).getRawFileSystem();
    BucketFileSystem bfs = new FSBucketFileSystem(fs, file.getAbsolutePath());



    HDSPrototype<MyDataKey, String> hds = new HDSPrototype<HDSTest.MyDataKey, String>();
    hds.bfs = bfs;
    hds.timeBucketUnit = TimeUnit.MILLISECONDS;
    hds.timeBucketSize = 10;
    hds.maxSize = 500;
    hds.setup(null);

    hds.beginWindow(10);

    MyDataKey key1 = MyDataKey.newKey(BUCKET1, 1);
    String data1 = "data01bucket1";
    KeyValPair<MyDataKey, String> entry1 = new KeyValPair<HDSTest.MyDataKey, String>(key1, data1);

    hds.put(entry1);

    File bucket1Dir = new File(file, Long.toString(BUCKET1));
    File bucket1DupsFile = new File(bucket1Dir, HDSPrototype.FNAME_WAL);
    Assert.assertTrue("exists " + bucket1Dir, bucket1Dir.exists() && bucket1Dir.isDirectory());
    Assert.assertFalse("exists " + bucket1DupsFile, bucket1DupsFile.exists());

    RegexFileFilter ff = new RegexFileFilter("\\d+.*");
    String[] files = bucket1Dir.list(ff);
    Assert.assertEquals("" + Arrays.asList(files), 1, files.length);

    hds.put(entry1); // duplicate key
    files = bucket1Dir.list(ff);
    Assert.assertEquals("" + Arrays.asList(files), 1, files.length);
    Assert.assertTrue("exists " + bucket1DupsFile, bucket1DupsFile.exists() && bucket1DupsFile.isFile());

    MyDataKey key12 = MyDataKey.newKey(BUCKET1, 2);
    String data12 = "data02bucket1";
    KeyValPair<MyDataKey, String> entry12 = new KeyValPair<HDSTest.MyDataKey, String>(key12, data12);

    hds.put(entry12); // same time bucket key
    files = bucket1Dir.list(ff);
    Assert.assertEquals("" + Arrays.asList(files), 1, files.length);
    Assert.assertTrue("exists " + bucket1DupsFile, bucket1DupsFile.exists() && bucket1DupsFile.isFile());


    MyDataKey key2 = MyDataKey.newKey(BUCKET1, 11);
    String data2 = "data11bucket1";
    KeyValPair<MyDataKey, String> entry2 = new KeyValPair<HDSTest.MyDataKey, String>(key2, data2);

    hds.put(entry2); // next time bucket
    files = bucket1Dir.list(ff);
    Assert.assertEquals("" + Arrays.asList(files), 2, files.length);

    hds.endWindow();

    File metaFile = new File(bucket1Dir, HDSPrototype.FNAME_META);
    Assert.assertFalse("exists " + metaFile, metaFile.exists());

    hds.committed(1);

    Assert.assertTrue("exists " + metaFile, metaFile.exists() && metaFile.isFile());

    bfs.close();

  }

}
