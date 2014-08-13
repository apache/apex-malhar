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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang3.Range;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.getopt.util.hash.MurmurHash;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class HDSTest
{

  TimeUnit timeBucketUnit = TimeUnit.MILLISECONDS;
  int timeBucketSize = 10;
  int maxSize = 500;


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

  protected Range<Long> getRange(long time)
  {
    long timeBucket = this.timeBucketUnit.convert(time, TimeUnit.MILLISECONDS);
    timeBucket = timeBucket - (timeBucket % this.timeBucketSize);
    long min = TimeUnit.MILLISECONDS.convert(timeBucket, timeBucketUnit);
    long max = TimeUnit.MILLISECONDS.convert(timeBucket + timeBucketSize, timeBucketUnit);
    return Range.between(min, max);
  }

  @Test
  public void test() throws Exception
  {
    File file = new File("target/hds");
    FileUtils.deleteDirectory(file);
    final long BUCKET1 = 1L;

    File bucket1Dir = new File(file, Long.toString(BUCKET1));
    File bucket1WalFile = new File(bucket1Dir, HDSBucketManager.FNAME_WAL);
    RegexFileFilter dataFileFilter = new RegexFileFilter("\\d+.*");

    FileSystem fs = FileSystem.getLocal(new Configuration(false)).getRawFileSystem();
    BucketFileSystem bfs = new FSBucketFileSystem(fs, file.getAbsolutePath());

    HDSBucketManager hds = new HDSBucketManager();
    hds.bfs = bfs;
    hds.setKeyComparator(new MyDataKey.SequenceComparator());
    hds.setMaxFileSize(1); // limit to single entry per file

    hds.setup(null);

    hds.beginWindow(10);
    Assert.assertFalse("exists " + bucket1WalFile, bucket1WalFile.exists());

    MyDataKey key1 = MyDataKey.newKey(BUCKET1, 1);
    String data1 = "data01bucket1";

    hds.put(BUCKET1, key1.getBytes(), data1.getBytes());
    Assert.assertArrayEquals("uncommited get1 " + key1, data1.getBytes(), hds.get(BUCKET1, key1.getBytes()));

    Assert.assertTrue("exists " + bucket1Dir, bucket1Dir.exists() && bucket1Dir.isDirectory());
    Assert.assertTrue("exists " + bucket1WalFile, bucket1WalFile.exists() && bucket1WalFile.isFile());

    hds.writeDataFiles();
    String[] files = bucket1Dir.list(dataFileFilter);
    Assert.assertEquals("" + Arrays.asList(files), 1, files.length);
    files = bucket1Dir.list(dataFileFilter);
    Assert.assertEquals("" + Arrays.asList(files), 1, files.length);

    // replace value
    String data1Updated = data1 + "-update1";
    hds.put(BUCKET1, key1.getBytes(), data1Updated.getBytes());
    Assert.assertArrayEquals("uncommited get2 " + key1, data1Updated.getBytes(), hds.get(BUCKET1, key1.getBytes()));

    hds.writeDataFiles();
    files = bucket1Dir.list(dataFileFilter);
    Assert.assertEquals("" + Arrays.asList(files), 1, files.length);
    Assert.assertArrayEquals("cold read key=" + key1, data1Updated.getBytes(), hds.readFile(BUCKET1, "1-0").get(key1.getBytes()));

    MyDataKey key12 = MyDataKey.newKey(BUCKET1, 2);
    String data12 = "data02bucket1";

    Assert.assertEquals(BUCKET1, key12.getBucketKey());

    hds.put(key12.getBucketKey(), key12.bytes, data12.getBytes()); // key 2, bucket 1
    hds.writeDataFiles();

    files = bucket1Dir.list(dataFileFilter);
    Assert.assertEquals("" + Arrays.asList(files), 2, files.length);
    Assert.assertArrayEquals("cold read key=" + key1, data1Updated.getBytes(), hds.readFile(BUCKET1, "1-0").get(key1.getBytes()));
    Assert.assertArrayEquals("cold read key=" + key12, data12.getBytes(), hds.readFile(BUCKET1, "1-1").get(key12.getBytes()));
    Assert.assertTrue("exists " + bucket1WalFile, bucket1WalFile.exists() && bucket1WalFile.isFile());

    hds.endWindow();

    File metaFile = new File(bucket1Dir, HDSBucketManager.FNAME_META);
    Assert.assertFalse("exists " + metaFile, metaFile.exists());

    hds.committed(1);

    Assert.assertTrue("exists " + metaFile, metaFile.exists() && metaFile.isFile());

    bfs.close();

  }



}
