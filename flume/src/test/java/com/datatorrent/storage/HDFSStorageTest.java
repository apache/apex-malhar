package com.datatorrent.storage;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSStorageTest
{
  public static final String STORAGE_DIRECTORY = "target";

  @Test
  public void testStorage() throws IOException
  {
    Storage storage = HDFSStorage.getInstance(STORAGE_DIRECTORY, true);
    if (storage == null) {
      return;
    }
    RandomAccessFile r = new RandomAccessFile("src/test/resources/TestInput.txt", "r");
    r.seek(0);
    byte[] b = r.readLine().getBytes();
    long val = storage.store(b);
    storage.flush();
    Assert.assertEquals("matched the stored value with retrieved value", new String(b), new String(storage.retrieve(val).getData()));
    r.close();
  }

  @Test
  public void testCleanup() throws IOException
  {
    Storage storage = HDFSStorage.getInstance(STORAGE_DIRECTORY, false);
    if (storage == null) {
      return;
    }
    RandomAccessFile r = new RandomAccessFile("src/test/resources/TestInput.txt", "r");
    r.seek(0);
    byte[] b = r.readLine().getBytes();
    long val = storage.store(b);
    storage.flush();
    val = storage.store(b);
    storage.flush();
    storage.clean(val);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    boolean exists =fs.exists(new Path(STORAGE_DIRECTORY+"/"+"0")); 
    Assert.assertEquals("file shoule not exist", false, exists);
  }

  /*
  public static void main(String[] args)
  {
    Storage storage = HDFSStorage.getInstance(".", true);
    if (storage == null) {
      return;
    }
    RandomAccessFile r;
    try {
      r = new RandomAccessFile("/home/gaurav/malhar/Core/flume/src/test/resources/TestInput.txt", "r");
      r.seek(0);
      byte[] b = r.readLine().getBytes();
      long val = storage.store(b);
      for (int i = 0; i < 10000000; i++)
        storage.store(b);
      storage.store(r.readLine().getBytes());
      System.out.println(storage.retrieve(val));
      System.out.println(storage.retrieveNext());
      r.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
  */

  private static final Logger logger = LoggerFactory.getLogger(HDFSStorageTest.class);
}
