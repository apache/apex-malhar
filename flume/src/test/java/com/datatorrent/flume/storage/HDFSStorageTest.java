package com.datatorrent.flume.storage;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.flume.storage.HDFSStorage;
import com.datatorrent.flume.storage.Storage;

public class HDFSStorageTest
{
  public static final String STORAGE_DIRECTORY = "target";

  @Test
  public void testStorage() throws IOException
  {
    Context ctx = new Context();
    ctx.put(HDFSStorage.BASE_DIR_KEY,STORAGE_DIRECTORY);
    ctx.put(HDFSStorage.RESTORE_KEY,Boolean.toString(false));    
    Storage storage = new HDFSStorage();
    ((Configurable)storage).configure(ctx);
    RandomAccessFile r = new RandomAccessFile("src/test/resources/TestInput.txt", "r");
    r.seek(0);
    byte[] b = r.readLine().getBytes();
    long val = storage.store(b);
    storage.close();
    Assert.assertEquals("matched the stored value with retrieved value", new String(b), new String(storage.retrieve(val).getData()));
    r.close();
  }
  
  @Test
  public void testStorageWithRestore() throws IOException
  {
    testStorage();
    Context ctx = new Context();
    ctx.put(HDFSStorage.BASE_DIR_KEY,STORAGE_DIRECTORY);
    ctx.put(HDFSStorage.RESTORE_KEY,Boolean.toString(true));    
    Storage storage = new HDFSStorage();
    ((Configurable)storage).configure(ctx);
    RandomAccessFile r = new RandomAccessFile("src/test/resources/TestInput.txt", "r");
    r.seek(0);
    byte[] b = r.readLine().getBytes();
    storage.store(b);
    storage.close();
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    boolean exists =fs.exists(new Path(STORAGE_DIRECTORY+"/"+"1")); 
    Assert.assertEquals("file shoule not exist", true, exists);
    r.close();
  }

  @Test
  public void testCleanup() throws IOException
  {
    Context ctx = new Context();
    ctx.put(HDFSStorage.BASE_DIR_KEY,STORAGE_DIRECTORY);
    ctx.put(HDFSStorage.RESTORE_KEY,Boolean.toString(false));    
    Storage storage = new HDFSStorage();
    ((Configurable)storage).configure(ctx);
    RandomAccessFile r = new RandomAccessFile("src/test/resources/TestInput.txt", "r");
    r.seek(0);
    byte[] b = r.readLine().getBytes();
    long val = storage.store(b);
    storage.close();
    val = storage.store(b);
    storage.close();
    storage.clean(val);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    boolean exists =fs.exists(new Path(STORAGE_DIRECTORY+"/"+"0")); 
    Assert.assertEquals("file shoule not exist", false, exists);
    r.close();
  }

  private static final Logger logger = LoggerFactory.getLogger(HDFSStorageTest.class);
}
