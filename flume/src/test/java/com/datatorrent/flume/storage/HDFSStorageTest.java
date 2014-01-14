/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
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

import com.datatorrent.flume.sink.Server;

/**
 * 
 * @author Gaurav Gupta <gaurav@datatorrent.com>
 */
public class HDFSStorageTest
{
  public static final String STORAGE_DIRECTORY = "target";

  private HDFSStorage getStorage(String id, boolean restore)
  {
    Context ctx = new Context();
    ctx.put(HDFSStorage.BASE_DIR_KEY, STORAGE_DIRECTORY);
    ctx.put(HDFSStorage.RESTORE_KEY, Boolean.toString(restore));
    ctx.put(HDFSStorage.ID, id);
    ctx.put(HDFSStorage.BLOCKSIZE, "256");
    HDFSStorage storage = new HDFSStorage();
    ((Configurable) storage).configure(ctx);
    return storage;
  }

  @Test
  public void testStorage() throws IOException
  {
    HDFSStorage storage = getStorage("1", false);
    Assert.assertNull(storage.retrieve(new byte[8]));
    byte[] b = new byte[200];
    Assert.assertNotNull(storage.store(b));
    Assert.assertNotNull(storage.store(b));
    storage.flush();
    byte[] data = storage.retrieve(new byte[8]);
    byte[] tempData = new byte[data.length - 8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    Assert.assertEquals("matched the stored value with retrieved value", new String(b), new String(tempData));
    byte[] identifier = new byte[8];
    Server.writeLong(identifier, 0, calculateOffset(0l, 1l));
    storage.clean(identifier);
    ((HDFSStorage) storage).cleanHelperFiles();
  }

  @Test
  public void testStorageWithRestore() throws IOException
  {
    HDFSStorage storage = getStorage("1", false);
    Assert.assertNull(storage.retrieve(new byte[8]));
    byte[] b = new byte[200];
    Assert.assertNotNull(storage.store(b));
    storage.flush();
    
    storage = getStorage("1", true);
    storage.store(b);
    storage.flush();
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    boolean exists = fs.exists(new Path(STORAGE_DIRECTORY + "/1/" + "1"));
    Assert.assertEquals("file shoule exist", true, exists);
    storage.cleanHelperFiles();
  }

  @Test
  public void testCleanup() throws IOException
  {
    HDFSStorage storage = getStorage("1", false);
    RandomAccessFile r = new RandomAccessFile("src/test/resources/TestInput.txt", "r");
    r.seek(0);
    byte[] b = r.readLine().getBytes();
    storage.store(b);
    byte[] val = storage.store(b);
    storage.flush();
    storage.clean(val);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    boolean exists = fs.exists(new Path(STORAGE_DIRECTORY + "/" + "0"));
    Assert.assertEquals("file shoule not exist", false, exists);
    r.close();
    storage.cleanHelperFiles();
  }

  @Test
  public void testNext() throws IOException
  {
    HDFSStorage storage = getStorage("1", false);
    RandomAccessFile r = new RandomAccessFile("src/test/resources/TestInput.txt", "r");
    r.seek(0);
    Assert.assertNull(storage.retrieve(new byte[8]));
    byte[] b = r.readLine().getBytes();
    storage.store(b);
    byte[] b1 = r.readLine().getBytes();
    storage.store(b1);
    storage.store(b);
    storage.flush();
    storage.store(b1);
    storage.store(b);
    storage.flush();
    byte[] data = storage.retrieveNext();
    byte[] tempData = new byte[data.length - 8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    Assert.assertEquals("matched the stored value with retrieved value", new String(b), new String(tempData));
    data = storage.retrieveNext();
    tempData = new byte[data.length - 8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    Assert.assertEquals("matched the stored value with retrieved value", new String(b1), new String(tempData));
    data = storage.retrieveNext();
    tempData = new byte[data.length - 8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    Assert.assertEquals("matched the stored value with retrieved value", new String(b), new String(tempData));
    r.close();
    storage.cleanHelperFiles();
  }
  
  @Test
  public void testRetrieval() throws IOException
  {
    HDFSStorage storage = getStorage("1", false);
    RandomAccessFile r = new RandomAccessFile("src/test/resources/TestInput.txt", "r");
    r.seek(0);
    byte[] address = new byte[8];
    Server.writeLong(address, 0, calculateOffset(0l, 2l));
    Assert.assertNull(storage.retrieve(address));
    byte[] b = r.readLine().getBytes();
    byte[] b1 = r.readLine().getBytes();
    Assert.assertNull(storage.store(b));
    Assert.assertNull(storage.store(b1));
    Assert.assertNull(storage.store(b));
    Assert.assertNull(storage.store(b1));
    Assert.assertNotNull(storage.store(b));
    Assert.assertNotNull(storage.store(b1));
    Assert.assertNotNull(storage.store(b));
    Assert.assertNotNull(storage.store(b1));
    storage.flush();
    byte[] data = storage.retrieveNext();
    byte[] tempData = new byte[data.length - 8];
    byte[] identifier = new byte[8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    System.arraycopy(data, 0, identifier, 0, 8);
    Assert.assertEquals("matched the stored value with retrieved value", new String(b), new String(tempData));

    data = storage.retrieve(address);
    tempData = new byte[data.length - 8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    System.arraycopy(data, 0, identifier, 0, 8);
    Assert.assertEquals("matched the stored value with retrieved value", new String(b), new String(tempData));
    
    data = storage.retrieveNext();
    tempData = new byte[data.length - 8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    System.arraycopy(data, 0, identifier, 0, 8);
    Assert.assertEquals("matched the stored value with retrieved value", new String(b1), new String(tempData));
    storage.cleanHelperFiles();
    r.close();
  }

  
  
  @Test
  public void testFailure() throws IOException
  {
    HDFSStorage storage = getStorage("1", false);
    byte[] address;
    byte[] b = new byte[200];
    for (int i = 0; i < 5; i++) {
      storage.store(b);
      address = storage.store(b);
      storage.flush();
      storage.clean(address);

    }
    byte[] identifier = new byte[8];
    storage = getStorage("1", true);
    try {
      storage.retrieve(identifier);
    } catch (Exception e) {
      storage.store(b);
      storage.store(b);
      storage.store(b);
      storage.flush();
      byte[] data = storage.retrieve(identifier);
      byte[] tempData = new byte[data.length - 8];
      System.arraycopy(data, 8, tempData, 0, tempData.length);
      Assert.assertEquals("matched the stored value with retrieved value", new String(b), new String(tempData));
    }
    storage.cleanHelperFiles();
  }

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(HDFSStorageTest.class);

  private long calculateOffset(long fileOffset, long fileCounter)
  {
    return ((fileCounter << 32) | (fileOffset & 0xffffffffl));
  }

}
