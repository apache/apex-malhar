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
import com.datatorrent.flume.storage.HDFSStorage;
import com.datatorrent.flume.storage.Storage;

/**
 * 
 * @author Gaurav Gupta <gaurav@datatorrent.com>
 */
public class HDFSStorageTest
{
  public static final String STORAGE_DIRECTORY = "target";

  @Test
  public void testStorage() throws IOException
  {
    Context ctx = new Context();
    ctx.put(HDFSStorage.BASE_DIR_KEY, STORAGE_DIRECTORY);
    ctx.put(HDFSStorage.RESTORE_KEY, Boolean.toString(false));
    ctx.put(HDFSStorage.ID, "1");
    Storage storage = new HDFSStorage();
    ((Configurable) storage).configure(ctx);
    byte[] b = new byte[1028];
    byte[] val = storage.store(b);
    storage.close();
    byte[] data = storage.retrieve(val);
    byte[] tempData = new byte[data.length - 8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    Assert.assertEquals("matched the stored value with retrieved value", new String(b), new String(tempData));
    byte[] identifier = new byte[8];
    Server.writeLong(identifier, 0, calculateOffset(0l, 1l));
    storage.clean(identifier);
    

  }

  @Test
  public void testStorageWithRestore() throws IOException
  {
    testStorage();
    Context ctx = new Context();
    ctx.put(HDFSStorage.BASE_DIR_KEY, STORAGE_DIRECTORY);
    ctx.put(HDFSStorage.RESTORE_KEY, Boolean.toString(true));
    ctx.put(HDFSStorage.ID, "1");
    Storage storage = new HDFSStorage();
    ((Configurable) storage).configure(ctx);
    byte[] b = new byte[1028];
    storage.store(b);
    storage.close();
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    boolean exists = fs.exists(new Path(STORAGE_DIRECTORY + "/" + "1"));
    Assert.assertEquals("file shoule exist", true, exists);
    byte[] identifier = new byte[8];
    Server.writeLong(identifier, 0, calculateOffset(0l, 2l));
    storage.clean(identifier);
  }

  @Test
  public void testCleanup() throws IOException
  {
    Context ctx = new Context();
    ctx.put(HDFSStorage.BASE_DIR_KEY, STORAGE_DIRECTORY);
    ctx.put(HDFSStorage.RESTORE_KEY, Boolean.toString(false));
    ctx.put(HDFSStorage.ID, "2");
    Storage storage = new HDFSStorage();
    ((Configurable) storage).configure(ctx);
    RandomAccessFile r = new RandomAccessFile("src/test/resources/TestInput.txt", "r");
    r.seek(0);
    byte[] b = r.readLine().getBytes();
    byte[] val = storage.store(b);
    storage.close();
    val = storage.store(b);
    storage.close();
    storage.clean(val);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    boolean exists = fs.exists(new Path(STORAGE_DIRECTORY + "/" + "0"));
    Assert.assertEquals("file shoule not exist", false, exists);
    r.close();
    byte[] identifier = new byte[8];
    Server.writeLong(identifier, 0, calculateOffset(0l, 2l));
    storage.clean(identifier);
  }

  @Test
  public void testNext() throws IOException
  {
    Context ctx = new Context();
    ctx.put(HDFSStorage.BASE_DIR_KEY, STORAGE_DIRECTORY);
    ctx.put(HDFSStorage.RESTORE_KEY, Boolean.toString(false));
    ctx.put(HDFSStorage.ID, "3");
    Storage storage = new HDFSStorage();
    ((Configurable) storage).configure(ctx);
    RandomAccessFile r = new RandomAccessFile("src/test/resources/TestInput.txt", "r");
    r.seek(0);
    byte[] b = r.readLine().getBytes();
    byte[] val = storage.store(b);
    byte[] b1 = r.readLine().getBytes();
    storage.store(b1);
    storage.store(b);
    storage.close();
    byte[] data = storage.retrieve(val);
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
    byte[] identifier = new byte[8];
    Server.writeLong(identifier, 0, calculateOffset(0l, 1l));
    storage.clean(identifier);
  }

  @Test
  public void testNextWithFileClose() throws IOException
  {
    Context ctx = new Context();
    ctx.put(HDFSStorage.BASE_DIR_KEY, STORAGE_DIRECTORY);
    ctx.put(HDFSStorage.RESTORE_KEY, Boolean.toString(false));
    ctx.put(HDFSStorage.ID, "4");
    Storage storage = new HDFSStorage();
    ((Configurable) storage).configure(ctx);
    RandomAccessFile r = new RandomAccessFile("src/test/resources/TestInput.txt", "r");
    r.seek(0);
    byte[] b = r.readLine().getBytes();
    byte[] val = storage.store(b);
    storage.close();
    byte[] b1 = r.readLine().getBytes();
    storage.store(b1);
    storage.close();
    byte[] data = storage.retrieve(val);
    byte[] tempData = new byte[data.length - 8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    Assert.assertEquals("matched the stored value with retrieved value", new String(b), new String(tempData));
    data = storage.retrieveNext();
    tempData = new byte[data.length - 8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    Assert.assertEquals("matched the stored value with retrieved value", new String(b1), new String(tempData));
    r.close();
    byte[] identifier = new byte[8];
    Server.writeLong(identifier, 0, calculateOffset(0l, 1l));
    storage.clean(identifier);
    Server.writeLong(identifier, 0, calculateOffset(0l, 2l));
    storage.clean(identifier);
  }

  @Test
  public void testRetrieval() throws IOException
  {
    Context ctx = new Context();
    ctx.put(HDFSStorage.BASE_DIR_KEY, STORAGE_DIRECTORY);
    ctx.put(HDFSStorage.RESTORE_KEY, Boolean.toString(false));
    ctx.put(HDFSStorage.ID, "5");
    Storage storage = new HDFSStorage();
    ((Configurable) storage).configure(ctx);
    RandomAccessFile r = new RandomAccessFile("src/test/resources/TestInput.txt", "r");
    r.seek(0);
    byte[] b = r.readLine().getBytes();
    byte[] val = storage.store(b);
    byte[] b1 = r.readLine().getBytes();
    storage.store(b1);
    storage.store(b);
    storage.close();
    byte[] data = storage.retrieve(val);
    byte[] tempData = new byte[data.length - 8];
    byte[] identifier = new byte[8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    System.arraycopy(data, 0, identifier, 0, 8);
    Assert.assertEquals("matched the stored value with retrieved value", new String(b), new String(tempData));

    data = storage.retrieve(identifier);
    tempData = new byte[data.length - 8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    System.arraycopy(data, 0, identifier, 0, 8);
    Assert.assertEquals("matched the stored value with retrieved value", new String(b1), new String(tempData));

    data = storage.retrieve(identifier);
    tempData = new byte[data.length - 8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    System.arraycopy(data, 0, identifier, 0, 8);
    Assert.assertEquals("matched the stored value with retrieved value", new String(b), new String(tempData));
    r.close();
    
    Server.writeLong(identifier, 0, calculateOffset(0l, 1l));
    storage.clean(identifier);
  }

  @Test
  public void testRetrievalWithFailure() throws IOException
  {
    Context ctx = new Context();
    ctx.put(HDFSStorage.BASE_DIR_KEY, STORAGE_DIRECTORY);
    ctx.put(HDFSStorage.RESTORE_KEY, Boolean.toString(false));
    ctx.put(HDFSStorage.ID, "6");
    Storage storage = new HDFSStorage();
    ((Configurable) storage).configure(ctx);
    RandomAccessFile r = new RandomAccessFile("src/test/resources/TestInput.txt", "r");
    r.seek(0);
    byte[] b = r.readLine().getBytes();
    storage.store(b);
    storage.flush();
    storage.close();
    byte[] identifier = new byte[8];
    Server.writeLong(identifier, 0, calculateOffset(0l, 1l));
    ctx.put(HDFSStorage.RESTORE_KEY, Boolean.toString(true));
    storage = new HDFSStorage();
    ((Configurable) storage).configure(ctx);
    try {
      storage.retrieve(identifier);
    } catch (Exception e) {
      Assert.assertNull("the identifier is null",storage.store(b));
      storage.close();
      byte[] data = storage.retrieve(identifier);
      byte[] tempData = new byte[data.length - 8];
      System.arraycopy(data, 8, tempData, 0, tempData.length);
      Assert.assertEquals("matched the stored value with retrieved value", new String(b), new String(tempData));
      
    }
    r.close();
    
    Server.writeLong(identifier, 0, calculateOffset(0l, 1l));
    storage.clean(identifier);
    Server.writeLong(identifier, 0, calculateOffset(0l, 2l));
    storage.clean(identifier);
  }

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(HDFSStorageTest.class);

  private long calculateOffset(long fileOffset, long fileCounter)
  {
    return ((fileCounter << 32) | (fileOffset & 0xffffffffl));
  }

}
