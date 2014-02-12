/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.storage;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.flume.sink.Server;
import com.google.common.primitives.Longs;

import org.junit.After;
import org.junit.Before;

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
    HDFSStorage lstorage = new HDFSStorage();
    lstorage.configure(ctx);
    lstorage.setup(null);
    return lstorage;
  }

  private HDFSStorage storage;

  @Before
  public void setup()
  {
    storage = getStorage("1", false);
  }

  @After
  public void teardown()
  {
    storage.cleanHelperFiles();
    storage.teardown();
  }

  @Test
  public void testPartialFlush() throws Exception{
    Assert.assertNull(storage.retrieve(new byte[8]));
    byte[] b = "ab".getBytes();
    byte[] address =  storage.store(b);
    Assert.assertNotNull(address);
    storage.flush();
    b = "cb".getBytes();
    byte[] addr = storage.store(b);
    Assert.assertNull(storage.retrieve(addr));
    Assert.assertNull(storage.store(b));
    storage.flush();
    match(storage.retrieve(address),"cb");
    Assert.assertNotNull(storage.store(b));
  }
  
  @Test
  public void testPartialFlushRollOver() throws Exception{
    Assert.assertNull(storage.retrieve(new byte[8]));
    byte[] b = new byte[] { 48, 48, 48, 48, 98, 48, 52, 54, 49, 57, 55, 51, 52, 97, 53, 101, 56, 56, 97, 55, 98, 53, 52, 51, 98, 50, 102, 51, 49, 97, 97, 54, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 32, 48, 48, 58, 51, 49, 58, 52, 56, 1, 49, 48, 53, 53, 57, 52, 50, 1, 50, 1, 49, 53, 49, 49, 54, 49, 56, 52, 1, 49, 53, 49, 49, 57, 50, 49, 49, 1, 49, 53, 49, 50, 57, 54, 54, 53, 1, 49, 53, 49, 50, 49, 53, 52, 56, 1, 49, 48, 48, 56, 48, 51, 52, 50, 1, 55, 56, 56, 50, 54, 53, 52, 56, 1, 49, 1, 48, 1, 48, 46, 48, 1, 48, 46, 48, 1, 48, 46, 48 };
    byte[] b_org = new byte[] { 48, 48, 48, 48, 98, 48, 52, 54, 49, 57, 55, 51, 52, 97, 53, 101, 56, 56, 97, 55, 98, 53, 52, 51, 98, 50, 102, 51, 49, 97, 97, 54, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 32, 48, 48, 58, 51, 49, 58, 52, 56, 1, 49, 48, 53, 53, 57, 52, 50, 1, 50, 1, 49, 53, 49, 49, 54, 49, 56, 52, 1, 49, 53, 49, 49, 57, 50, 49, 49, 1, 49, 53, 49, 50, 57, 54, 54, 53, 1, 49, 53, 49, 50, 49, 53, 52, 56, 1, 49, 48, 48, 56, 48, 51, 52, 50, 1, 55, 56, 56, 50, 54, 53, 52, 56, 1, 49, 1, 48, 1, 48, 46, 48, 1, 48, 46, 48, 1, 48, 46, 48 };
    int length = b.length;
    byte[] address =  storage.store(b);
    Assert.assertNotNull(address);
    storage.flush();
    byte[] addr = null;
    for(int i = 0; i < 5; i++){
      b[0] = (byte) (b[0]+1);
      addr = storage.store(b);
    }
    Assert.assertNull(storage.retrieve(addr));
    for(int i = 0; i < 5; i++){
      b[0] = (byte) (b[0]+1);
      Assert.assertNull(storage.store(b));
    }    
    storage.flush();
    b_org[0]=(byte)(b_org[0]+1);
    match(storage.retrieve(address),new String(b_org));
    b_org[0]=(byte)(b_org[0]+1);
    match(storage.retrieveNext(),new String(b_org));
    b_org[0]=(byte)(b_org[0]+1);
    match(storage.retrieveNext(),new String(b_org));
        
  }
  
  @Test
  public void testPartialFlushRollOverWithFailure() throws Exception{
    Assert.assertNull(storage.retrieve(new byte[8]));
    byte[] b = new byte[] { 48, 48, 48, 48, 98, 48, 52, 54, 49, 57, 55, 51, 52, 97, 53, 101, 56, 56, 97, 55, 98, 53, 52, 51, 98, 50, 102, 51, 49, 97, 97, 54, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 32, 48, 48, 58, 51, 49, 58, 52, 56, 1, 49, 48, 53, 53, 57, 52, 50, 1, 50, 1, 49, 53, 49, 49, 54, 49, 56, 52, 1, 49, 53, 49, 49, 57, 50, 49, 49, 1, 49, 53, 49, 50, 57, 54, 54, 53, 1, 49, 53, 49, 50, 49, 53, 52, 56, 1, 49, 48, 48, 56, 48, 51, 52, 50, 1, 55, 56, 56, 50, 54, 53, 52, 56, 1, 49, 1, 48, 1, 48, 46, 48, 1, 48, 46, 48, 1, 48, 46, 48 };
    byte[] b_org = new byte[] { 48, 48, 48, 48, 98, 48, 52, 54, 49, 57, 55, 51, 52, 97, 53, 101, 56, 56, 97, 55, 98, 53, 52, 51, 98, 50, 102, 51, 49, 97, 97, 54, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 32, 48, 48, 58, 51, 49, 58, 52, 56, 1, 49, 48, 53, 53, 57, 52, 50, 1, 50, 1, 49, 53, 49, 49, 54, 49, 56, 52, 1, 49, 53, 49, 49, 57, 50, 49, 49, 1, 49, 53, 49, 50, 57, 54, 54, 53, 1, 49, 53, 49, 50, 49, 53, 52, 56, 1, 49, 48, 48, 56, 48, 51, 52, 50, 1, 55, 56, 56, 50, 54, 53, 52, 56, 1, 49, 1, 48, 1, 48, 46, 48, 1, 48, 46, 48, 1, 48, 46, 48 };
    int length = b.length;
    byte[] address =  storage.store(b);
    Assert.assertNotNull(address);
    storage.flush();
    byte[] addr = null;
    for(int i = 0; i < 5; i++){
      b[0] = (byte) (b[0]+1);
      addr = storage.store(b);
    }
    storage = getStorage("1", true);
    Assert.assertNull(storage.retrieve(addr));
    for(int i = 0; i < 5; i++){
      b[0] = (byte) (b[0]+1);
      Assert.assertNull(storage.store(b));
    }    
    storage.flush();
    b_org[0]=(byte)(b_org[0]+1);
    match(storage.retrieve(address),new String(b_org));
    b_org[0]=(byte)(b_org[0]+1);
    match(storage.retrieveNext(),new String(b_org));
    b_org[0]=(byte)(b_org[0]+1);
    match(storage.retrieveNext(),new String(b_org));
        
  }
  
  
  @Test
  public void testPartialFlushWithClean() throws Exception{
    Assert.assertNull(storage.retrieve(new byte[8]));
    byte[] b = new byte[] { 48, 48, 48, 48, 98, 48, 52, 54, 49, 57, 55, 51, 52, 97, 53, 101, 56, 56, 97, 55, 98, 53, 52, 51, 98, 50, 102, 51, 49, 97, 97, 54, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 32, 48, 48, 58, 51, 49, 58, 52, 56, 1, 49, 48, 53, 53, 57, 52, 50, 1, 50, 1, 49, 53, 49, 49, 54, 49, 56, 52, 1, 49, 53, 49, 49, 57, 50, 49, 49, 1, 49, 53, 49, 50, 57, 54, 54, 53, 1, 49, 53, 49, 50, 49, 53, 52, 56, 1, 49, 48, 48, 56, 48, 51, 52, 50, 1, 55, 56, 56, 50, 54, 53, 52, 56, 1, 49, 1, 48, 1, 48, 46, 48, 1, 48, 46, 48, 1, 48, 46, 48 };
    byte[] b_org = new byte[] { 48, 48, 48, 48, 98, 48, 52, 54, 49, 57, 55, 51, 52, 97, 53, 101, 56, 56, 97, 55, 98, 53, 52, 51, 98, 50, 102, 51, 49, 97, 97, 54, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 32, 48, 48, 58, 51, 49, 58, 52, 56, 1, 49, 48, 53, 53, 57, 52, 50, 1, 50, 1, 49, 53, 49, 49, 54, 49, 56, 52, 1, 49, 53, 49, 49, 57, 50, 49, 49, 1, 49, 53, 49, 50, 57, 54, 54, 53, 1, 49, 53, 49, 50, 49, 53, 52, 56, 1, 49, 48, 48, 56, 48, 51, 52, 50, 1, 55, 56, 56, 50, 54, 53, 52, 56, 1, 49, 1, 48, 1, 48, 46, 48, 1, 48, 46, 48, 1, 48, 46, 48 };
    int length = b.length;
    byte[] address =  storage.store(b);
    Assert.assertNotNull(address);
    storage.flush();
    storage.clean(address);
    byte[] addr = null;
    for(int i = 0; i < 5; i++){
      b[0] = (byte) (b[0]+1);
      addr = storage.store(b);
    }
    Assert.assertNull(storage.retrieve(addr));
    for(int i = 0; i < 5; i++){
      b[0] = (byte) (b[0]+1);
      Assert.assertNull(storage.store(b));
    }    
    storage.flush();
    b_org[0]=(byte)(b_org[0]+1);
    match(storage.retrieve(address),new String(b_org));
    b_org[0]=(byte)(b_org[0]+1);
    match(storage.retrieveNext(),new String(b_org));
    b_org[0]=(byte)(b_org[0]+1);
    match(storage.retrieveNext(),new String(b_org));
        
  }
  
  @Test
  public void testPartialFlushWithCleanFailure() throws Exception{
    Assert.assertNull(storage.retrieve(new byte[8]));
    byte[] b = new byte[] { 48, 48, 48, 48, 98, 48, 52, 54, 49, 57, 55, 51, 52, 97, 53, 101, 56, 56, 97, 55, 98, 53, 52, 51, 98, 50, 102, 51, 49, 97, 97, 54, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 32, 48, 48, 58, 51, 49, 58, 52, 56, 1, 49, 48, 53, 53, 57, 52, 50, 1, 50, 1, 49, 53, 49, 49, 54, 49, 56, 52, 1, 49, 53, 49, 49, 57, 50, 49, 49, 1, 49, 53, 49, 50, 57, 54, 54, 53, 1, 49, 53, 49, 50, 49, 53, 52, 56, 1, 49, 48, 48, 56, 48, 51, 52, 50, 1, 55, 56, 56, 50, 54, 53, 52, 56, 1, 49, 1, 48, 1, 48, 46, 48, 1, 48, 46, 48, 1, 48, 46, 48 };
    byte[] b_org = new byte[] { 48, 48, 48, 48, 98, 48, 52, 54, 49, 57, 55, 51, 52, 97, 53, 101, 56, 56, 97, 55, 98, 53, 52, 51, 98, 50, 102, 51, 49, 97, 97, 54, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 32, 48, 48, 58, 51, 49, 58, 52, 56, 1, 49, 48, 53, 53, 57, 52, 50, 1, 50, 1, 49, 53, 49, 49, 54, 49, 56, 52, 1, 49, 53, 49, 49, 57, 50, 49, 49, 1, 49, 53, 49, 50, 57, 54, 54, 53, 1, 49, 53, 49, 50, 49, 53, 52, 56, 1, 49, 48, 48, 56, 48, 51, 52, 50, 1, 55, 56, 56, 50, 54, 53, 52, 56, 1, 49, 1, 48, 1, 48, 46, 48, 1, 48, 46, 48, 1, 48, 46, 48 };
    int length = b.length;
    byte[] address =  storage.store(b);
    Assert.assertNotNull(address);
    storage.flush();
    storage.clean(address);
    byte[] addr = null;
    for(int i = 0; i < 5; i++){
      b[0] = (byte) (b[0]+1);
      addr = storage.store(b);
    }
    storage = getStorage("1", true);
    Assert.assertNull(storage.retrieve(addr));
    for(int i = 0; i < 5; i++){
      b[0] = (byte) (b[0]+1);
      Assert.assertNull(storage.store(b));
    }    
    storage.flush();
    b_org[0]=(byte)(b_org[0]+1);
    match(storage.retrieve(address),new String(b_org));
    b_org[0]=(byte)(b_org[0]+1);
    match(storage.retrieveNext(),new String(b_org));
    b_org[0]=(byte)(b_org[0]+1);
    match(storage.retrieveNext(),new String(b_org));
        
  }
  
  /**
   * This test covers following use case
   * The file is flushed and then more data is written to the same file, but the new data is not flushed and file is not roll over and storage fails
   * The new storage comes up and client asks for data at the last returned address from earlier storage instance. The new storage returns null. 
   * Client stores the data again but the address returned this time is null and the retrieval of the earlier address now returns data 
   * @throws Exception
   */
  @Test
  public void testPartialFlushWithFailure() throws Exception{
    Assert.assertNull(storage.retrieve(new byte[8]));
    byte[] b = "ab".getBytes();
    byte[] address =  storage.store(b);
    Assert.assertNotNull(address);
    storage.flush();
    b = "cb".getBytes();
    byte[] addr = storage.store(b);
    storage = getStorage("1", true);
    Assert.assertNull(storage.retrieve(addr));
    Assert.assertNull(storage.store(b));
    storage.flush();
    match(storage.retrieve(address),"cb");    
  }
  
  private void match(byte[] data, String match){
    byte[] tempData = new byte[data.length - 8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    Assert.assertEquals("matched the stored value with retrieved value", match, new String(tempData));
  }
  
  @Test
  public void testStorage() throws IOException
  {
    Assert.assertNull(storage.retrieve(new byte[8]));
    byte[] b = new byte[200];
    byte[] identifier = new byte[8];
    Assert.assertNotNull(storage.store(b));
    Assert.assertNotNull(storage.store(b));
    Assert.assertNull(storage.retrieve(new byte[8]));
    Assert.assertNotNull(storage.store(b));
    Assert.assertNotNull(storage.store(b));
    storage.flush();
    byte[] data = storage.retrieve(new byte[8]);
    Assert.assertNotNull(storage.store(b));
    identifier = storage.store(b);
    byte[] tempData = new byte[data.length - 8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    Assert.assertEquals("matched the stored value with retrieved value", new String(b), new String(tempData));
    Assert.assertNull(storage.retrieve(identifier));
  }

  @Test
  public void testStorageWithRestore() throws IOException
  {
    Assert.assertNull(storage.retrieve(new byte[8]));
    byte[] b = new byte[200];
    Assert.assertNotNull(storage.store(b));
    storage.flush();
    storage.teardown();

    storage = getStorage("1", true);
    storage.store(b);
    storage.flush();
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    boolean exists = fs.exists(new Path(STORAGE_DIRECTORY + "/1/" + "1"));
    Assert.assertEquals("file shoule exist", true, exists);
  }

  @Test
  public void testCleanup() throws IOException
  {
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
  }

  @Test
  public void testNext() throws IOException
  {
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
    byte[] data = storage.retrieve(new byte[8]);
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
  }

   @Test
  public void testFailure() throws IOException
  {
    byte[] address;
    byte[] b = new byte[200];
    storage.retrieve(new byte[8]);
    for (int i = 0; i < 5; i++) {
      storage.store(b);
      address = storage.store(b);
      storage.flush();
      storage.clean(address);
    }
    storage.teardown();

    byte[] identifier = new byte[8];
    storage = getStorage("1", true);

    storage.retrieve(identifier);

    storage.store(b);
    storage.store(b);
    storage.store(b);
    storage.flush();
    byte[] data = storage.retrieve(identifier);
    byte[] tempData = new byte[data.length - 8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    Assert.assertEquals("matched the stored value with retrieved value", new String(b), new String(tempData));
  }

  @Test
  public void testCleanForUnflushedData() throws IOException
  {
    byte[] address = null;
    byte[] b = new byte[200];
    storage.retrieve(new byte[8]);
    for (int i = 0; i < 5; i++) {
      storage.store(b);
      address = storage.store(b);
      storage.flush();
      // storage.clean(address);
    }
    byte[] lastWrittenAddress = null;
    for (int i = 0; i < 5; i++) {
      storage.store(b);
      lastWrittenAddress = storage.store(b);
    }
    storage.clean(lastWrittenAddress);
    byte[] cleanedOffset = storage.readData(new Path(STORAGE_DIRECTORY + "/1/cleanoffsetFile"));
    Assert.assertArrayEquals(address, cleanedOffset);

  }

  @Test
  public void testCleanForFlushedData() throws IOException
  {
    byte[] b = new byte[200];
    storage.retrieve(new byte[8]);
    for (int i = 0; i < 5; i++) {
      storage.store(b);
      storage.store(b);
      storage.flush();
      // storage.clean(address);
    }
    byte[] lastWrittenAddress = null;
    for (int i = 0; i < 5; i++) {
      storage.store(b);
      lastWrittenAddress = storage.store(b);
    }
    storage.flush();
    storage.clean(lastWrittenAddress);
    byte[] cleanedOffset = storage.readData(new Path(STORAGE_DIRECTORY + "/1/cleanoffsetFile"));
    Assert.assertArrayEquals(lastWrittenAddress, cleanedOffset);

  }

  @Test
  public void testCleanForPartialFlushedData() throws IOException
  {
    byte[] b = new byte[8];
    storage.retrieve(new byte[8]);

    storage.store(b);
    byte[] address = storage.store("1a".getBytes());
    storage.flush();
    storage.clean(address);

    byte[] lastWrittenAddress = null;
    for (int i = 0; i < 5; i++) {
      storage.store((i+"").getBytes());
      lastWrittenAddress = storage.store(b);
    }
    Assert.assertNull(storage.retrieve(new byte[8]));
    Assert.assertNull(storage.retrieve(lastWrittenAddress));
    address = storage.store(b);
    storage.flush();
    Assert.assertNull(storage.retrieve(lastWrittenAddress));
  }

  @Test
  public void testRandomSequence() throws IOException
  {
    storage.retrieve(new byte[] {0, 0, 0, 0, 0, 0, 0, 0 });
    storage.store(new byte[] { 48, 48, 48, 51, 101, 100, 55, 56, 55, 49, 53, 99, 52, 101, 55, 50, 97, 52, 48, 49, 51, 99, 97, 54, 102, 57, 55, 53, 57, 100, 49, 99, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 32, 48, 48, 58, 48, 48, 58, 52, 54, 1, 52, 50, 49, 50, 51, 1, 50, 1, 49, 53, 49, 49, 52, 50, 54, 53, 1, 49, 53, 49, 49, 57, 51, 53, 49, 1, 49, 53, 49, 50, 57, 56, 50, 52, 1, 49, 53, 49, 50, 49, 55, 48, 55, 1, 49, 48, 48, 55, 55, 51, 57, 51, 1, 49, 57, 49, 52, 55, 50, 53, 52, 54, 49, 1, 49, 1, 48, 1, 48, 46, 48, 1, 48, 46, 48, 1, 48, 46, 48 });
    storage.flush();
    storage.clean(new byte[] { -109, 0, 0, 0, 0, 0, 0, 0 });
    storage.retrieve(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 });
    for (int i = 0; i < 2555; i++) {
      storage.store(new byte[] { 48, 48, 48, 55, 56, 51, 98, 101, 50, 54, 50, 98, 52, 102, 50, 54, 56, 97, 55, 56, 102, 48, 54, 54, 50, 49, 49, 54, 99, 98, 101, 99, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 32, 48, 48, 58, 48, 48, 58, 53, 49, 1, 49, 49, 49, 49, 54, 51, 57, 1, 50, 1, 49, 53, 49, 48, 57, 57, 56, 51, 1, 49, 53, 49, 49, 49, 55, 48, 52, 1, 49, 53, 49, 50, 49, 51, 55, 49, 1, 49, 53, 49, 49, 52, 56, 51, 49, 1, 49, 48, 48, 55, 49, 57, 56, 49, 1, 49, 50, 48, 50, 55, 54, 49, 54, 56, 53, 1, 49, 1, 48, 1, 48, 46, 48, 1, 48, 46, 48, 1, 48, 46, 48 });
      storage.flush();
    }
    storage.retrieve(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 });
    for (int i = 0; i < 1297; i++) {
      storage.retrieveNext();
    }
    storage.retrieve(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 });
    for (int i = 0; i < 1302; i++) {
      storage.retrieveNext();
    }
    storage.retrieve(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 });
    for (int i = 0; i < 1317; i++) {
      storage.retrieveNext();
    }
    storage.retrieve(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 });
    for (int i = 0; i < 2007; i++) {
      storage.retrieveNext();
    }
    storage.retrieve(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 });
    for (int i = 0; i < 2556; i++) {
      ;
      storage.retrieveNext();
    }
    storage.store(new byte[] { 48, 48, 48, 48, 98, 48, 52, 54, 49, 57, 55, 51, 52, 97, 53, 101, 56, 56, 97, 55, 98, 53, 52, 51, 98, 50, 102, 51, 49, 97, 97, 54, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 1, 50, 48, 49, 51, 45, 49, 49, 45, 48, 55, 32, 48, 48, 58, 51, 49, 58, 52, 56, 1, 49, 48, 53, 53, 57, 52, 50, 1, 50, 1, 49, 53, 49, 49, 54, 49, 56, 52, 1, 49, 53, 49, 49, 57, 50, 49, 49, 1, 49, 53, 49, 50, 57, 54, 54, 53, 1, 49, 53, 49, 50, 49, 53, 52, 56, 1, 49, 48, 48, 56, 48, 51, 52, 50, 1, 55, 56, 56, 50, 54, 53, 52, 56, 1, 49, 1, 48, 1, 48, 46, 48, 1, 48, 46, 48, 1, 48, 46, 48 });
    storage.flush();
    storage.retrieve(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 });
    for (int i = 0; i < 2062; i++) {
      storage.retrieveNext();

    }
  }

  private long calculateOffset(long fileOffset, long fileCounter)
  {
    return ((fileCounter << 32) | (fileOffset & 0xffffffffl));
  }

  
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(HDFSStorageTest.class);
}
