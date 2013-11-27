package com.datatorrent.storage;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSStorageTest
{
  public static final String STORAGE_DIRECTORY = "target/testdata/hdfs";
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
    storage.retrieve(val);
   // logger.debug("{}",storage.retrieveNext());
   // storage.flush();
    r.close();
  }

  @Test
  public void testCleanup() throws IOException
  {
    Storage storage = DiskStorage.getInstance(STORAGE_DIRECTORY, false);
    if (storage == null) {
      return;
    }
    storage.clean(1);
  }
  
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
      for(int i = 0; i< 10000000; i++)
        storage.store(b);
      storage.store(r.readLine().getBytes());
      System.out.println(storage.retrieve(val));
      System.out.println(storage.retrieveNext());
      r.close();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }
  private static final Logger logger = LoggerFactory.getLogger(HDFSStorageTest.class);
}
