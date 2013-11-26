package com.datatorrent.storage;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.Test;

public class HDFSStorageTest
{
  @Test
  public void testStorage() throws IOException{
    Storage storage =HDFSStorage.getInstance("src/test/resources/",true);
    if(storage == null)
      return;
    RandomAccessFile r = new RandomAccessFile("src/test/resources/TestInput.txt","r");
    r.seek(0);
    storage.store(r.readLine().getBytes());
    System.out.println(new String(storage.retrieveNext()));
  }
  
  @Test
  public void testCleanup() throws IOException{
    Storage storage = DiskStorage.getInstance("src/test/resources/",false);
    if(storage == null)
      return;
    storage.clean(1);
  }

}
