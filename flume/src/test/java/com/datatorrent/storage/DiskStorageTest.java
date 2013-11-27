package com.datatorrent.storage;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiskStorageTest
{
  public static final String STORAGE_DIRECTORY = "target/testdata/local";
  @Test
  public void testStorage() throws IOException
  {
    Storage storage = DiskStorage.getInstance(STORAGE_DIRECTORY, true);
    if (storage == null) {
      return;
    }
    RandomAccessFile r = new RandomAccessFile("src/test/resources/TestInput.txt", "r");
    r.seek(0);
    storage.store(r.readLine().getBytes());
    //logger.debug(new String(storage.retrieveNext()));
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

  private static final Logger logger = LoggerFactory.getLogger(DiskStorageTest.class);
}
