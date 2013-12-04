package com.datatorrent.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import org.apache.flume.Context;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiskStorageTest
{
  public static final String STORAGE_DIRECTORY = "target/testdata/local";

  @Test
  public void testStorage() throws IOException
  {
    Context context = new Context();
    context.put("baseDir", STORAGE_DIRECTORY);
    context.put("restore", Boolean.toString(false));

    DiskStorage storage = new DiskStorage();
    storage.configure(context);
    RandomAccessFile r = new RandomAccessFile("src/test/resources/TestInput.txt", "r");
    r.seek(0);
    storage.store(r.readLine().getBytes());
  }

  @Test
  public void testCleanup() throws IOException
  {
    Context context = new Context();
    context.put("baseDir", STORAGE_DIRECTORY);
    context.put("restore", Boolean.toString(false));

    DiskStorage storage = new DiskStorage();
    storage.configure(context);
    storage.clean(1);
  }

  private static final Logger logger = LoggerFactory.getLogger(DiskStorageTest.class);
}
