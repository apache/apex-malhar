package com.datatorrent.flume.storage;

import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;

public class HDFSStoragePerformance
{

  public static void main(String[] args)
  {
    Context ctx = new Context();
    ctx.put(HDFSStorage.BASE_DIR_KEY, ".");
    ctx.put(HDFSStorage.RESTORE_KEY, Boolean.toString(false));
    Storage storage = new HDFSStorage();
    ((Configurable) storage).configure(ctx);

    byte[] b = new byte[1024];
    for (int i = 0; i < 10000; i++)
      storage.store(b);

  }
}
