/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.hds;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Encapsulate management of meta information and underlying file system interaction.
 * <p>
 * The state of the object will be serialized as part of check-pointing, providing an opportunity to persist meta information and reach consistent state.
 */
public interface BucketFileSystem extends Closeable
{

  public class BucketFileMeta
  {
    /**
     * Name of file (relative to bucket)
     */
    String name;
    /**
     * Lower bound sequence key
     */
    byte[] fromSeq;

    @Override
    public String toString()
    {
      return "BucketFileMeta [name=" + name + ", fromSeq=" + Arrays.toString(fromSeq) + "]";
    }

  }

  /**
   * Performs setup operations eg. crate database connections, delete events of windows greater than last committed
   * window, etc.
   */
  void init();

  DataOutputStream getOutputStream(long bucketKey, String fileName) throws IOException;
  DataInputStream getInputStream(long bucketKey, String fileName) throws IOException;

  /**
   * Atomic file rename.
   * @param bucketKey
   * @param oldName
   * @param newName
   * @throws IOException
   */
  void rename(long bucketKey, String oldName, String newName) throws IOException;
  void delete(long bucketKey, String fileName) throws IOException;

}
