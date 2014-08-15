/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.hds;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.TreeMap;

/**
 * Encapsulate management of meta information and underlying file system interaction.
 * <p>
 * The state of the object will be serialized as part of check-pointing, providing an opportunity to persist meta information and reach consistent state.
 */
public interface BucketFileSystem extends Closeable
{

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

  // placeholder interface
  interface HDSFileReader extends Closeable {
    void readFully(TreeMap<byte[], byte[]> data) throws IOException;
  }

  // placeholder interface
  interface HDSFileWriter extends Closeable {
    void append(byte[] key, byte[] value) throws IOException;
    int getFileSize();
  }

  /**
   * Obtain a reader for the given data file. Since existing file formats may depend on the file system directly (vs.
   * work just based on InputStream), construction of the reader is part of the file system abstraction itself.
   */
  public HDSFileReader getReader(long bucketKey, String fileName) throws IOException;

  /**
   * Obtain a writer for the given data file. Since existing file formats may depend on the file system directly (vs.
   * work just based on OutputStream), construction of the writer is part of the file system abstraction itself.
   */
  public HDSFileWriter getWriter(long bucketKey, String fileName) throws IOException;

}
