/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hds;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.TreeMap;

/**
 * Abstraction for file system and format interaction.
 */
public interface HDSFileAccess extends Closeable
{
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

  /**
   * Truncate file, used for WAL recovery.
   * @param bucketKey
   * @param fileName
   * @param size
   * @throws IOException
   */
  void truncate(long bucketKey, String fileName, long size) throws IOException;

  // placeholder interface
  interface HDSFileReader extends Closeable {
    void readFully(TreeMap<byte[], byte[]> data) throws IOException;

    /**
     * Searches for a matching key, and returns the corresponding value.  Exception is thrown if a matching key is not found.
     * @param key Byte array representing the key
     * @return Byte array representing the value
     * @throws IOException
     */
    byte[] getValue(byte[] key) throws IOException;

    /**
     * Repositions the pointer to the beginning of the underlying file.
     * @throws IOException
     */
    void reset() throws IOException;

    /**
     * Searches for a matching key, and positions the pointer before the start of the key.
     * @param key Byte array representing the key
     * @throws IOException
     */
    void seek(byte[] key) throws IOException;

    /**
     * Advances the pointer to the next available key in the underlying file, or returns false if end of file is reached.
     * @return true if another key is available, false otherwise
     * @throws IOException
     */
    boolean next() throws IOException;

    /**
     * Reads the next available key/value into {@link com.datatorrent.contrib.hds.MutableKeyValue} starting at the current pointer location, or throws Exception if end of file is reached.
     * @param mutableKeyValue A key/value pair represented by byte arrays
     * @throws IOException
     */
    public void get(MutableKeyValue mutableKeyValue) throws IOException;
  }

  // placeholder interface
  interface HDSFileWriter extends Closeable {
    /**
     * Appends key/value pair to the underlying file.
     * @param key
     * @param value
     * @throws IOException
     */
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
