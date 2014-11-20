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
import java.util.Set;
import java.util.TreeMap;

import com.datatorrent.common.util.Slice;

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
   * HDS Data File Format Reader
   */
  interface HDSFileReader extends Closeable
  {
    /**
     * Read the entire contents of the underlying file into a TreeMap structure
     * @param data
     * @throws IOException
     */
    void readFully(TreeMap<Slice, byte[]> data) throws IOException;

    /**
     * Repositions the pointer to the beginning of the underlying file.
     * @throws IOException
     */
    void reset() throws IOException;

    /**
     * Searches for a matching key, and positions the pointer before the start of the key.
     * @param key Byte array representing the key
     * @throws IOException
     * @return true if a given key is found
     */
    boolean seek(Slice key) throws IOException;

    /**
     * Reads next available key/value pair starting from the current pointer position
     * into Slice objects and advances pointer to next key.  If pointer is at the end
     * of the file, false is returned, and Slice objects remains unmodified.
     *
     * @param key Empty slice object
     * @param value Empty slice object
     * @return True if key/value were successfully read, false otherwise
     * @throws IOException
     */
    boolean next(Slice key, Slice value) throws IOException;

  }

  /**
   * HDS Data File Format Writer
   */
  interface HDSFileWriter extends Closeable {
    /**
     * Appends key/value pair to the underlying file.
     * @param key
     * @param value
     * @throws IOException
     */
    void append(byte[] key, byte[] value) throws IOException;

    /**
     * Returns number of bytes written to the underlying stream.
     * @return
     * @throws IOException
     */
    long getBytesWritten() throws IOException;
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

  /**
   * Register an exporter
   */
  public void registerExporter(HDSFileExporter exporter);

  /**
   * Remove all registered exporters
   */
  public void clearExporters();

  /**
   * Callback to perform exports on newly flushed files.
   */
  public void exportFiles(long bucketKey, Set<String> filesAdded, Set<String> filesToDelete) throws IOException;

}
