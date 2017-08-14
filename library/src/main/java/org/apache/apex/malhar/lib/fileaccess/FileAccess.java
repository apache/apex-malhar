/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.fileaccess;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import com.datatorrent.netlet.util.Slice;

/**
 * Abstraction for file system and format interaction.
 *
 * @since 2.0.0
 */
@InterfaceStability.Evolving
public interface FileAccess extends Closeable
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

  void deleteBucket(long bucketKey) throws IOException;

  long getFileSize(long bucketKey, String s) throws IOException;

  /**
   * Checks if a file exists under a bucket.
   *
   * @param bucketKey bucket key
   * @param fileName  file name
   * @return true if file exists; false otherwise.
   * @throws IOException
   */
  boolean exists(long bucketKey, String fileName) throws IOException;

  /**
   * Lists the files in the bucket path.
   *
   * @param bucketKey bucket key
   * @return status of all the files in the bucket path if the bucket exists; null otherwise.
   * @throws IOException
   */
  RemoteIterator<LocatedFileStatus> listFiles(long bucketKey) throws IOException;

  /**
   * Data File Format Reader
   */
  interface FileReader extends Closeable
  {
    /**
     * Read the entire contents of the underlying file into a TreeMap structure
     * @param data
     * @throws IOException
     */
    void readFully(TreeMap<Slice, Slice> data) throws IOException;

    /**
     * Repositions the pointer to the beginning of the underlying file.
     * @throws IOException
     */
    void reset() throws IOException;

    /**
     * Searches for the minimum key that is greater than or equal to the given key, and positions the pointer before the start of
     * that key.
     *
     * @param key Byte array representing the key
     * @throws IOException
     * @return true if the pointer points to a key that equals the given key, false if the key is greater than the given key or if there is no key that is greater than or equal to the key
     */
    boolean seek(Slice key) throws IOException;

    /**
     * Reads the key/value pair starting at the current pointer position
     * into Slice objects.  If pointer is at the end
     * of the file, false is returned, and Slice objects remains unmodified.
     *
     * @param key Empty slice object
     * @param value Empty slice object
     * @return True if key/value were successfully read, false otherwise
     * @throws IOException
     */
    boolean peek(Slice key, Slice value) throws IOException;

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

    /**
     * Returns whether the pointer is at the end of the file
     *
     * @return True if the pointer is at the end of the file
     * @throws IOException
     */
    boolean hasNext();

  }

  /**
   * Data File Format Writer
   */
  interface FileWriter extends Closeable
  {
    /**
     * Appends key/value pair to the underlying file.
     * @deprecated use {@link FileWriter#append(Slice, Slice)} instead.
     * @param key
     * @param value
     * @throws IOException
     */
    @Deprecated
    void append(byte[] key, byte[] value) throws IOException;

    /**
     * Appends key/value pair to the underlying file.
     * @param key
     * @param value
     * @throws IOException
     */
    void append(Slice key, Slice value) throws IOException;

    /**
     * Returns number of bytes written to the underlying stream.
     * @return The bytes written.
     * @throws IOException
     */
    long getBytesWritten() throws IOException;
  }

  /**
   * Obtain a reader for the given data file. Since existing file formats may depend on the file system directly (vs.
   * work just based on InputStream), construction of the reader is part of the file system abstraction itself.
   */
  public FileReader getReader(long bucketKey, String fileName) throws IOException;

  /**
   * Obtain a writer for the given data file. Since existing file formats may depend on the file system directly (vs.
   * work just based on OutputStream), construction of the writer is part of the file system abstraction itself.
   */
  public FileWriter getWriter(long bucketKey, String fileName) throws IOException;

}
