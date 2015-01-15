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
package com.datatorrent.contrib.hdht;

import java.io.Closeable;
import java.io.IOException;

import com.datatorrent.common.util.Slice;

public interface HDHT
{
  public interface Reader
  {
    byte[] get(long bucketKey, Slice key) throws IOException;
  }

  public interface Writer
  {
    void put(long bucketKey, Slice key, byte[] value) throws IOException;
    byte[] getUncommitted(long bucketKey, Slice key);
  }

  /**
   * A simple iterator type interface to read WAL
   * entries.
   */
  public interface WALReader extends Closeable
  {
    static final byte[] DELETED = {};

    /**
     * Close WAL after read.
     */
    @Override
    public void close() throws IOException;

    /**
     * Seek to middle of the WAL. This is used primarily during recovery,
     * when we need to start recovering data from middle of WAL file.
     *
     * @param offset   seek offset.
     * @throws IOException
     */
    public void seek(long offset) throws IOException;

    /**
     * Advance WAL by one entry, returns true if it can advance, else false
     * in case of any other error throws an Exception.
     * @return true if next data item is read successfully, false if data can not be read.
     * @throws IOException
     */
    public boolean advance() throws IOException;

    /**
     * Return current entry from WAL, returns null if end of file has reached.
     * @return MutableKeyValue
     */
    public MutableKeyValue get();
  }

  /**
   * WAL writer interface.
   */
  public interface WALWriter extends Cloneable
  {
    /**
     * flush pending data to disk and close file.
     * @throws IOException
     */
    public void close() throws IOException;


    /**
     * Append key value byte arrays to WAL, data is not flushed immediately to the disks.
     * @param key    key byte array.
     * @param value  value byte array.
     * @throws IOException
     */
    public void append(Slice key, byte[] value) throws IOException;

    /**
     * Flush data to persistent storage.
     * @throws IOException
     */
    public void flush() throws IOException;

    /**
     * Return count of byte which may not be flushed to persistent storage.
     * @return The count of byte which may not be flushed to persistent storage.
     */
    public long getUnflushedCount();

    /**
     * Returns offset of the file, till which data is known to be persisted on the disk.
     * @return The offset of the file, till which data is known to be persisted on the disk.
     */
    public long getCommittedLen();

    /**
     * Returns file size, last part of the file may not be persisted on disk.
     * @return The file size, last part of the file may not be persisted on disk.
     */
    public long logSize();
  }

}
