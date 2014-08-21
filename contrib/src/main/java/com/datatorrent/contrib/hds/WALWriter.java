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

import java.io.IOException;

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
  public void append(byte[] key, byte[] value) throws IOException;

  /**
   * Flush data to persistent storage.
   * @throws IOException
   */
  public void flush() throws IOException;

  /**
   * Return count of byte which may not be flushed to persistent storage.
   * @return
   */
  public long getUnflushedCount();

  /**
   * Returns offset of the file, till which data is known to be persisted on the disk.
   * @return
   */
  public long getCommittedLen();

  /**
   * Returns file size, last part of the file may not be persisted on disk.
   * @return
   */
  public long logSize();
}
