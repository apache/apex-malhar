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
package org.apache.apex.malhar.lib.wal;

import java.io.IOException;

import com.datatorrent.netlet.util.Slice;

/**
 * This interface represents a write ahead log that can be used by operator.
 * the WAL is split into two interfaces, a WALWriter which allows writing
 * data, and WALReader which provides iterator like interface to read entries
 * written to the WAL.
 *
 * @param <READER> Type of WAL Reader
 * @param <WRITER> WAL Pointer Type.
 *
 * @since 3.4.0
 */
public interface WAL<READER extends WAL.WALReader, WRITER extends WAL.WALWriter>
{
  void setup();

  void teardown();

  void beforeCheckpoint(long window);

  void committed(long window);

  READER getReader();

  WRITER getWriter();

  /**
   * Provides iterator like interface to read entries from the WAL.
   * @param <P> type of Pointer in the WAL
   */
  interface WALReader<P> extends AutoCloseable
  {
    /**
     * Seek to middle of the WAL. This is used primarily during recovery,
     * when we need to start recovering data from middle of WAL file.
     */
    void seek(P pointer) throws IOException;

    /**
     * Returns the next entry from WAL. It returns null if data isn't available.
     * @return next entry when available; null otherwise.
     */
    Slice next() throws IOException;

    /**
     * Skips the next entry in the WAL.
     */
    void skipNext() throws IOException;

    /**
     * Returns the start pointer from which data is available to read.<br/>
     * WAL Writer supports purging of aged data so the start pointer will change over time.
     *
     * @return the start pointer from which the data is available to read.
     */
    P getStartPointer();
  }

  /**
   * Provide method to write entries to the WAL.
   * @param <P> type of Pointer in the WAL
   */
  interface WALWriter<P> extends AutoCloseable
  {
    /**
     * Write an entry to the WAL
     */
    int append(Slice entry) throws IOException;

    /**
     *
     * @return current pointer in the WAL.
     */
    P getPointer();

    /**
     * Deletes the WAL data till the given pointer.<br/>
     *
     * @param pointer pointer in the WAL
     * @throws IOException
     */
    void delete(P pointer) throws IOException;

  }
}
