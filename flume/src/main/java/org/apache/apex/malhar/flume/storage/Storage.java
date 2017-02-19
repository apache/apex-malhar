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
package org.apache.apex.malhar.flume.storage;

import com.datatorrent.netlet.util.Slice;

/**
 * <p>Storage interface.</p>
 *
 * @since 0.9.2
 */
public interface Storage
{
  /**
   * key in the context for Unique identifier for the storage which may be used to recover from failure.
   */
  String ID = "id";

  /**
   * This stores the bytes and returns the unique identifier to retrieve these bytes
   *
   * @param bytes
   * @return
   */
  byte[] store(Slice bytes);

  /**
   * This returns the data bytes for the current identifier and the identifier for next data bytes. <br/>
   * The first eight bytes contain the identifier and the remaining bytes contain the data
   *
   * @param identifier
   * @return
   */
  byte[] retrieve(byte[] identifier);

  /**
   * This returns data bytes and the identifier for the next data bytes. The identifier for current data bytes is based
   * on the retrieve method call and number of retrieveNext method calls after retrieve method call. <br/>
   * The first eight bytes contain the identifier and the remaining bytes contain the data
   *
   * @return
   */
  byte[] retrieveNext();

  /**
   * This is used to clean up the files identified by identifier
   *
   * @param identifier
   */
  void clean(byte[] identifier);

  /**
   * This flushes the data from stream
   *
   */
  void flush();

}
