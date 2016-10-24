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
package org.apache.apex.malhar.lib.utils.serde;

import com.datatorrent.netlet.util.Slice;

/**
 * This interface provides methods to write binary data and manage blocks.
 * The process of write an object is as follow:
 *   - write(): Write the data of the object, the write() methods could call multiple times.
 *   - toSlice(): Return the slice that represents the object. This method also indicates the end of current object and is ready for next object.
 */
public abstract interface ByteStream
{

  /**
   * @return The size of the data in stream
   */
  public long size();

  /**
   * @return The current capacity of the stream.
   */
  public long capacity();

  /**
   * @return The slice of the serialized object.
   */
  public Slice toSlice();

  /**
   * Reset the stream. Invalid all previous written data for reuse the buffer
   */
  public void reset();

  /**
   * Release allocated resource.
   */
  public void release();

}
