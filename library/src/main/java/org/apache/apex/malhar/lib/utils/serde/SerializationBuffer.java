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
 * This interface declares methods of writing binary data and manages the buffer.
 *
 */
public interface SerializationBuffer extends ResettableWindowListener
{
  /**
   * write value. it could be part of the object
   * @param value
   */
  public void write(byte[] value);


  /**
   * write value. it could be part of the object
   *
   * @param value
   * @param offset
   * @param length
   */
  public void write(byte[] value, int offset, int length);

  /**
   * write one byte
   * @param value
   */
  public void write(byte value);

  /**
   * write int
   * @param value
   */
  public void write(int value);

  /**
   * write long
   * @param value
   */
  public void write(long value);

  /**
   * Writes the string to the buffer prefixed by the string length
   * @param string
   * @return
   */
  public int writeStringPrefixedByLength(String string);

  /**
   * write String
   * @param string
   * @return The length of the serialized string
   */
  public int writeString(String string);

  /**
   * reset to reuse the resource.
   */
  public void reset();

  /**
   * release allocated resource.
   */
  public void release();

  /**
   * This method should be called only after the whole object has been written.
   * Call toSlice() means the end of one object and start with another new object
   * @return The slice which represents the object
   */
  public Slice toSlice();
}
