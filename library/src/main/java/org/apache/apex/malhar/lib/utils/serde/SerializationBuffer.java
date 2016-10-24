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

import java.io.DataOutput;

import org.apache.apex.malhar.lib.state.spillable.WindowListener;

import com.datatorrent.netlet.util.Slice;

/**
 * This interface declares methods of writing binary data and manages the buffer.
 *
 */
public interface SerializationBuffer extends DataOutput, WindowListener
{
  /**
   * Override the super interface to avoid throw IOException.
   */
  @Override
  public void write(int b);

  /**
   * Override the super interface to avoid throw IOException.
   */
  @Override
  public void write(byte[] b, int off, int len);

  /**
   * Override the super interface to avoid throw IOException.
   */
  @Override
  void write(byte[] b);

  /**
   * write one byte
   * @param value
   */
  public void write(byte value);

  /**
   * reset to reuse the resource.
   */
  void reset();

  /**
   * release allocated resource.
   */
  void release();

  /**
   * This method should be called only after the whole object has been written.
   * Call toSlice() means the end of one object and start with another new object
   * @return The slice which represents the object
   */
  Slice toSlice();

  /**
   *
   * @return The output stream
   */
  WindowedBlockStream getOutputStream();
}
