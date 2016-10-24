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

import java.io.DataOutputStream;

import com.datatorrent.netlet.util.Slice;

public class DefaultSerializationBuffer extends DataOutputStream implements SerializationBuffer
{
  /*
   * Singleton read buffer for serialization
   */
  public static final DefaultSerializationBuffer READ_BUFFER = new DefaultSerializationBuffer(new WindowedBlockStream());

  public DefaultSerializationBuffer()
  {
    this(new WindowedBlockStream());
  }

  public DefaultSerializationBuffer(WindowedBlockStream windowableByteStream)
  {
    super(windowableByteStream);
  }

  @Override
  public WindowedBlockStream getOutputStream()
  {
    return (WindowedBlockStream)this.out;
  }

  /**
   * The super implement write one byte after another. I can impact the performance as it need to check the available space for each call.
   * And avoid throw IOException.
   */
  @Override
  public void write(byte[] b, int off, int len)
  {
    getOutputStream().write(b, off, len);
    incCount(len);
  }

  /**
  * Avoid throw IOException.
  */
  @Override
  public void write(int b)
  {
    getOutputStream().write(b);
    incCount(1);
  }

  /**
   * avoid throw IOException
   */
  @Override
  public void write(byte[] b)
  {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte value)
  {
    getOutputStream().write(value);
    incCount(1);
  }

  /**
   * copied from super class as it was defined as private
   * @param value
   */
  protected void incCount(int value)
  {
    int temp = written + value;
    if (temp < 0) {
      temp = Integer.MAX_VALUE;
    }
    written = temp;
  }

  @Override
  public void reset()
  {
    getOutputStream().reset();
  }

  @Override
  public void release()
  {
    getOutputStream().release();
  }

  @Override
  public Slice toSlice()
  {
    return getOutputStream().toSlice();
  }

  @Override
  public void beginWindow(long windowId)
  {
    getOutputStream().beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    getOutputStream().endWindow();
  }
}
