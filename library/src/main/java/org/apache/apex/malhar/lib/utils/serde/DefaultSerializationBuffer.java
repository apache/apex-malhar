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

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

public class DefaultSerializationBuffer implements SerializationBuffer
{
  /*
   * Singleton read buffer for seralization
   */
  public static final DefaultSerializationBuffer READ_BUFFER = new DefaultSerializationBuffer(new WindowedBlockStream());

  protected WindowedByteStream windowedByteStream;

  @SuppressWarnings("unused")
  private DefaultSerializationBuffer()
  {
    //kyro
  }

  public DefaultSerializationBuffer(WindowedByteStream windowableByteStream)
  {
    this.windowedByteStream = windowableByteStream;
  }

  /**
   * write value. it can be part of the object
   * @param value
   */
  @Override
  public void write(byte[] value)
  {
    windowedByteStream.write(value);
  }

  /**
   * write value. it can be part of the object
   *
   * @param value
   * @param offset
   * @param length
   */
  @Override
  public void write(byte[] value, int offset, int length)
  {
    windowedByteStream.write(value, offset, length);
  }

  protected final transient byte[] tmpLengthAsBytes = new byte[4];
  protected final transient MutableInt tmpOffset = new MutableInt(0);
  public void setObjectLength(int length)
  {
    try {
      GPOUtils.serializeInt(length, tmpLengthAsBytes, new MutableInt(0));
      windowedByteStream.write(tmpLengthAsBytes);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public long size()
  {
    return windowedByteStream.size();
  }

  public long capacity()
  {
    return windowedByteStream.capacity();
  }

  /**
   * This method should be called only after the whole object has been written
   * @return The slice which represents the object
   */
  @Override
  public Slice toSlice()
  {
    return windowedByteStream.toSlice();
  }


  /**
   * reset the environment to reuse the resource.
   */
  @Override
  public void reset()
  {
    windowedByteStream.reset();
  }


  @Override
  public void beginWindow(long windowId)
  {
    windowedByteStream.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    windowedByteStream.endWindow();
  }

  @Override
  public void release()
  {
    reset();
    windowedByteStream.reset();
  }

  public WindowedByteStream createWindowableByteStream()
  {
    return new WindowedBlockStream();
  }

  public WindowedByteStream createWindowableByteStream(int capacity)
  {
    return new WindowedBlockStream(capacity);
  }

  public WindowedByteStream getWindowableByteStream()
  {
    return windowedByteStream;
  }

  public void setWindowableByteStream(WindowedByteStream windowableByteStream)
  {
    this.windowedByteStream = windowableByteStream;
  }

  /**
   * reset for all windows with window id less than or equal to the input windowId
   * this interface doesn't call reset window for each windows. Several windows can be reset at the same time.
   * @param windowId
   */
  @Override
  public void resetUpToWindow(long windowId)
  {
    windowedByteStream.resetUpToWindow(windowId);
  }

  //used for byte, int and long
  private transient byte[] tmpBytes = new byte[8];
  @Override
  public void write(byte data)
  {
    tmpBytes[0] = data;
    write(tmpBytes, 0, 1);
  }

  @Override
  public void write(int value)
  {
    tmpBytes[0] = (byte)((value >> 24) & 0xFF);
    tmpBytes[1] = (byte)((value >> 16) & 0xFF);
    tmpBytes[2] = (byte)((value >> 8) & 0xFF);
    tmpBytes[3] = (byte)(value & 0xFF);

    write(tmpBytes, 0, 4);
  }

  @Override
  public void write(long value)
  {
    tmpBytes[0] = (byte)((value >> 56) & 0xFFL);
    tmpBytes[1] = (byte)((value >> 48) & 0xFFL);
    tmpBytes[2] = (byte)((value >> 40) & 0xFFL);
    tmpBytes[3] = (byte)((value >> 32) & 0xFFL);
    tmpBytes[4] = (byte)((value >> 24) & 0xFFL);
    tmpBytes[5] = (byte)((value >> 16) & 0xFFL);
    tmpBytes[6] = (byte)((value >> 8) & 0xFFL);
    tmpBytes[7] = (byte)(value & 0xFFL);

    write(tmpBytes, 0, 8);
  }

  /**
   *
   * @param string
   */
  @Override
  public int writeString(String string)
  {
    /**
     * TODO: String getBytes() in fact create an temporary memory,
     * We can provide our implementation to avoid allocate temporary memory
     */
    byte[] bytes = string.getBytes();
    write(bytes);
    return bytes.length;
  }

  /**
   * Usually the String need to serialized prefixed with length in order to deserialize
   * @param string
   * @return
   */
  @Override
  public int writeStringPrefixedByLength(String string)
  {
    /**
     * TODO: We probably can provide function such like reserve() and fill() to write the value first and then fill the length
     * to avoid temporary memory
     */
    byte[] bytes = string.getBytes();
    write(bytes.length);
    write(bytes);
    return bytes.length + 4;
  }
}
