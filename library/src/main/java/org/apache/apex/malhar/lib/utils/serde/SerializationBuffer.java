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

import org.apache.apex.malhar.lib.state.spillable.WindowListener;

import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.netlet.util.Slice;

/**
 * @since 3.6.0
 */
public class SerializationBuffer extends Output implements WindowCompleteListener, WindowListener
{
  /*
   * Singleton read buffer for serialization
   */
  public static final SerializationBuffer READ_BUFFER = new SerializationBuffer(new WindowedBlockStream());

  private WindowedBlockStream windowedBlockStream;

  @SuppressWarnings("unused")
  private SerializationBuffer()
  {
    this(new WindowedBlockStream());
  }

  public SerializationBuffer(WindowedBlockStream windowedBlockStream)
  {
    super(windowedBlockStream);
    this.windowedBlockStream = windowedBlockStream;
  }

  public long size()
  {
    return windowedBlockStream.size();
  }

  public long capacity()
  {
    return windowedBlockStream.capacity();
  }

  /**
   * This method should be called only after the whole object has been written
   * @return The slice which represents the object
   */
  public Slice toSlice()
  {
    this.flush();
    return windowedBlockStream.toSlice();
  }

  /**
   * reset the environment to reuse the resource.
   */
  public void reset()
  {
    windowedBlockStream.reset();
  }


  @Override
  public void beginWindow(long windowId)
  {
    windowedBlockStream.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    windowedBlockStream.endWindow();
  }

  public void release()
  {
    reset();
    windowedBlockStream.reset();
  }

  public WindowedBlockStream createWindowedBlockStream()
  {
    return new WindowedBlockStream();
  }

  public WindowedBlockStream createWindowedBlockStream(int capacity)
  {
    return new WindowedBlockStream(capacity);
  }

  public WindowedBlockStream getWindowedBlockStream()
  {
    return windowedBlockStream;
  }

  public void setWindowableByteStream(WindowedBlockStream windowableByteStream)
  {
    this.windowedBlockStream = windowableByteStream;
  }

  /**
   * reset for all windows with window id less than or equal to the input windowId
   * this interface doesn't call reset window for each windows. Several windows can be reset at the same time.
   * @param windowId
   */
  @Override
  public void completeWindow(long windowId)
  {
    windowedBlockStream.completeWindow(windowId);
  }

  public byte[] toByteArray()
  {
    return toSlice().toByteArray();
  }
}
