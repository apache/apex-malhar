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

import java.util.Map;

import org.apache.apex.malhar.lib.state.spillable.WindowListener;
import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.collect.Maps;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

/**
 * This class keep the object with length value format.try to get rid of memory slice and memory data copy Basically used by memory
 * serialize
 *
 */
public class LengthValueBuffer implements ResetableWindowListener
{
  protected WindowableByteStream windowableByteStream;
  protected Map<Integer, Integer> placeHolderIdentifierToValue = Maps.newHashMap();

  public LengthValueBuffer()
  {
    windowableByteStream = createWindowableByteStream();
  }

  public LengthValueBuffer(int capacity)
  {
    windowableByteStream = createWindowableByteStream(capacity);
  }

  protected final transient byte[] tmpLengthAsBytes = new byte[4];
  protected final transient MutableInt tmpOffset = new MutableInt(0);

  public void setObjectLength(int length)
  {
    try {
      GPOUtils.serializeInt(length, tmpLengthAsBytes, new MutableInt(0));
      windowableByteStream.write(tmpLengthAsBytes);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * only set value.
   * 
   * @param value
   * @param offset
   * @param length
   */
  public void setObjectValue(byte[] value, int offset, int length)
  {
    windowableByteStream.write(value, offset, length);
  }

  /**
   * set value and length. the input value is value only, it doesn't include
   * length information.
   * 
   * @param value
   * @param offset
   * @param length
   */
  public void setObjectWithValue(byte[] value, int offset, int length)
  {
    setObjectLength(length);
    setObjectValue(value, offset, length);
  }

  public void setObjectWithValue(byte[] value)
  {
    setObjectWithValue(value, 0, value.length);
  }

  /**
   * mark place hold for length. In some case, we don't know the length until
   * really processed data. mark place holder for set length later.
   * 
   * @return the identity for this placeholder
   */
  protected static final byte[] lengthPlaceHolder = new byte[]{0, 0, 0, 0};

  public int markPlaceHolderForLength()
  {
    try {
      int offset = windowableByteStream.size();
      windowableByteStream.write(lengthPlaceHolder);
      return offset;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public int getSize()
  {
    return windowableByteStream.size();
  }

  /**
   * 
   * @param placeHolderId
   * @param length
   */
  public void setValueForLengthPlaceHolder(int placeHolderId, int length)
  {
    //don't convert to byte array now. just keep the information
    placeHolderIdentifierToValue.put(placeHolderId, length);
  }

  /**
   * This method should be called only the whole object has been written
   * @return The slice which represents the object
   */
  public Slice toSlice()
  {
    Slice slice = windowableByteStream.toSlice();

    if (placeHolderIdentifierToValue != null && !placeHolderIdentifierToValue.isEmpty()) {
      MutableInt offset = new MutableInt();
      for (Map.Entry<Integer, Integer> entry : placeHolderIdentifierToValue.entrySet()) {
        offset.setValue(slice.offset + entry.getKey());
        GPOUtils.serializeInt(entry.getValue(), slice.buffer, offset);
      }
    }
    
    return slice;
  }


  /**
   * reset the environment to reuse the resource.
   */
  public void reset()
  {
    windowableByteStream.reset();
    placeHolderIdentifierToValue.clear();
  }
  

  @Override
  public void beginWindow(long windowId)
  {
    windowableByteStream.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    windowableByteStream.endWindow();    
  }
  
  /**
   * reset for all windows which window id less or equal input windowId
   * this interface doesn't enforce to call reset window for each windows. Several windows can be reset at the same time.
   * @param windowId
   */
  public void resetUpToWindow(long windowId)
  {
    windowableByteStream.resetUpToWindow(windowId);
  }

  public void release()
  {
    reset();
    windowableByteStream.release();
  }
  
  public WindowableByteStream createWindowableByteStream()
  {
    return new WindowableBlocksStream();
  }

  public WindowableByteStream createWindowableByteStream(int capacity)
  {
    return new WindowableBlocksStream(capacity);
  }
}
