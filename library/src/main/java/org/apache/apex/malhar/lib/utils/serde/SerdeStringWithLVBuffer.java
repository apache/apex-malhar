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

import com.google.common.base.Preconditions;

import com.datatorrent.netlet.util.Slice;

public class SerdeStringWithLVBuffer extends SerdeStringSlice implements SerToLVBuffer<String>
{
  //implement with shared buff
  protected LengthValueBuffer buffer;
  
  /**
   * if don't use SerdeStringWithLVBuffer.serialize(String), can ignore LVBuffer
   */
  public SerdeStringWithLVBuffer()
  {
  }
  
  public SerdeStringWithLVBuffer(LengthValueBuffer buffer)
  {
    this.buffer = Preconditions.checkNotNull(buffer);
  }
  
  @Override
  public Slice serialize(String object)
  {
    if (buffer == null) {
      buffer = new LengthValueBuffer();
    }
    serTo(object, buffer);
    return buffer.toSlice();
  }

// implement with tmp buffer  
//  @Override
//  public Slice serialize(String object)
//  {
//    LVBuffer buffer = new LVBuffer();
//    serTo(object, buffer);
//    return buffer.toSlice();
//  }
  
  @Override
  public void serTo(String str, LengthValueBuffer buffer)
  {
    buffer.setObjectWithValue(str.getBytes());
  }

  @Override
  public void reset()
  {
    if (buffer != null) {
      buffer.reset();
    }
  }
}
