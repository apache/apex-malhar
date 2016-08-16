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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.util.Slice;

/**
 * This implementation will not move the memory if already output slices.
 * The memory could be moved if there hadn't any output slices.
 *
 */
public class FixedBlock extends Block
{
  private static final Logger logger = LoggerFactory.getLogger(FixedBlock.class);
      
  public static class OutOfBlockBufferMemoryException extends RuntimeException
  {
    private static final long serialVersionUID = 3813792889200989131L;
  }

  public FixedBlock()
  {
    super();
  }

  public FixedBlock(int capacity)
  {
    super(capacity);
  }

  @Override
  public void write(byte[] data, final int offset, final int length) throws OutOfBlockBufferMemoryException
  {
    super.write(data, offset, length);
  }

  /**
   * check the buffer size and reallocate if buffer is not enough
   * 
   * @param length
   */
  @Override
  protected void checkOrReallocateBuffer(int length) throws OutOfBlockBufferMemoryException
  {
    if (size + length > capacity && slices.size() > 0) {
      throw new OutOfBlockBufferMemoryException();
    }

    super.checkOrReallocateBuffer(length);
  }

  /**
   * Similar as toSlice, this method is used to get the information of the
   * object regards the data already write to buffer. But unlike toSlice() which
   * indicate all data of this object already done, this method can be called at
   * any time
   */
  public Slice getLastObjectSlice()
  {
    return new Slice(buffer, objectBeginOffset, size - objectBeginOffset);
  }

  public void discardLastObjectData()
  {
    if (objectBeginOffset == 0) {
      return;
    }
    size = objectBeginOffset;
  }

  public void moveLastObjectDataTo(FixedBlock newBuffer)
  {
    if(size > objectBeginOffset) {
      newBuffer.write(buffer, objectBeginOffset, size - objectBeginOffset);
      discardLastObjectData();
    }
  }
}
