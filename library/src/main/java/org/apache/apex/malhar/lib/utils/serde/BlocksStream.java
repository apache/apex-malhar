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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import com.datatorrent.netlet.util.Slice;

/**
 * A stream implemented by an array of block ( or a map from index to block )
 * BlockBuffer can reallocate the memory and copy the data if buffer is not
 * enough But it is not the good solution if there already have slices output.
 * BlockStream try to avoid copy the data which already output slices
 * 
 */
public class BlocksStream implements ByteStream
{
  private static final Logger logger = LoggerFactory.getLogger(BlocksStream.class);
  
  public static final int DEFAULT_BLOCK_CAPACITY = 1000000;

  //the initial capacity of each block
  protected final int blockCapacity;

  protected Map<Integer, FixedBlock> blocks = Maps.newHashMap();
  //the index of current block, valid block index should >= 0
  protected int currentBlockIndex = 0;
  protected long size = 0;

  protected FixedBlock currentBlock;

  public BlocksStream()
  {
    this(DEFAULT_BLOCK_CAPACITY);
  }

  public BlocksStream(int blockCapacity)
  {
    this.blockCapacity = blockCapacity;
  }

  @Override
  public void write(byte[] data)
  {
    write(data, 0, data.length);
  }

  /**
   * This write could be the first or the continuous write for an object. For t
   * 
   * @param data
   * @param offset
   * @param length
   */
  @Override
  public void write(byte[] data, final int offset, final int length)
  {
    //start with a block which at least can hold current data
    currentBlock = getOrCreateCurrentBlock();
    try {
      currentBlock.write(data, offset, length);
    } catch (FixedBlock.OutOfBlockBufferMemoryException e) {
      //use next block
      FixedBlock previousBlock = null;

      previousBlock = moveToNextBlock();
      if(!currentBlock.isFresh()) {
        throw new RuntimeException("New block is not fresh.");
      }
      if(!previousBlock.isClear()) {
        previousBlock.moveLastObjectDataTo(currentBlock);
      }
      currentBlock.write(data, offset, length);
    }
    size += length;
  }

  /**
   * 
   * @return The previous block
   */
  protected FixedBlock moveToNextBlock()
  {
    FixedBlock previousBlock = currentBlock;
    
    ++currentBlockIndex;
    currentBlock = getOrCreateCurrentBlock();
    if (!currentBlock.isFresh()) {
      throw new RuntimeException("Assigned non fresh block.");
    }
    return previousBlock;
  }

  protected FixedBlock getOrCreateCurrentBlock()
  {
    FixedBlock block = blocks.get(currentBlockIndex);
    if (block == null) {
      block = new FixedBlock(blockCapacity);
      blocks.put(currentBlockIndex, block);
      if (blocks.size() % 50 == 0) {
        logger.info("Assigned blocks: {}, size of each block: {}", blocks.size(), blockCapacity);
      }
    }
    return block;
  }

  @Override
  public long size()
  {
    return size;
  }

  @Override
  public long capacity()
  {
    long capacity = 0;
    for (FixedBlock block : blocks.values()) {
      capacity += block.capacity();
    }
    return capacity;
  }
  
  /**
   * 
   * this is the last call which represent the end of an object
   */
  @Override
  public Slice toSlice()
  {
    return blocks.get(currentBlockIndex).toSlice();
  }

  /**
   * Don't need to maintain original buffer now.
   */
  @Override
  public void reset()
  {
    currentBlockIndex = 0;
    size = 0;
    for (FixedBlock block : blocks.values()) {
      block.reset();
    }
  }

  @Override
  public void release()
  {
    reset();
    blocks.clear();
  }
}
