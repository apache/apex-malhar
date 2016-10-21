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

import java.io.OutputStream;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import com.datatorrent.netlet.util.Slice;

/**
 * A stream is a collection of blocks
 * BlockStream avoids copying the data that are already exposed to the caller
 *
 */
public class BlockStream extends OutputStream implements ByteStream
{
  private static final Logger logger = LoggerFactory.getLogger(BlockStream.class);

  //the initial capacity of each block
  protected final int blockCapacity;

  protected Map<Integer, Block> blocks = Maps.newHashMap();
  //the index of current block, valid block index should >= 0
  protected int currentBlockIndex = 0;
  protected long size = 0;

  protected Block currentBlock;

  public BlockStream()
  {
    this(Block.DEFAULT_BLOCK_SIZE);
  }

  public BlockStream(int blockCapacity)
  {
    this.blockCapacity = blockCapacity;
  }

  @Override
  public void write(byte[] data)
  {
    write(data, 0, data.length);
  }

  /**
   * This write could be called multiple times for an object.
   * The write method makes sure the same object only write to one block
   *
   * @param data
   * @param offset
   * @param length
   */
  @Override
  public void write(byte[] data, final int offset, final int length)
  {
    //start with a block which at least can hold this data
    currentBlock = getOrCreateCurrentBlock();
    try {
      currentBlock.write(data, offset, length);
    } catch (Block.OutOfBlockBufferMemoryException e) {
      //use next block
      Block previousBlock = null;

      previousBlock = moveToNextBlock();
      if (!currentBlock.isFresh()) {
        throw new RuntimeException("New block is not fresh.");
      }
      if (!previousBlock.isClear()) {
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
  protected Block moveToNextBlock()
  {
    Block previousBlock = currentBlock;

    ++currentBlockIndex;
    currentBlock = getOrCreateCurrentBlock();
    if (!currentBlock.isFresh()) {
      throw new RuntimeException("Assigned non fresh block.");
    }
    return previousBlock;
  }

  protected Block getOrCreateCurrentBlock()
  {
    Block block = blocks.get(currentBlockIndex);
    if (block == null) {
      block = new Block(blockCapacity);
      blocks.put(currentBlockIndex, block);
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
    for (Block block : blocks.values()) {
      capacity += block.capacity();
    }
    return capacity;
  }

  /**
   *
   * this is the call that represents the end of an object
   */
  @Override
  public Slice toSlice()
  {
    return blocks.get(currentBlockIndex).toSlice();
  }

  /**
   * resets all blocks
   */
  @Override
  public void reset()
  {
    currentBlockIndex = 0;
    size = 0;
    for (Block block : blocks.values()) {
      block.reset();
    }
  }

  @Override
  public void release()
  {
    reset();
    blocks.clear();
  }

  @Override
  public void write(int b)
  {
    write((byte)b);
  }

  private byte[] tmpByteAsArray = new byte[1];
  public void write(byte value)
  {
    tmpByteAsArray[0] = value;
    write(tmpByteAsArray);
  }
}
