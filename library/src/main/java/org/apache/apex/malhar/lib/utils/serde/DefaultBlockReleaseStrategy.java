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

import java.util.Arrays;

import org.apache.commons.collections.buffer.CircularFifoBuffer;

/**
 * This implementation get the minimum number of free blocks in the period to release.
 *
 */
public class DefaultBlockReleaseStrategy implements BlockReleaseStrategy
{
  public static final int DEFAULT_PERIOD = 60; // 60 reports
  protected CircularFifoBuffer freeBlockNumQueue;
  private Integer[] tmpArray;

  public DefaultBlockReleaseStrategy()
  {
    this(DEFAULT_PERIOD);
  }

  public DefaultBlockReleaseStrategy(int period)
  {
    freeBlockNumQueue = new CircularFifoBuffer(period);
    tmpArray = new Integer[period];
    Arrays.fill(tmpArray, 0);
  }

  /**
   * The stream calls this to report to the strategy how many blocks are free currently.
   * @param freeBlockNum
   */
  @Override
  public void currentFreeBlocks(int freeBlockNum)
  {
    if (freeBlockNum < 0) {
      throw new IllegalArgumentException("The number of free blocks could not less than zero.");
    }
    freeBlockNumQueue.add(freeBlockNum);
  }

  /**
   * Get how many blocks that can be released
   * @return
   */
  @Override
  public int getNumBlocksToRelease()
  {
    int minNum = Integer.MAX_VALUE;
    for (Object num : freeBlockNumQueue) {
      minNum = Math.min((Integer)num, minNum);
    }
    return minNum;
  }


  /**
   * report how many blocks that have been released.
   * @param numReleasedBlocks
   */
  @Override
  public void releasedBlocks(int numReleasedBlocks)
  {
    if (numReleasedBlocks == 0) {
      return;
    }
    if (numReleasedBlocks < 0) {
      throw new IllegalArgumentException("Num of released blocks should not be negative");
    }
    /**
     * decrease by released blocks
     */
    for (Object num : freeBlockNumQueue) {
      freeBlockNumQueue.add(Math.max((Integer)num - numReleasedBlocks, 0));
    }
  }

}
