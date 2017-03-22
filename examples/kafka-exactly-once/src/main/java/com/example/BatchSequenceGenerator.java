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
package com.example;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * Simple operator that emits Strings from 1 to maxTuplesTotal
 */
public class BatchSequenceGenerator extends BaseOperator implements InputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(BatchSequenceGenerator.class);

  // properties

  @Min(1)
  private int maxTuplesTotal;     // max number of tuples in total
  @Min(1)
  private int maxTuples;           // max number of tuples per window

  private int sleepTime;

  private int numTuplesTotal = 0;

  //start with empty windows to ensure tests run reliable
  private int emptyWindowsCount = 0;

  // transient fields

  private transient int numTuples = 0;    // number emitted in current window

  public final transient DefaultOutputPort<String> out = new DefaultOutputPort<>();

  @Override
  public void beginWindow(long windowId)
  {
    numTuples = 0;
    super.beginWindow(windowId);
    LOG.debug("beginWindow: " + windowId);
    ++emptyWindowsCount;
  }

  @Override
  public void emitTuples()
  {

    if (numTuplesTotal < maxTuplesTotal && numTuples < maxTuples && emptyWindowsCount > 3) {
      ++numTuplesTotal;
      ++numTuples;
      out.emit(String.valueOf(numTuplesTotal));
      LOG.info("Line emitted: " + numTuplesTotal);

      try {
        // avoid repeated calls to this function
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        LOG.info("Sleep interrupted");
      }
    }

  }

  public int getMaxTuples()
  {
    return maxTuples;
  }

  public void setMaxTuples(int v)
  {
    maxTuples = v;
  }

  public int getMaxTuplesTotal()
  {
    return maxTuplesTotal;
  }

  public void setMaxTuplesTotal(int v)
  {
    maxTuplesTotal = v;
  }
}
