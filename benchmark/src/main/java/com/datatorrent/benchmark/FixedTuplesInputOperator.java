/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.benchmark;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * <p>FixedTuplesInputOperator class.</p>
 *
 * @since 0.3.2
 */
public class FixedTuplesInputOperator implements InputOperator
{
  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();
  private int count;
  private boolean firstTime;
  private ArrayList<Integer> millis = new ArrayList<Integer>();

  @Override
  public void emitTuples()
  {
    if (firstTime) {
      long start = System.currentTimeMillis();
      for (int i = count; i-- > 0;) {
        output.emit(new byte[64]);
      }
      firstTime = false;
      millis.add((int)(System.currentTimeMillis() - start));
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    firstTime = true;
  }

  @Override
  public void endWindow()
  {
    if (millis.size() % 10 == 0) {
      logger.info("millis = {}", millis);
      millis.clear();
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
    logger.info("millis = {}", millis);
  }

  /**
   * @return the count
   */
  public int getCount()
  {
    return count;
  }

  /**
   * Sets the number of tuples to emit.
   * 
   * @param count the number of tuples to emit
   */
  public void setCount(int count)
  {
    this.count = count;
  }

  private static final Logger logger = LoggerFactory.getLogger(FixedTuplesInputOperator.class);
}
