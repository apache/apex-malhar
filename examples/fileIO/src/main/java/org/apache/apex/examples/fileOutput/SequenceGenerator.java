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

package org.apache.apex.examples.fileOutput;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * Simple operator that emits pairs of integers where the first value is the
 * operator id and the second forms elements of an arithmetic progression whose
 * increment is 'divisor' (can be changed dynamically).
 */
public class SequenceGenerator extends BaseOperator implements InputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(SequenceGenerator.class);

  // properties

  @Min(1)
  private int maxTuples = 5;           // max number of tuples per window
  @Min(1)
  private long divisor = 1;            // only values divisible by divisor are output

  private int sleepTime;

  private long nextValue;              // next value to emit

  // transient fields

  private transient int numTuples = 0;    // number emitted in current window
  private transient long id;              // operator id

  public final transient DefaultOutputPort<Long[]> out = new DefaultOutputPort<>();

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    id = context.getId();
    sleepTime = context.getValue(OperatorContext.SPIN_MILLIS);
    LOG.debug("Leaving setup, id = {}, sleepTime = {}, divisor = {}",
        id, sleepTime, divisor);
  }

  @Override
  public void beginWindow(long windowId)
  {
    numTuples = 0;
    super.beginWindow(windowId);
  }

  @Override
  public void emitTuples()
  {
    if (numTuples < maxTuples) {
      // nextValue will normally be divisible by divisor but the divisor can be changed
      // externally (e.g. after a repartition) so find next value that is divisible by
      // divisor
      //
      final long rem = nextValue % divisor;
      if (0 != rem) {
        nextValue += (divisor - rem);
      }
      ++numTuples;
      out.emit(new Long[]{id, nextValue});
      nextValue += divisor;
    } else {

      try {
        // avoid repeated calls to this function
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        LOG.info("Sleep interrupted");
      }
    }
  }

  // getters and setters

  public long getDivisor()
  {
    return divisor;
  }

  public void setDivisor(long v)
  {
    divisor = v;
  }

  public int getMaxTuples()
  {
    return maxTuples;
  }

  public void setMaxTuples(int v)
  {
    maxTuples = v;
  }
}
