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

package org.apache.apex.examples.partition;

import java.util.Random;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple operator that emits random integer.
 *
 * @since 3.7.0
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(RandomNumberGenerator.class);

  @Min(1)
  private int numTuples = 20;
  private transient int count = 0;

  private int sleepTime;
  private transient long curWindowId;
  private transient Random random = new Random();

  public final transient DefaultOutputPort<Integer> out = new DefaultOutputPort<Integer>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);

    long appWindowId = context.getValue(context.ACTIVATION_WINDOW_ID);
    sleepTime = context.getValue(context.SPIN_MILLIS);
    LOG.debug("Started setup, appWindowId = {}, sleepTime = {}", appWindowId, sleepTime);
  }

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
    LOG.debug("beginWindow: windowId = {}", windowId);
  }

  @Override
  public void emitTuples()
  {
    if (count++ < numTuples) {
      out.emit(random.nextInt());
    } else {
      LOG.debug("count = {}, time = {}", count, System.currentTimeMillis());

      try {
        // avoid repeated calls to this function
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        LOG.info("Sleep interrupted");
      }
    }
  }

  public int getNumTuples()
  {
    return numTuples;
  }

  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
  }

}
