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

package org.apache.apex.examples.throttle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple operator that emits random number.
 *
 * @since 3.7.0
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator
{
  private int numTuples = 1000;
  private int origNumTuples = numTuples;
  private transient int count = 0;

  private static final Logger logger = LoggerFactory.getLogger(RandomNumberGenerator.class);

  public final transient DefaultOutputPort<Double> out = new DefaultOutputPort<Double>();

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
  }

  @Override
  public void emitTuples()
  {
    if (count++ < numTuples) {
      out.emit(Math.random());
    }
  }

  // Simple suspend and
  public void suspend()
  {
    logger.debug("Slowing down");
    numTuples = 0;
  }

  public void normal()
  {
    logger.debug("Normal");
    numTuples = origNumTuples;
  }

  public int getNumTuples()
  {
    return numTuples;
  }

  /**
   * Sets the number of tuples to be emitted every window.
   * @param numTuples number of tuples
   */
  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
    this.origNumTuples = numTuples;
  }
}
