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
package org.apache.apex.benchmark.window;

import java.nio.ByteBuffer;
import java.util.Random;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * @since 3.7.0
 */
public abstract class AbstractGenerator<T> extends BaseOperator implements InputOperator
{
  public final transient DefaultOutputPort<T> data = new DefaultOutputPort<T>();

  protected int emitBatchSize = 1000;
  protected byte[] val = ByteBuffer.allocate(1000).putLong(1234).array();
  protected int rate = 20000;
  protected int emitCount = 0;
  protected final Random random = new Random();
  protected int range = 1000 * 60; // one minute range of hot keys

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    emitCount = 0;
  }

  public int getEmitBatchSize()
  {
    return emitBatchSize;
  }

  public void setEmitBatchSize(int emitBatchSize)
  {
    this.emitBatchSize = emitBatchSize;
  }

  public int getRate()
  {
    return rate;
  }

  public void setRate(int rate)
  {
    this.rate = rate;
  }

  public int getRange()
  {
    return range;
  }

  public void setRange(int range)
  {
    this.range = range;
  }

  @Override
  public void emitTuples()
  {
    for (int i = 0; i < emitBatchSize && emitCount < rate; i++) {
      data.emit(generateNextTuple());
      emitCount++;
    }
  }

  protected abstract T generateNextTuple();
}
