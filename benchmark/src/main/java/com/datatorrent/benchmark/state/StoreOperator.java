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
package com.datatorrent.benchmark.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.managed.ManagedStateImpl;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.netlet.util.Slice;

public class StoreOperator extends BaseOperator implements Operator.CheckpointNotificationListener
{
  private static final Logger logger = LoggerFactory.getLogger(StoreOperator.class);

  protected static final int numOfWindowPerStatistics = 10;

  protected ManagedStateImpl store;
  protected long bucketId = 1;

  protected long lastCheckPointWindowId = -1;
  protected long currentWindowId;
  protected long tupleCount = 0;
  protected int windowCountPerStatistics = 0;
  protected long statisticsBeginTime = 0;

  public final transient DefaultInputPort<KeyValPair<byte[], byte[]>> input = new DefaultInputPort<KeyValPair<byte[], byte[]>>()
  {
    @Override
    public void process(KeyValPair<byte[], byte[]> tuple)
    {
      processTuple(tuple);
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    store.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    store.beginWindow(windowId);
    if (statisticsBeginTime <= 0) {
      statisticsBeginTime = System.currentTimeMillis();
    }
  }

  @Override
  public void endWindow()
  {
    store.endWindow();
    if (++windowCountPerStatistics >= numOfWindowPerStatistics) {
      logStatistics();
      windowCountPerStatistics = 0;
    }
  }

  protected void processTuple(KeyValPair<byte[], byte[]> tuple)
  {
    Slice key = new Slice(tuple.getKey());
    Slice value = new Slice(tuple.getValue());
    store.put(bucketId, key, value);
    ++tupleCount;
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
    store.committed(windowId);
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    store.beforeCheckpoint(windowId);
    logger.info("beforeCheckpoint {}", windowId);
  }

  public ManagedStateImpl getStore()
  {
    return store;
  }

  public void setStore(ManagedStateImpl store)
  {
    this.store = store;
  }

  protected void logStatistics()
  {
    long spentTime = System.currentTimeMillis() - statisticsBeginTime;
    logger.info("Time Spent: {}, Processed tuples: {}, rate: {}", spentTime, tupleCount, tupleCount / spentTime);

    statisticsBeginTime = System.currentTimeMillis();
    tupleCount = 0;
  }
}
