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
package org.apache.apex.benchmark.spillable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.spillable.SpillableArrayListImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableArrayListMultimapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableMapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.LongSerde;
import org.apache.apex.malhar.lib.utils.serde.StringSerde;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.ShutdownException;
import com.datatorrent.common.util.BaseOperator;

/**
 * @since 3.6.0
 */
public class SpillableTestOperator extends BaseOperator implements Operator.CheckpointNotificationListener
{
  private static final Logger logger = LoggerFactory.getLogger(SpillableTestOperator.class);

  public static final byte[] ID1 = new byte[] {(byte)1};
  public static final byte[] ID2 = new byte[] {(byte)2};
  public static final byte[] ID3 = new byte[] {(byte)3};

  public SpillableArrayListMultimapImpl<String, String> multiMap;

  public ManagedStateSpillableStateStore store;

  public long totalCount = 0;
  public transient long countInWindow;
  public long minWinId = -1;
  public long committedWinId = -1;
  public long windowId;

  public SpillableMapImpl<Long, Long> windowToCount;

  public long shutdownCount = -1;

  public static Throwable errorTrace;

  private long lastLogTime;
  private long beginTime;

  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      processTuple(tuple);
    }
  };

  public void processTuple(String tuple)
  {
    if (++totalCount == shutdownCount) {
      throw new RuntimeException("Test recovery. count = " + totalCount);
    }
    countInWindow++;
    multiMap.put("" + windowId, tuple);
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if (windowToCount == null) {
      windowToCount = createWindowToCountMap(store);
    }
    if (multiMap == null) {
      multiMap = createMultimap(store);
    }

    store.setup(context);
    windowToCount.setup(context);
    multiMap.setup(context);

    lastLogTime = System.currentTimeMillis();
    beginTime = lastLogTime;

    checkData();
  }

  public void checkData()
  {
    long startTime = System.currentTimeMillis();
    logger.debug("check data: totalCount: {}; minWinId: {}; committedWinId: {}; curWinId: {}", totalCount,
        this.minWinId, committedWinId, this.windowId);
    for (long winId = Math.max(committedWinId + 1, minWinId); winId < this.windowId; ++winId) {
      Long count = this.windowToCount.get(winId);
      SpillableArrayListImpl<String> datas = (SpillableArrayListImpl<String>)multiMap.get("" + winId);
      String msg;
      if (((datas == null && count != null) || (datas != null && count == null)) || (datas == null && count == null)) {
        msg = "Invalid data/count. datas: " + datas + "; count: " + count;
        logger.error(msg);
        errorTrace = new RuntimeException(msg);
        throw new ShutdownException();
      } else {
        int dataSize = datas.size();
        if ((long)count != (long)dataSize) {
          msg = String.format("data size not equal: window Id: %d; datas size: %d; count: %d", winId, dataSize, count);
          logger.error(msg);
          errorTrace = new RuntimeException(msg);
          throw new ShutdownException();
        }
      }
    }
    logger.info("check data took {} millis.", System.currentTimeMillis() - startTime);
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public void beginWindow(long windowId)
  {
    store.beginWindow(windowId);
    multiMap.beginWindow(windowId);
    if (minWinId < 0) {
      minWinId = windowId;
    }

    this.windowId = windowId;
    countInWindow = 0;
  }

  @Override
  public void endWindow()
  {
    multiMap.endWindow();
    windowToCount.put(windowId, countInWindow);
    windowToCount.endWindow();
    store.endWindow();

    if (windowId % 10 == 0) {
      checkData();
      logStatistics();
    }
  }

  private long lastTotalCount = 0;

  public void logStatistics()
  {
    long countInPeriod = totalCount - lastTotalCount;
    long timeInPeriod = System.currentTimeMillis() - lastLogTime;
    long totalTime = System.currentTimeMillis() - beginTime;
    logger.info(
        "Statistics: total count: {}; period count: {}; total rate (per second): {}; period rate (per second): {}",
        totalCount, countInPeriod, totalCount * 1000 / totalTime, countInPeriod * 1000 / timeInPeriod);
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    store.beforeCheckpoint(windowId);
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
    this.committedWinId = windowId;
    store.committed(windowId);
  }

  public static SpillableArrayListMultimapImpl<String, String> createMultimap(SpillableStateStore store)
  {
    return new SpillableArrayListMultimapImpl<String, String>(store, ID1, 0L, new StringSerde(),
        new StringSerde());
  }

  public static SpillableMapImpl<String, String> createMap(SpillableStateStore store)
  {
    return new SpillableMapImpl<String, String>(store, ID2, 0L, new StringSerde(),
        new StringSerde());
  }

  public static SpillableMapImpl<Long, Long> createWindowToCountMap(SpillableStateStore store)
  {
    return new SpillableMapImpl<Long, Long>(store, ID3, 0L, new LongSerde(),
        new LongSerde());
  }
}
