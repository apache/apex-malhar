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
package com.datatorrent.benchmark.spillable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.spillable.SpillableArrayListImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteArrayListMultimapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteMapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeLongSlice;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

public class SpillableTestOperator extends BaseOperator implements Operator.CheckpointNotificationListener
{
  public static transient final Logger logger = LoggerFactory.getLogger(SpillableTestOperator.class);
  
  public static final byte[] ID1 = new byte[] { (byte)1 };
  public static final byte[] ID2 = new byte[] { (byte)2 };
  public static final byte[] ID3 = new byte[] { (byte)3 };
  
  public SpillableByteArrayListMultimapImpl<String, String> multiMap;
  
  public ManagedStateSpillableStateStore store;
  
  public long totalCount = 0;
  public transient long countInWindow;
  public long minWinId = -1;
  public long committedWinId = -1;
  public long windowId;
  
  public SpillableByteMapImpl<Long, Long> windowToCount;
  
  public long shutdownCount = -1;
  
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
    if(++totalCount == shutdownCount)
      throw new RuntimeException("Test recovery. count = " + totalCount);
    countInWindow++;
    multiMap.put(""+windowId, tuple);
  }
  
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if(windowToCount == null) {
      windowToCount = createWindowToCountMap(store);
    }
    if(multiMap == null) {
      multiMap = createMultimap(store);
    }
    
    store.setup(context);
    multiMap.setup(context);

    checkData();
  }

  public void checkData()
  {
    logger.info("checkData(): totalCount: {}; minWinId: {}; committedWinId: {}; curWinId: {}", totalCount, this.minWinId, committedWinId, this.windowId);
    for(long winId = Math.max(committedWinId+1, minWinId); winId < this.windowId; ++winId) {
      Long count = this.windowToCount.get(winId);
      SpillableArrayListImpl<String> datas = (SpillableArrayListImpl<String>)multiMap.get("" + winId);
      if((datas == null && count != null) || (datas != null && count == null)) {
        logger.error("datas: {}; count: {}", datas, count);
      } else if(datas == null && count == null) {
        logger.error("Both datas and count are null. probably something wrong.");
      } else {
        int dataSize = datas.size();
        if((long)count != (long)dataSize) {
          logger.error("data size not equal: window Id: {}; datas size: {}; count: {}", winId, dataSize, count);
        } 
      }
    }
  }
  
  
  /**
   * {@inheritDoc}
   */
  @Override
  public void beginWindow(long windowId)
  {
    store.beginWindow(windowId);
    multiMap.beginWindow(windowId);
    if(minWinId < 0) {
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

    if(windowId % 10 == 0) {
      long startTime = System.currentTimeMillis();
      checkData();
      logger.info("checkData() took {} millis.", System.currentTimeMillis() - startTime);
    }
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
  
  public static SpillableByteArrayListMultimapImpl<String, String> createMultimap(SpillableStateStore store)
  {
    return new SpillableByteArrayListMultimapImpl<String, String>(store, ID1, 0L, new SerdeStringSlice(),
        new SerdeStringSlice());
  }
  
  public static SpillableByteMapImpl<String, String> createMap(SpillableStateStore store)
  {
    return new SpillableByteMapImpl<String, String>(store, ID2, 0L, new SerdeStringSlice(),
        new SerdeStringSlice());
  }
  
  public static SpillableByteMapImpl<Long, Long> createWindowToCountMap(SpillableStateStore store)
  {
    return new SpillableByteMapImpl<Long, Long>(store, ID3, 0L, new SerdeLongSlice(),
        new SerdeLongSlice());
  }
}
