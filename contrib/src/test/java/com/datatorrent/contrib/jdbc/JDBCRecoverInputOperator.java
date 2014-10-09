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
package com.datatorrent.contrib.jdbc;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.api.Operator.CheckpointListener;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class JDBCRecoverInputOperator implements InputOperator, CheckpointListener, ActivationListener<OperatorContext>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCRecoverInputOperator.class);
  public final transient DefaultOutputPort<HashMap<String, Object>> output = new DefaultOutputPort<HashMap<String, Object>>();
  transient boolean first;
  transient long windowId;
  transient ArrayBlockingQueue<HashMap<String, Object>> holdingBuffer;
  boolean failed;
  transient boolean transient_fail;
  int maximumTuples;
  private static int columnCount = 7;

  public void setMaximumTuples(int count)
  {
    maximumTuples = count;
  }

  @Override
  public void emitTuples()
  {
//    if (first) {
//      logger.debug("generating tuple {}", Codec.getStringWindowId(windowId));
//      output.emit(windowId);
    if (first) {
      HashMap<String, Object> map = holdingBuffer.poll();
      if (map == null) {
        return;
      }
      output.emit(map);
      first = false;
      if (--maximumTuples == 0) {
        throw new RuntimeException(new InterruptedException("Just want to stop!"));
      }

      if (maximumTuples == 25) {
        throw new RuntimeException("failure before checkpointing");
      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    logger.debug(" beginWindow: {}", windowId);
    this.windowId = windowId;
    first = true;
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    holdingBuffer = new ArrayBlockingQueue<HashMap<String, Object>>(1024 * 1024);
    transient_fail = !failed;
    failed = true;
    logger.debug("RecoverableInputOperator setup context:" + context.getId());
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void checkpointed(long windowId)
  {
    if (transient_fail) {
      throw new RuntimeException("Failure Simulation from " + this);
    }
  }

  @Override
  public void committed(long windowId)
  {
  }

  @Override
  public void activate(OperatorContext ctx)
  {
    for (int i = 0; i < maximumTuples; ++i) {
      HashMap<String, Object> hm = new HashMap<String, Object>();
      for (int j = 1; j <= columnCount; ++j) {
        hm.put("prop" + (j), new Integer((columnCount * i) + j));
      }
      holdingBuffer.add(hm);
    }
  }

  @Override
  public void deactivate()
  {
  }
}
