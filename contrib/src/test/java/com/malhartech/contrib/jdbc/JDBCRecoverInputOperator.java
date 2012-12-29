/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.jdbc;

import com.malhartech.api.ActivationListener;
import com.malhartech.api.CheckpointListener;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import com.malhartech.bufferserver.util.Codec;
import com.malhartech.util.CircularBuffer;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang<zhongjian@malhar-inc.com>
 */
public class JDBCRecoverInputOperator implements InputOperator, CheckpointListener, ActivationListener<OperatorContext>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCRecoverInputOperator.class);
  public final transient DefaultOutputPort<HashMap<String, Object>> output = new DefaultOutputPort<HashMap<String, Object>>(this);
  transient boolean first;
  transient long windowId;
  transient CircularBuffer<HashMap<String, Object>> holdingBuffer;
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
      output.emit(holdingBuffer.pollUnsafe());
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
    logger.debug(" beginWindow: {}", Codec.getStringWindowId(windowId));
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
    holdingBuffer = new CircularBuffer<HashMap<String, Object>>(1024 * 1024);
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
