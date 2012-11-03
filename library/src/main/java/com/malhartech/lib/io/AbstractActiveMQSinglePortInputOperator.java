/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public abstract class AbstractActiveMQSinglePortInputOperator<T> extends AbstractActiveMQInputOperator<T>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQSinglePortInputOperator.class);

  /**
   * The single output port.
   */
  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>(this);

  /**
   * Implement InputOperator Interface.
   */
  @Override
  public void emitTuples()
  {
    //logger.debug("emitTuples got called from {}", this);
    int bufferSize = holdingBuffer.size();
    for (int i = getTuplesBlast() < bufferSize ? getTuplesBlast() : bufferSize; i-- > 0;) {
      T tuple = (T)holdingBuffer.pollUnsafe();
      outputPort.emit(tuple);
      logger.debug("emitTuples() got called from {} with tuple: {}", this, tuple);
    }
  }
}
