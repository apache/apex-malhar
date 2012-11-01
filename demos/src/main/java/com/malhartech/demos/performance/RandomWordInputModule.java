/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.demos.performance;

import com.malhartech.api.Context.OperatorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class RandomWordInputModule implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(RandomWordInputModule.class);
  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>(this);
  transient long lastWindowId = 0;
  transient int count;
//  int totalIterations = 0;

  @Override
  public void emitTuples(long windowId)
  {
    if (windowId == lastWindowId) {
      output.emit(new byte[64]);
      count++;
    }
    else {
      for (int i = count--; i-- > 0;) {
        output.emit(new byte[64]);
      }
      lastWindowId = windowId;
//      if (++totalIterations > 20) {
//        Thread.currentThread().interrupt();
//      }
    }
  }

  @Override
  public void replayTuples(long arg0) {
  }


  @Override
  public void beginWindow()
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }
}
