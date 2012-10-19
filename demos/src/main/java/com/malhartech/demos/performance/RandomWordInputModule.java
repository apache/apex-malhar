/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.demos.performance;

import com.malhartech.api.AsyncInputOperator;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.dag.*;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class RandomWordInputModule implements AsyncInputOperator
{
  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>(this);
  long lastWindowId = 0;
  int count = 1;
//  int totalIterations = 0;

  @Override
  public void injectTuples(long windowId)
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
  public void beginWindow()
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorConfiguration config) throws FailedOperationException
  {
  }

  @Override
  public void activated(OperatorContext context)
  {
  }

  @Override
  public void deactivated()
  {
  }

  @Override
  public void teardown()
  {
  }
}
