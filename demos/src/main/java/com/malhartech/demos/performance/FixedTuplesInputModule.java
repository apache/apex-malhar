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
public class FixedTuplesInputModule implements InputOperator
{
  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>(this);
  private int count;
  private boolean firstTime;

  @Override
  public void emitTuples()
  {
    if (firstTime) {
      for (int i = count; i-- > 0;) {
        output.emit(new byte[64]);
      }
      firstTime = false;
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    firstTime = true;
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

  private static final Logger logger = LoggerFactory.getLogger(FixedTuplesInputModule.class);

  /**
   * @return the count
   */
  public int getCount()
  {
    return count;
  }

  /**
   * @param count the count to set
   */
  public void setCount(int count)
  {
    this.count = count;
  }
}
