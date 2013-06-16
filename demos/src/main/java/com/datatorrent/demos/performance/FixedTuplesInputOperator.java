/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.demos.performance;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class FixedTuplesInputOperator implements InputOperator
{
  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>(this);
  private int count;
  private boolean firstTime;
  private ArrayList<Integer> millis = new ArrayList<Integer>();

  @Override
  public void emitTuples()
  {
    if (firstTime) {
      long start = System.currentTimeMillis();
      for (int i = count; i-- > 0;) {
        output.emit(new byte[64]);
      }
      firstTime = false;
      millis.add((int)(System.currentTimeMillis() - start));
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
    if (millis.size() % 10 == 0) {
      logger.info("millis = {}", millis);
      millis.clear();
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
    logger.info("millis = {}", millis);
  }

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

  private static final Logger logger = LoggerFactory.getLogger(FixedTuplesInputOperator.class);
}
