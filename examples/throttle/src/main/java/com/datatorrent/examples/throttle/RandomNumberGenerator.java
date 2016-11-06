/**
 * Put your copyright and license info here.
 */
package com.datatorrent.examples.throttle;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a simple operator that emits random number.
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator
{
  private int numTuples = 1000;
  private int origNumTuples = numTuples;
  private transient int count = 0;

  private static final Logger logger = LoggerFactory.getLogger(RandomNumberGenerator.class);

  public final transient DefaultOutputPort<Double> out = new DefaultOutputPort<Double>();

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
  }

  @Override
  public void emitTuples()
  {
    if (count++ < numTuples) {
      out.emit(Math.random());
    }
  }

  // Simple suspend and
  public void suspend() {
    logger.debug("Slowing down");
    numTuples = 0;
  }

  public void normal() {
    logger.debug("Normal");
    numTuples = origNumTuples;
  }

  public int getNumTuples()
  {
    return numTuples;
  }

  /**
   * Sets the number of tuples to be emitted every window.
   * @param numTuples number of tuples
   */
  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
    this.origNumTuples = numTuples;
  }
}
