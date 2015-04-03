/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.benchmarks;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import javax.validation.constraints.Min;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class LongArrayGenerator implements InputOperator
{
  @Min(1)
  private int numArrays = 1000;
  @Min(1)
  private int arrayLength = 10;

  private transient int arrayCounter = 0;

  public final transient DefaultOutputPort<Long[]> output = new DefaultOutputPort<Long[]>();

  public LongArrayGenerator()
  {
  }

  @Override
  public void emitTuples()
  {
    for(;
        arrayCounter < numArrays;
        arrayCounter++) {
      Long[] outputArray = new Long[arrayLength];//(long) arrayCounter;

      for(int arrayIndex = 0;
          arrayIndex < arrayLength;
          arrayIndex++) {
        outputArray[arrayIndex] = (long) arrayCounter;
      }

      output.emit(outputArray);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    arrayCounter = 0;
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

  /**
   * @return the numArrays
   */
  public int getNumArrays()
  {
    return numArrays;
  }

  /**
   * @param numArrays the numArrays to set
   */
  public void setNumArrays(int numArrays)
  {
    this.numArrays = numArrays;
  }

  /**
   * @return the arrayLength
   */
  public int getArrayLength()
  {
    return arrayLength;
  }

  /**
   * @param arrayLength the arrayLength to set
   */
  public void setArrayLength(int arrayLength)
  {
    this.arrayLength = arrayLength;
  }
}
