/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.chart;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.lib.util.HighLow;
import java.util.TreeMap;

/**
 * This is the base class for all chart operators that plot a histogram
 * @param <K> The key type
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class HistogramChartOperator<K> extends EnumChartOperator<K, HighLow>
{
  protected Number high;
  protected Number low;
  protected int numDivisions;
  protected NumberType xNumberType = NumberType.LONG;
  protected TreeMap<Number, HighLow> highLows = new TreeMap<Number, HighLow>();

  @Override
  public Type getChartType()
  {
    return Type.HISTOGRAM;
  }

  /**
   * Gets the upper bound on the x-axis for the histogram
   * @return
   */
  public Number getHigh()
  {
    return high;
  }

  /**
   * Sets the upper bound on the x-axis for the histogram
   * @param high
   */
  public void setHigh(Number high)
  {
    this.high = high;
  }

  /**
   * Gets the lower bound on the x-axis for the histogram
   * @return
   */
  public Number getLow()
  {
    return low;
  }

  /**
   * Sets the lower bound on the x-axis for the histogram
   * @param low
   */
  public void setLow(Number low)
  {
    this.low = low;
  }

  /**
   * Gets the number of divisions on the x-axis for the histogram
   * @return
   */
  public int getNumDivisions()
  {
    return numDivisions;
  }

  /**
   * Gets the number type of the values on the X-axis
   * @return
   */
  public NumberType getxNumberType()
  {
    return xNumberType;
  }

  /**
   * Sets the number type of the values on the X-axis
   * @param xNumberType
   */
  public void setxNumberType(NumberType xNumberType)
  {
    this.xNumberType = xNumberType;
  }

  /**
   * Sets the number of divisions on the x-axis for the histogram
   * @param numDivisions
   */
  public void setNumDivisions(int numDivisions)
  {
    this.numDivisions = numDivisions;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if (numDivisions == 0) {
      throw new RuntimeException("setNumDivisions() must be called first");
    }
    if (xNumberType == NumberType.DOUBLE) {
      double increment = (high.doubleValue() - low.doubleValue()) / numDivisions;
      double current = low.doubleValue();
      if (increment <= 0) {
        throw new RuntimeException("increment will be <= 0 given high, low and numDivisions");
      }
      for (int i = 0; i < numDivisions; i++) {
        highLows.put(current, new HighLow(i == numDivisions - 1 ? high : current + increment, current));
        current += increment;
      }
    }
    else {
      long increment = (high.longValue() - low.longValue()) / numDivisions;
      long current = low.longValue();
      if (increment <= 0) {
        throw new RuntimeException("increment will be < 0 given high, low and numDivisions");
      }
      for (int i = 0; i < numDivisions; i++) {
        if (current + increment - 1 > high.longValue()) {
          throw new RuntimeException("Too many divisions for long (integer) x values");
        }
        highLows.put(current, new HighLow(i == numDivisions - 1 ? high : current + increment - 1, current));
        current += increment;
      }
    }
  }

  /**
   * Tells the histogram chart operator how to get the X value given the tuple.
   * The value will be used to determine which bucket of the X-axis the tuple belongs to
   * @param tuple
   * @return The X value
   */
  public abstract Number convertTupleToXSource(Object tuple);

  @Override
  public HighLow convertTupleToX(Object tuple)
  {
    Number number = convertTupleToXSource(tuple);
    if (xNumberType == NumberType.DOUBLE) {
      if (number.doubleValue() < low.doubleValue() || number.doubleValue() > high.doubleValue()) {
        // out of range
        return null;
      }
    }
    else {
      if (number.longValue() < low.longValue() || number.longValue() > high.longValue()) {
        return null;
      }
    }
    return highLows.higherEntry(number).getValue();
  }

}
