/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

/**
 *
 * A mutable double for basic operations. Better performance<p>
 * <br>
 *
 * @author amol<br>
 *
 */

public class MutableDouble
{
  public double value;
  /**
   * Added default constructor for deserializer
   */
  public MutableDouble()
  {
    value = 0;
  }

  /**
   * Sets value to i
   * @param i
   */
  public MutableDouble(double i)
  {
    value = i;
  }

  /**
   * Increaments value by i
   * @param i
   */
  public void add(double i) {
    value += i;
  }
}
