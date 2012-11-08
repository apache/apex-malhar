/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

/**
 *
 * A mutable double for basic operations. Less memory needs for adding<p>
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

  public MutableDouble(double i)
  {
    value = i;
  }

  public void add(double i) {
    value += i;
  }
}
