/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

/**
 *
 * A mutable float for basic operations. Better performance<p>
 * <br>
 *
 * @author amol<br>
 *
 */

public class MutableFloat
{
  public float value;
  /**
   * Added default constructor for deserializer
   */
  public MutableFloat()
  {
    value = 0;
  }

  /**
   * Sets value to i
   * @param i
   */
  public MutableFloat(float i)
  {
    value = i;
  }

  /**
   * Increaments value by i
   * @param i
   */
  public void add(float i) {
    value += i;
  }
}
