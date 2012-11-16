/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

/**
 *
 * A mutable short for basic operations. Better performance<p>
 * <br>
 *
 * @author amol<br>
 *
 */

public class MutableShort
{
  public short value;

  /**
   * Added default constructor for deserializer
   */
  public MutableShort()
  {
    value = 0;
  }

  /**
   * Constructs and sets value to i
   * @param i
   */
  public MutableShort(short i)
  {
    value = i;
  }

  /**
   * Increments by i
   * @param i
   */
  public void add(short i) {
    value += i;
  }

  /**
   * Increments by 1
   */
  public void increment() {
    value++;
  }
}
