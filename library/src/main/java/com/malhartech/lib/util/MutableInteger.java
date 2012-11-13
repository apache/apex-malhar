/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

/**
 *
 * A mutable integer for basic operations. Better performance<p>
 * <br>
 *
 * @author amol<br>
 *
 */

public class MutableInteger
{
  public int value;

  /**
   * Added default constructor for deserializer
   */
  public MutableInteger()
  {
    value = 0;
  }

  /**
   * Constructs and sets value to i
   * @param i
   */
  public MutableInteger(int i)
  {
    value = i;
  }

  /**
   * Increments by i
   * @param i
   */
  public void add(int i) {
    value += i;
  }

  /**
   * Increments by 1
   */
  public void increment() {
    value++;
  }
}
