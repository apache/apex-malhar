/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

/**
 *
 * A mutable long for basic operations. Better performance<p>
 * <br>
 *
 * @author amol<br>
 *
 */

public class MutableLong
{
  public long value;

  /**
   * Added default constructor for deserializer
   */
  public MutableLong()
  {
    value = 0;
  }

  /**
   * Constructs and sets value to i
   * @param i
   */
  public MutableLong(long i)
  {
    value = i;
  }

  /**
   * Increments by i
   * @param i
   */
  public void add(long i) {
    value += i;
  }

  /**
   * Increments by 1
   */
  public void increment() {
    value++;
  }
}
