/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import java.util.Comparator;

/**
 *
 * A mutable integer for basic operations. Makes things easy for incrementing etc.<p>
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

  public MutableInteger(int i)
  {
    value = i;
  }

  public void add(int i) {
    value += i;
  }

  public void increment() {
    value++;
  }
}
