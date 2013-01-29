/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

/**
 *
 * A single KeyValPair for basic data passing, It is a write once, and read often model. <p>
 * <br>
 * Key and Value are to be treated as immutable objects.
 *
 * @param <K>
 * @param <V>
 * @author amol<br>
 *
 */
public class HighLow<V extends Number>
{
  V high;
  V low;

  /**
   * Added default constructor for deserializer.
   */
  public HighLow()
  {
    high = null;
    low = null;
  }

  /**
   * Constructor
   */
  public HighLow(V h, V l)
  {
    high = h;
    low = l;
  }

  /**
   * @return high value
   */
  public V getHigh()
  {
    return high;
  }

  /**
   *
   * @return low value
   */
  public V getLow()
  {
    return low;
  }

  /**
   * @param h sets high value
   */
  public void setHigh(V h)
  {
    high = h;
  }

  /**
   *
   * @param l sets low value
   */
  public void setLow(V l)
  {
    low = l;
  }
}
