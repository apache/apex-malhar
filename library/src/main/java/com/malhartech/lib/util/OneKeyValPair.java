/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

/**
 *
 * A single KeyValPair for basic data passing, It is a write once, and read often model<p>
 * <br>
 * Key and Value are to be treated as immutable objects.
 *
 * @author amol<br>
 *
 */

public class OneKeyValPair<K,V>
{
  K key;
  V value;
  /**
   * Added default constructor for deserializer
   */
  public OneKeyValPair()
  {
  }


  /**
   * Setter function for value
   * @param i
   */
  public void setValue (V i)
  {
    value = i;
  }

  /**
   * Setter function for key
   * @param i
   */
  public void setKey(K i)
  {
    key = i;
  }

  /**
   * getter function for value
   * @return
   */
  public V getValue()
  {
    return value;
  }

  /**
   * getter function for key
   * @return
   */
  public K getKey()
  {
    return key;
  }

  /**
   * Constructor
   * @param k
   * @param v
   */
  public OneKeyValPair(K k, V v)
  {
    key = k;
    value = v;
  }
}
