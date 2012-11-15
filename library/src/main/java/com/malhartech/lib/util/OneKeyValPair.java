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
   * @param value sets value
   */
  public void setValue (V value)
  {
    this.value = value;
  }

  /**
   * Setter function for key
   * @param key sets key
   */
  public void setKey(K key)
  {
    this.key = key;
  }

  /**
   * getter function for value
   * @return value
   */
  public V getValue()
  {
    return value;
  }

  /**
   * getter function for key
   * @return key
   */
  public K getKey()
  {
    return key;
  }

  /**
   * Constructor
   * @param k sets key
   * @param v sets value
   */
  public OneKeyValPair(K k, V v)
  {
    key = k;
    value = v;
  }
}
