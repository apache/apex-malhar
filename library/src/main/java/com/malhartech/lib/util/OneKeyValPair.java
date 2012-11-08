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


  public void setValue (V i)
  {
    value = i;
  }

  public void setKey(K i)
  {
    key = i;
  }

  public V getValue()
  {
    return value;
  }

  public K getKey()
  {
    return key;
  }
  public OneKeyValPair(K k, V v)
  {
    key = k;
    value = v;
  }
}
