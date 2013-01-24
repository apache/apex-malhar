/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.engine.DefaultStreamCodec;
import java.util.AbstractMap;

/**
 *
 * A single KeyValPair for basic data passing, It is a write once, and read often model<p>
 * <br>
 * Key and Value are to be treated as immutable objects.
 *
 * @author amol<br>
 *
 */
public class KeyValPair<K, V> extends AbstractMap.SimpleEntry<K, V>
{
  /**
   * Added default constructor for deserializer
   */
  private KeyValPair()
  {
    super(null, null);
  }

  /**
   * Constructor
   *
   * @param k sets key
   * @param v sets value
   */
  public KeyValPair(K k, V v)
  {
    super(k, v);
  }

  public class Codec extends DefaultStreamCodec<KeyValPair<K, V>>
  {
    /**
     * A codec to enable partitioning to be done by key
     */
    @Override
    public int getPartition(KeyValPair<K, V> o)
    {
      return o.getKey().hashCode();
    }
  }
}
