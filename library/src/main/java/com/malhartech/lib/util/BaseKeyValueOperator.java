/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.codec.DefaultPartitionCodec;
import com.malhartech.common.KeyValPair;
import com.malhartech.api.StreamCodec;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for operators that allows cloneValue and cloneKey for enabling users to use mutable objects<p>
 *
 * @author amol<br>
 *
 */
public class BaseKeyValueOperator<K, V> extends BaseKeyOperator<K>
{
  /**
   * By default an immutable object is assumed. Override if V is mutable
   *
   * @param v
   * @return v as is
   */
  public V cloneValue(V v)
  {
    return v;
  }

  /**
   * By default an immutable object is assumed. Override if V is mutable
   *
   * @param kv
   * @return v as is
   */
  public KeyValPair<K, V> cloneKeyValPair(KeyValPair<K,V> kv)
  {
    return kv;
  }

  /**
   * Creates a HashMap&lt;K,V&gt; by using cloneKey(K) and cloneValue(V)
   *
   * @param tuple to be cloned
   * @return HashMap&lt;K,V&gt;
   */
  public HashMap<K, V> cloneTuple(Map<K, V> tuple)
  {
    if (tuple == null) {
      return null;
    }
    HashMap<K, V> ret = new HashMap<K, V>(tuple.size());
    for (Map.Entry<K, V> e: tuple.entrySet()) {
      ret.put(cloneKey(e.getKey()), cloneValue(e.getValue()));
    }
    return ret;
  }

  /**
   * Creates a HashMap<K,V>(1) by using cloneKey(K) and cloneValue(V)
   *
   * @param key key to be cloned
   * @param val value to be cloned
   * @return HashMap<K,V>(1)
   */
  public HashMap<K, V> cloneTuple(K key, V val)
  {
    HashMap<K, V> ret = new HashMap<K, V>(1);
    ret.put(cloneKey(key), cloneValue(val));
    return ret;
  }

  /**
   * A codec to enable partitioning to be done by key
   */
  public Class<? extends StreamCodec<KeyValPair<K, V>>> getKeyValPairStreamCodec()
  {
    Class c = DefaultPartitionCodec.class;
    return (Class<? extends StreamCodec<KeyValPair<K, V>>>)c;
  }
}
