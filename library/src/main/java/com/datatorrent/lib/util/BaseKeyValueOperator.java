/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.lib.codec.JavaSerializationStreamCodec;

import com.datatorrent.api.StreamCodec;

/**
 * This is an abstract operator that allows cloneKey and cloneValue to allow users to use mutable objects.
 * <p></p>
 * @displayName Base Key Value
 * @category Algorithmic
 * @tags key value abstract
 * @since 0.3.2
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
  public KeyValPair<K, V> cloneKeyValPair(KeyValPair<K, V> kv)
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
    for (Map.Entry<K, V> e : tuple.entrySet()) {
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
  public StreamCodec<KeyValPair<K, V>> getKeyValPairStreamCodec()
  {
    return new DefaultPartitionCodec<K, V>();
  }

  public static class DefaultPartitionCodec<K, V> extends JavaSerializationStreamCodec<KeyValPair<K, V>> implements Serializable
  {
    /**
     * A codec to enable partitioning to be done by key
     */
    @Override
    public int getPartition(KeyValPair<K, V> o)
    {
      return o.getKey().hashCode();
    }

    private static final long serialVersionUID = 201411031350L;
  }

}
