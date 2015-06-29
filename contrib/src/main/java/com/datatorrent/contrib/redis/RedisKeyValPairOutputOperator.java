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
package com.datatorrent.contrib.redis;

import com.datatorrent.lib.util.KeyValPair;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a Redis output operator, which takes key value pairs and writes them out to Redis.
 * <p>
 * This output adapter takes key value pairs as tuples and just writes to the redis store with the keys and the values in the key value pair
 * Note: Redis output operator should never use the passthrough method because it begins a transaction at beginWindow and commits a transaction at
 * endWindow, and a transaction in Redis blocks all other clients.
 * </p>
 *
 * @displayName Redis Key Val Pair Output
 * @category Store
 * @tags output operator, key value
 *
 * @param <K> The key type.
 * @param <V> The value type.
 * @since 0.3.2
 */
public class RedisKeyValPairOutputOperator<K, V> extends AbstractRedisAggregateOutputOperator<KeyValPair<K, V>>
{
  protected final Map<Object, Object> map = new HashMap<Object, Object>();

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    map.clear();
  }

  @Override
  public void processTuple(KeyValPair<K, V> t)
  {
    map.put(t.getKey(), t.getValue());
  }

  @Override
  public void storeAggregate()
  {
    // RedisStore.putAll does not work for hash values
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      store.put(entry.getKey(), entry.getValue());
    }
  }

}
