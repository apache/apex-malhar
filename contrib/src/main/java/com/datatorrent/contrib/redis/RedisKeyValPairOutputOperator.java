/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.redis;

import com.datatorrent.lib.util.KeyValPair;

/**
 * <p>RedisMapOutputOperator class.</p>
 *
 * This output adapter takes key value pairs as tuples and just writes to the redis store with the keys and the values in the key value pair
 *
 * @param <K> The key type.
 * @param <V> The value type.
 * @since 0.3.2
 */
public class RedisKeyValPairOutputOperator<K, V> extends AbstractRedisPassThruOutputOperator<KeyValPair<K, V>>
{
  @Override
  public void processTuple(KeyValPair<K, V> t)
  {
    store.put(t.getKey(), t.getValue());
  }

}
