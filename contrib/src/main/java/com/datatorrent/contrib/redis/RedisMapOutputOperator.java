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

import java.util.Map;

/**
 * <p>RedisMapOutputOperator class.</p>
 *
 * This output adapter takes maps as tuples and just writes to the redis store with the keys and the values in the map
 *
 * @param <K> The key type.
 * @param <V> The value type.
 * @since 0.3.2
 */
public class RedisMapOutputOperator<K, V> extends AbstractRedisPassThruOutputOperator<Map<K, V>>
{
  @Override
  public void processTuple(Map<K, V> t)
  {
    for (Map.Entry<K, V> entry : t.entrySet()) {
      store.put(entry.getKey(), entry.getValue());
    }
  }

}
