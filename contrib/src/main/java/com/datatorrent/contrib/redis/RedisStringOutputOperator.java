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
 * <p>
 * RedisStringOutputOperator class.
 * This class is used to optimized for writing Map<String,String> to Redis in a single set call instead of different set call for each of the keys in map 
 * </p>
 *
 * @since 0.3.2
 */
public class RedisStringOutputOperator<K> extends RedisOutputOperator<K,Map<String,String>>
{

  @Override
  public void store(Map<K, Object> t)
  {
    for (Map.Entry<K, Object> entry : t.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof Map) {        
        redisConnection.hmset(entry.getKey().toString(), (Map) value);
      } 
      if (keyExpiryTime != -1) {
        redisConnection.expire(entry.getKey().toString(), keyExpiryTime);
      }
    }
  }
}
