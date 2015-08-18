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

import java.util.Map;

import com.datatorrent.lib.util.KeyValPair;

/**
 * This is the an implementation of a Redis input operator It takes in keys to
 * fetch and emits Values stored as Maps in Redis i.e. when value datatype in
 * Redis is HashMap 
 * 
 * @displayName Redis Input Operator for Map
 * @category Store
 * @tags input operator, key value
 *
 */

public class RedisMapAsValueInputOperator extends AbstractRedisInputOperator<KeyValPair<String, Map<String, String>>>
{
  @Override
  public void processTuples()
  {
    for (String key : keys) {
      if (store.getType(key).equals("hash")) {
        Map<String, String> mapValue = store.getMap(key);
        outputPort.emit(new KeyValPair<String, Map<String, String>>(key, mapValue));
      }
    }
    keys.clear();
  }

  @Override
  public KeyValPair<String, Map<String, String>> convertToTuple(Map<Object, Object> o)
  {
    // Do nothing for the override, Emit already handled in processTuples
    return null;
  }
}
