/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.redis;

import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.lib.util.KeyValPair;

/**
 * This class provides an output adapter that takes a key value pair.&nbsp;
 * If the values in the key value pairs are maps, the operator will increment the values in the key value pair in redis.&nbsp;
 * Otherwise, it will increment the value as is.
 * <p></p>
 *
 * @displayName Redis Number Summation Key Val Pair Output
 * @category Output
 * @tags redis, key value
 *
 * @param <K> The key type
 * @param <V> The value type
 * @since 0.3.2
 */
public class RedisNumberSummationKeyValPairOutputOperator<K, V> extends AbstractRedisAggregateOutputOperator<KeyValPair<K, V>>
{
  private Map<Object, Object> dataMap = new HashMap<Object, Object>();
  private transient NumberSummation<K, V> numberAggregation = new NumberSummation<K, V>(this, dataMap);

  @Override
  public void processTuple(KeyValPair<K, V> t)
  {
    numberAggregation.process(t.getKey(), t.getValue());
  }

  @Override
  public void storeAggregate()
  {
    numberAggregation.storeAggregate();
  }

}
