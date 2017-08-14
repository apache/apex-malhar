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

import org.apache.apex.malhar.lib.db.AbstractAggregateTransactionableKeyValueStoreOutputOperator;

/**
 * This is the base implementation of a Redis aggregate output operator.
 * <p></p>
 * @displayName Abstract Redis Aggregate Output
 * @category Output
 * @tags redis, key value, aggregate
 *
 * @param <T> The tuple type.
 * @since 0.9.3
 */
public abstract class AbstractRedisAggregateOutputOperator<T>
    extends AbstractAggregateTransactionableKeyValueStoreOutputOperator<T, RedisStore>
{
  public AbstractRedisAggregateOutputOperator()
  {
    store = new RedisStore();
  }

}
