/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.datatorrent.contrib.adsdimension;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import redis.clients.jedis.Jedis;

import com.datatorrent.api.annotation.ShipContainingJars;

import com.datatorrent.contrib.redis.RedisStore;
import com.datatorrent.lib.db.AbstractPassThruTransactionableStoreOutputOperator;
import com.datatorrent.lib.db.AbstractTransactionableStoreOutputOperator;

/**
 * An {@link AbstractTransactionableStoreOutputOperator} that persists aggregated dimensions in the Redis store.<br/>
 */
@ShipContainingJars(classes = {DateTimeFormatter.class, DateTimeFormat.class, Jedis.class})
public class RedisAggregateOutputOperator extends AbstractPassThruTransactionableStoreOutputOperator<AdInfo, RedisStore>
{
  public RedisAggregateOutputOperator()
  {
    store = new RedisStore();
  }

  @Override
  public void processTuple(AdInfo event)
  {
    StringBuilder keyBuilder = new StringBuilder(32);
    keyBuilder.append(formatter.print(event.timestamp));
    if (event.publisherId != 0) {
      keyBuilder.append("|0:").append(event.publisherId);
    }
    if (event.advertiserId != 0) {
      keyBuilder.append("|1:").append(event.advertiserId);
    }
    if (event.adUnit != 0) {
      keyBuilder.append("|2:").append(event.adUnit);
    }

    String key = keyBuilder.toString();
    store.hincrByFloat(key, "0", event.impressions + event.clicks);
    store.hincrByFloat(key, "1", event.cost);
    store.hincrByFloat(key, "2", event.revenue);
    store.hincrByFloat(key, "3", event.impressions);
    store.hincrByFloat(key, "4", event.clicks);
  }

  public static final DateTimeFormatter formatter = DateTimeFormat.forPattern("'m|'yyyyMMddHHmm");
}
