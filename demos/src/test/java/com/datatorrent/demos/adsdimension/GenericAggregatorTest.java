/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.adsdimension;

import com.google.common.collect.Maps;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class GenericAggregatorTest
{
  @Test
  public void test() {
    MapAggregator aggregator = new MapAggregator(GenericEventSerializerTest.getDataDesc());
    aggregator.init("time=DAYS:pubId:adUnit:adId");
    /* prepare a object */
    Map<String, Object> event = Maps.newHashMap();
    event.put("timestamp", System.currentTimeMillis());
    event.put("pubId", 1);
    event.put("adUnit", 2);
    event.put("adId", 3);
    event.put("clicks", new Long(10));

    MapAggregate aggr = aggregator.getGroup(event, 0);
    aggregator.aggregate(aggr, event);

    /* prepare a object */
    Map<String, Object> event2 = Maps.newHashMap();
    event2.put("timestamp", System.currentTimeMillis());
    event2.put("pubId", 1);
    event2.put("adUnit", 2);
    event2.put("adId", 3);
    event2.put("clicks", new Long(20));

    aggregator.aggregate(aggr, event2);

    Assert.assertEquals("sum is 30", aggr.fields.get("clicks"), 30L);
  }

  @Test
  public void test1() {
    MapAggregator aggregator = new MapAggregator(GenericEventSerializerTest.getDataDesc());
    aggregator.init("time=DAYS:pubId:adUnit");

    /* prepare a object */
    Map<String, Object> event = Maps.newHashMap();
    event.put("timestamp", System.currentTimeMillis());
    event.put("pubId", 1);
    event.put("adUnit", 2);
    event.put("adId", 3);
    event.put("clicks", new Long(10));

    MapAggregate aggr = aggregator.getGroup(event, 0);
    aggregator.aggregate(aggr, event);

    /* prepare a object */
    Map<String, Object> event2 = Maps.newHashMap();
    event2.put("timestamp", System.currentTimeMillis());
    event2.put("pubId", 1);
    event2.put("adUnit", 2);
    event.put("adId", 5);
    event2.put("clicks", new Long(20));

    aggregator.aggregate(aggr, event2);

    Assert.assertEquals("sum is 30", aggr.fields.get("clicks"), 30L);
  }


  @Test
  public void test2() {
    MapAggregator aggregator = new MapAggregator(GenericEventSerializerTest.getDataDesc());
    aggregator.init("time=MINUTES:pubId:adUnit");

    long baseTime = System.currentTimeMillis();
    long baseMinute = TimeUnit.MILLISECONDS.convert(TimeUnit.MINUTES.convert(baseTime, TimeUnit.MILLISECONDS), TimeUnit.MINUTES);

    /* prepare a object */
    Map<String, Object> event = Maps.newHashMap();
    event.put("timestamp", System.currentTimeMillis());
    event.put("pubId", 1);
    event.put("adUnit", 2);
    event.put("adId", 3);
    event.put("clicks", new Long(10));

    MapAggregate aggr = aggregator.getGroup(event, 0);
    aggregator.aggregate(aggr, event);

    /* prepare a object */
    Map<String, Object> event2 = Maps.newHashMap();
    event2.put("timestamp", System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1));
    event2.put("pubId", 1);
    event2.put("adUnit", 2);
    event2.put("adId", 5);
    event2.put("clicks", new Long(20));

    aggregator.aggregate(aggr, event2);

    Assert.assertEquals("sum is 30", aggr.fields.get("clicks"), 30L);
  }

  @Test
  public void mapTest() throws IOException
  {
    /* prepare a object */
    Map<String, Object> event = Maps.newHashMap();
    event.put("timestamp", System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1));
    event.put("pubId", 1);
    event.put("adUnit", 2);
    event.put("adId", 5);
    event.put("clicks", new Long(20));

    ObjectMapper mapper = new ObjectMapper();
    System.out.println(mapper.writeValueAsString(event));

    String eventStr = "{\"timestamp\":1410789704559,\"adId\":5,\"pubId\":1,\"clicks\":20,\"adUnit\":2}";
    Map<String, Object> event1 = mapper.readValue(eventStr, Map.class);
    System.out.println(event1);
  }

}
