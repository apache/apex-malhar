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
package com.datatorrent.demos.dimensions.generic;

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
    EventSchema eventSchema = GenericAggregateSerializerTest.getEventSchema();
    GenericAggregator aggregator = new GenericAggregator(eventSchema);
    aggregator.init("time=DAYS:pubId:adUnit:adId");
    /* prepare a object */
    Map<String, Object> event = Maps.newHashMap();
    event.put("timestamp", System.currentTimeMillis());
    event.put("pubId", 1);
    event.put("adUnit", 2);
    event.put("adId", 3);
    event.put("clicks", 10L);

    GenericEvent ge = eventSchema.convertMapToGenericEvent(event);
    GenericAggregate aggr = new GenericAggregate(ge);

    /* prepare a object */
    Map<String, Object> event2 = Maps.newHashMap();
    event2.put("timestamp", System.currentTimeMillis());
    event2.put("pubId", 1);
    event2.put("adUnit", 2);
    event2.put("adId", 3);
    event2.put("clicks", 20L);

    GenericEvent ge2 = eventSchema.convertMapToGenericEvent(event2);
    aggregator.aggregate(aggr, ge2);

    Assert.assertEquals("sum is 30", 30L, eventSchema.getValue(aggr, "clicks"));
  }

  @Test
  public void test1() {
    EventSchema eventSchema = GenericAggregateSerializerTest.getEventSchema();
    GenericAggregator aggregator = new GenericAggregator(eventSchema);
    aggregator.init("time=DAYS:pubId:adUnit");

    /* prepare a object */
    Map<String, Object> event = Maps.newHashMap();
    event.put("timestamp", System.currentTimeMillis());
    event.put("pubId", 1);
    event.put("adUnit", 2);
    event.put("adId", 3);
    event.put("clicks", 10L);

    GenericEvent ge = eventSchema.convertMapToGenericEvent(event);
    GenericAggregate aggr = new GenericAggregate(ge);

    /* prepare a object */
    Map<String, Object> event2 = Maps.newHashMap();
    event2.put("timestamp", System.currentTimeMillis());
    event2.put("pubId", 1);
    event2.put("adUnit", 2);
    event.put("adId", 5);
    event2.put("clicks", 20L);

    GenericEvent ge2 = eventSchema.convertMapToGenericEvent(event2);
    aggregator.aggregate(aggr, ge2);

    Assert.assertEquals("sum is 30", 30L, eventSchema.getValue(aggr, "clicks"));
  }


  @Test
  public void test2() {
    EventSchema eventSchema = GenericAggregateSerializerTest.getEventSchema();
    GenericAggregator aggregator = new GenericAggregator(eventSchema);
    aggregator.init("time=MINUTES:pubId:adUnit");

    /* prepare a object */
    Map<String, Object> event = Maps.newHashMap();
    event.put("timestamp", System.currentTimeMillis());
    event.put("pubId", 1);
    event.put("adUnit", 2);
    event.put("adId", 3);
    event.put("clicks", new Long(10));

    GenericEvent ge = eventSchema.convertMapToGenericEvent(event);
    GenericAggregate aggr = new GenericAggregate(ge);

    /* prepare a object */
    Map<String, Object> event2 = Maps.newHashMap();
    event2.put("timestamp", System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1));
    event2.put("pubId", 1);
    event2.put("adUnit", 2);
    event2.put("adId", 5);
    event2.put("clicks", 20L);

    GenericEvent ge2 = eventSchema.convertMapToGenericEvent(event2);
    aggregator.aggregate(aggr, ge2);

    Assert.assertEquals("sum is 30", 30L, eventSchema.getValue(aggr, "clicks"));
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
    event.put("clicks", 20L);

    ObjectMapper mapper = new ObjectMapper();
    // TODO: replace with assertion
    System.out.println(mapper.writeValueAsString(event));

    String eventStr = "{\"timestamp\":1410789704559,\"adId\":5,\"pubId\":1,\"clicks\":20,\"adUnit\":2}";
    // TODO: replace with assertion
    System.out.println(mapper.readValue(eventStr, Map.class));
  }


  @Test
  public void testArrayAggregator() {
    GenericAggregator aggregator = new GenericAggregator(GenericAggregateSerializerTest.getEventSchema());
    aggregator.init("time=DAYS:pubId:adUnit:adId");
    /* prepare a object */
    GenericEvent event = new GenericEvent();
    event.timestamp = System.currentTimeMillis();
    Object[] keys = new Object[3];
    keys[0] = 1;
    keys[1] = 2;
    keys[2] = 3;
    event.keys = keys;

    Object[] fields = new Object[1];
    fields[0] = 10L;
    event.values = fields;

    GenericAggregate aggr = aggregator.getGroup(event, 0);
    aggregator.aggregate(aggr, event);

    GenericEvent event2 = new GenericEvent();
    event.timestamp = System.currentTimeMillis();
    keys = new Object[3];
    keys[0] = 1;
    keys[1] = 2;
    keys[2] = 3;
    event2.keys = keys;

    fields = new Object[1];
    fields[0] = 20L;
    event2.values = fields;


    aggregator.aggregate(aggr, event2);

    Assert.assertEquals("sum is 30", 30L, aggr.aggregates[0]);
  }

  @Test
  public void testArrayAggregator2() {
    GenericAggregator aggregator = new GenericAggregator(GenericAggregateSerializerTest.getEventSchema());
    aggregator.init("time=DAYS:pubId:adUnit");
    /* prepare a object */
    GenericEvent event = new GenericEvent();
    event.timestamp = System.currentTimeMillis();
    Object[] keys = new Object[3];
    keys[0] = 1;
    keys[1] = 2;
    keys[2] = 3;
    event.keys = keys;

    Object[] fields = new Object[1];
    fields[0] = 10L;
    event.values = fields;

    GenericAggregate aggr = aggregator.getGroup(event, 0);
    aggregator.aggregate(aggr, event);

    GenericEvent event2 = new GenericEvent();
    event.timestamp = System.currentTimeMillis();
    keys = new Object[3];
    keys[0] = 1;
    keys[1] = 2;
    keys[2] = 3;
    event2.keys = keys;

    fields = new Object[1];
    fields[0] = 20L;
    event2.values = fields;


    aggregator.aggregate(aggr, event2);

    Assert.assertEquals("sum is 30", 30L, aggr.aggregates[0]);
  }

  @Test
  public void testArrayAggregator3() {
    EventSchema eventSchema = GenericAggregateSerializerTest.getEventSchema();
    GenericAggregator aggregator = new GenericAggregator(eventSchema);
    aggregator.init("time=DAYS:pubId:adUnit:adId");
    /* prepare a object */
    Map<String, Object> event = Maps.newHashMap();
    event.put("timestamp", System.currentTimeMillis());
    event.put("pubId", 1);
    event.put("adUnit", 2);
    event.put("adId", 3);
    event.put("clicks", 10L);
    GenericEvent ae = eventSchema.convertMapToGenericEvent(event);

    GenericAggregate aggr = aggregator.getGroup(ae, 0);
    aggregator.aggregate(aggr, ae);

    /* prepare a object */
    Map<String, Object> event2 = Maps.newHashMap();
    event2.put("timestamp", System.currentTimeMillis());
    event2.put("pubId", 1);
    event2.put("adUnit", 2);
    event2.put("adId", 3);
    event2.put("clicks", 20L);
    GenericEvent ae2 = eventSchema.convertMapToGenericEvent(event2);

    aggregator.aggregate(aggr, ae2);

    Assert.assertEquals("sum is 30", 30L, aggr.aggregates[0]);
  }

  @Test
  public void testArrayAggregator4() {
    EventSchema eventSchema = GenericAggregateSerializerTest.getEventSchema();
    GenericAggregator aggregator = new GenericAggregator(eventSchema);
    aggregator.init("time=DAYS:pubId:adUnit");
    /* prepare a object */
    Map<String, Object> event = Maps.newHashMap();
    event.put("timestamp", System.currentTimeMillis());
    event.put("pubId", 1);
    event.put("adUnit", 2);
    event.put("adId", 3);
    event.put("clicks", 10L);
    GenericEvent ae = eventSchema.convertMapToGenericEvent(event);

    GenericAggregate aggr = aggregator.getGroup(ae, 0);
    aggregator.aggregate(aggr, ae);

    // odering is pubId, adId, adUnit
    Assert.assertEquals("Aggregator 0", 1, aggr.keys[0]);
    Assert.assertEquals("Aggregator 1 " , 0, aggr.keys[1]);
    Assert.assertEquals("Aggregator 2 ", 2, aggr.keys[2]);

    /* prepare a object */
    Map<String, Object> event2 = Maps.newHashMap();
    event2.put("timestamp", System.currentTimeMillis());
    event2.put("pubId", 1);
    event2.put("adUnit", 2);
    event2.put("adId", 3);
    event2.put("clicks", 20L);
    GenericEvent ae2 = eventSchema.convertMapToGenericEvent(event2);

    aggregator.aggregate(aggr, ae2);

    Assert.assertEquals("sum is 30", 30L, aggr.aggregates[0]);
  }


  @Test
  public void dimensionComputationTest()
  {
    SchemaConverter converter = new SchemaConverter();
    converter.setEventSchemaJSON(GenericAggregateSerializerTest.TEST_SCHEMA_JSON);
    GenericDimensionComputation dimensions = new GenericDimensionComputation();
    dimensions.setSchema(converter.getEventSchema());
    dimensions.setup(null);

    for(int i = 0; i < 10; i++) {
      Map<String, Object> event = Maps.newHashMap();
      event.put("timestamp", System.currentTimeMillis());
      event.put("pubId", 1);
      event.put("adUnit", 2);
      event.put("adId", 3);
      event.put("clicks", 10L);

      dimensions.data.process(converter.getEventSchema().convertMapToGenericEvent(event));
    }
    System.out.println("Something needs to be done");
  }


}
