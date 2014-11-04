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

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;

import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.contrib.hds.tfile.TFileImpl;
import com.datatorrent.demos.dimensions.generic.DimensionStoreOperator.HDSRangeQueryResult;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.google.common.util.concurrent.MoreExecutors;

public class DimensionStoreOperatorTest
{
  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  @Test
  public void testQueryFromHDS() throws Exception {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    DimensionStoreOperator hdsOut = new DimensionStoreOperator() {
      @Override
      public void setup(OperatorContext arg0)
      {
        super.setup(arg0);
        super.writeExecutor = super.queryExecutor = MoreExecutors.sameThreadExecutor(); // synchronous processing
      }
    };
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsOut.setFileStore(hdsFile);
    hdsFile.setBasePath(testInfo.getDir());
    EventSchema eventSchema = GenericAggregateSerializerTest.getEventSchema();
    GenericAggregator aggregator = new GenericAggregator(eventSchema);
    aggregator.init("time=MINUTES:pubId:adId:adUnit");
    hdsOut.setEventSchemaJSON(GenericAggregateSerializerTest.TEST_SCHEMA_JSON);
    hdsOut.setAggregator(aggregator);
    hdsOut.setMaxCacheSize(1);
    hdsOut.setFlushIntervalCount(0);
    hdsOut.setup(null);

    CollectorTestSink<DimensionStoreOperator.HDSRangeQueryResult> queryResults = new CollectorTestSink<DimensionStoreOperator.HDSRangeQueryResult>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink) queryResults;
    hdsOut.queryResult.setSink(tmp);

    hdsOut.beginWindow(1);

    long baseTime = System.currentTimeMillis();
    long baseMinute = TimeUnit.MILLISECONDS.convert(TimeUnit.MINUTES.convert(baseTime, TimeUnit.MILLISECONDS), TimeUnit.MINUTES);

    // Events ae1 and ae2 fall into same aggregation as they have same key
    Map<String, Object> eventMap = Maps.newHashMap();
    eventMap.put("timestamp", baseMinute);
    eventMap.put("pubId", 1);
    eventMap.put("adId", 2);
    eventMap.put("adUnit", 3);
    eventMap.put("clicks", 10L);

    GenericAggregate ae1 = new GenericAggregate(eventSchema.convertMapToGenericEvent(eventMap));
    hdsOut.input.process(ae1);

    // Modify click count and create new event
    eventMap.put("clicks", 20L);
    GenericAggregate ae2 = new GenericAggregate(eventSchema.convertMapToGenericEvent(eventMap));
    hdsOut.input.process(ae2);

    // Modify clicks to 10 and time by 1 minute and create new event
    eventMap.put("clicks", 10L);
    eventMap.put("timestamp", baseMinute + TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
    GenericAggregate ae3 = new GenericAggregate(eventSchema.convertMapToGenericEvent(eventMap));
    hdsOut.input.process(ae3);

    hdsOut.endWindow();

    hdsOut.beginWindow(2);

    JSONObject keys = new JSONObject();
    keys.put("pubId", 1);
    keys.put("adId", 2);
    keys.put("adUnit", 3);

    JSONObject query = new JSONObject();
    query.put("numResults", "20");
    query.put("keys", keys);
    query.put("id", "query1");
    query.put("startTime", baseMinute);
    query.put("endTime", baseMinute + TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES));

    hdsOut.query.process(query.toString());

    Assert.assertEquals("timeSeriesQueries " + hdsOut.rangeQueries, 1, hdsOut.rangeQueries.size());
    DimensionStoreOperator.HDSRangeQuery aq = hdsOut.rangeQueries.values().iterator().next();
    Assert.assertEquals("numTimeUnits " + hdsOut.rangeQueries, baseMinute, aq.startTime);

    hdsOut.endWindow();

    Assert.assertEquals("queryResults " + queryResults.collectedTuples, 1, queryResults.collectedTuples.size());
    HDSRangeQueryResult r = queryResults.collectedTuples.iterator().next();
    Assert.assertEquals("result points " + r, 2, r.data.size());

    // ae1 object is stored as referenced in cache, and when new tuple is aggregated,
    // the new values are updated in ae1 itself, causing following check to fail.
    //Assert.assertEquals("clicks", ae1.clicks + ae2.clicks, r.data.get(0).clicks);
    Assert.assertEquals("clicks", 30L, r.data.get(0).get("clicks"));
    Assert.assertEquals("clicks", eventSchema.getValue(ae3, "clicks"), r.data.get(1).get("clicks"));
  }


  @Test
  public void testQueryFromHDSWithSubsetKeys() throws Exception {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    DimensionStoreOperator hdsOut = new DimensionStoreOperator() {
      @Override
      public void setup(OperatorContext arg0)
      {
        super.setup(arg0);
        super.writeExecutor = super.queryExecutor = MoreExecutors.sameThreadExecutor(); // synchronous processing
      }
    };
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsOut.setFileStore(hdsFile);
    hdsFile.setBasePath(testInfo.getDir());
    EventSchema eventSchema = GenericAggregateSerializerTest.getEventSchema();
    GenericAggregator aggregator = new GenericAggregator(eventSchema);
    aggregator.init("time=MINUTES:pubId:adId:adUnit");
    hdsOut.setEventSchemaJSON(GenericAggregateSerializerTest.TEST_SCHEMA_JSON);
    hdsOut.setAggregator(aggregator);
    hdsOut.setMaxCacheSize(1);
    hdsOut.setFlushIntervalCount(0);
    hdsOut.setup(null);

    CollectorTestSink<DimensionStoreOperator.HDSRangeQueryResult> queryResults = new CollectorTestSink<DimensionStoreOperator.HDSRangeQueryResult>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink) queryResults;
    hdsOut.queryResult.setSink(tmp);

    hdsOut.beginWindow(1);

    long baseTime = System.currentTimeMillis();
    long baseMinute = TimeUnit.MILLISECONDS.convert(TimeUnit.MINUTES.convert(baseTime, TimeUnit.MILLISECONDS), TimeUnit.MINUTES);

    // Events ae1 and ae2 fall into same aggregation as they have same key
    Map<String, Object> eventMap = Maps.newHashMap();
    eventMap.put("timestamp", baseMinute);
    eventMap.put("pubId", 1);
    eventMap.put("adUnit", 3);
    eventMap.put("clicks", 10L);

    GenericAggregate ae1 = new GenericAggregate(eventSchema.convertMapToGenericEvent(eventMap));
    hdsOut.input.process(ae1);

    // Modify click count and create new event
    eventMap.put("clicks", 20L);
    GenericAggregate ae2 = new GenericAggregate(eventSchema.convertMapToGenericEvent(eventMap));
    hdsOut.input.process(ae2);

    // Modify clicks to 10 and time by 1 minute and create new event
    eventMap.put("timestamp", baseMinute + TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
    eventMap.put("clicks", 10L);
    GenericAggregate ae3 = new GenericAggregate(eventSchema.convertMapToGenericEvent(eventMap));
    hdsOut.input.process(ae3);

    hdsOut.endWindow();

    hdsOut.beginWindow(2);

    JSONObject keys = new JSONObject();
    keys.put("pubId", 1);
    keys.put("adUnit", 3);

    JSONObject query = new JSONObject();
    query.put("numResults", "20");
    query.put("keys", keys);
    query.put("id", "query1");
    query.put("startTime", baseMinute);
    query.put("endTime", baseMinute + TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES));

    hdsOut.query.process(query.toString());

    Assert.assertEquals("timeSeriesQueries " + hdsOut.rangeQueries, 1, hdsOut.rangeQueries.size());
    DimensionStoreOperator.HDSRangeQuery aq = hdsOut.rangeQueries.values().iterator().next();
    Assert.assertEquals("numTimeUnits " + hdsOut.rangeQueries, baseMinute, aq.startTime);

    hdsOut.endWindow();

    Assert.assertEquals("queryResults " + queryResults.collectedTuples, 1, queryResults.collectedTuples.size());
    HDSRangeQueryResult r = queryResults.collectedTuples.iterator().next();
    Assert.assertEquals("result points " + r, 2, r.data.size());

    // ae1 object is stored as referenced in cache, and when new tuple is aggregated,
    // the new values are updated in ae1 itself, causing following check to fail.
    //Assert.assertEquals("clicks", ae1.clicks + ae2.clicks, r.data.get(0).clicks);
    Assert.assertEquals("clicks", 30L, r.data.get(0).get("clicks"));
    Assert.assertEquals("clicks", eventSchema.getValue(ae3, "clicks"), r.data.get(1).get("clicks"));
    // when data is returned from HDS, all keys are part of response,
    // not present keys will have 0 values.
    Assert.assertEquals("from HDS", 0, r.data.get(0).get("adId"));
    // when data is returned from Cache, not specified keys will not
    // be present in the map.
    Assert.assertEquals("from cache", 0, r.data.get(1).get("adId"));
  }


  @Test
  public void testQueryLessKeys() throws Exception {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    DimensionStoreOperator hdsOut = new DimensionStoreOperator() {
      @Override
      public void setup(OperatorContext arg0)
      {
        super.setup(arg0);
        super.writeExecutor = super.queryExecutor = MoreExecutors.sameThreadExecutor(); // synchronous processing
      }
    };
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsOut.setFileStore(hdsFile);
    hdsFile.setBasePath(testInfo.getDir());
    EventSchema eventSchema = GenericAggregateSerializerTest.getEventSchema();
    GenericAggregator aggregator = new GenericAggregator(eventSchema);
    aggregator.init("time=MINUTES:pubId:adId:adUnit");
    hdsOut.setEventSchemaJSON(GenericAggregateSerializerTest.TEST_SCHEMA_JSON);
    hdsOut.setAggregator(aggregator);
    hdsOut.setMaxCacheSize(100);
    hdsOut.setFlushIntervalCount(100);
    hdsOut.setup(null);

    CollectorTestSink<DimensionStoreOperator.HDSRangeQueryResult> queryResults = new CollectorTestSink<DimensionStoreOperator.HDSRangeQueryResult>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink) queryResults;
    hdsOut.queryResult.setSink(tmp);

    hdsOut.beginWindow(1);

    long baseTime = System.currentTimeMillis();
    long baseMinute = TimeUnit.MILLISECONDS.convert(TimeUnit.MINUTES.convert(baseTime, TimeUnit.MILLISECONDS), TimeUnit.MINUTES);

    // Check aggregation for ae1 and ae2 as they have same key.
    Map<String, Object> eventMap = Maps.newHashMap();
    eventMap.put("timestamp", baseMinute);
    eventMap.put("pubId", 1);
    eventMap.put("adId", 2);
    eventMap.put("clicks", 10L);

    GenericAggregate ae1 = new GenericAggregate(eventSchema.convertMapToGenericEvent(eventMap));
    hdsOut.input.process(ae1);

    // Modify click count and create new event
    eventMap.put("clicks", 20L);
    GenericAggregate ae2 = new GenericAggregate(eventSchema.convertMapToGenericEvent(eventMap));
    hdsOut.input.process(ae2);

    // Modify clicks to 10 and time by 1 minute and create new event
    eventMap.put("timestamp", baseMinute + TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
    eventMap.put("clicks", 10L);
    GenericAggregate ae3 = new GenericAggregate(eventSchema.convertMapToGenericEvent(eventMap));
    hdsOut.input.process(ae3);


    hdsOut.endWindow();

    hdsOut.beginWindow(2);

    JSONObject keys = new JSONObject();
    keys.put("pubId", 1);
    keys.put("adId", 2);

    JSONObject query = new JSONObject();
    query.put("numResults", "20");
    query.put("keys", keys);
    query.put("id", "query1");
    query.put("startTime", baseMinute);
    query.put("endTime", baseMinute + TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES));

    hdsOut.query.process(query.toString());

    Assert.assertEquals("timeSeriesQueries " + hdsOut.rangeQueries, 1, hdsOut.rangeQueries.size());
    DimensionStoreOperator.HDSRangeQuery aq = hdsOut.rangeQueries.values().iterator().next();
    Assert.assertEquals("numTimeUnits " + hdsOut.rangeQueries, baseMinute, aq.startTime);

    hdsOut.endWindow();

    Assert.assertEquals("queryResults " + queryResults.collectedTuples, 1, queryResults.collectedTuples.size());
    HDSRangeQueryResult r = queryResults.collectedTuples.iterator().next();
    Assert.assertEquals("result points " + r, 2, r.data.size());

    // ae1 object is stored as referenced in cache, and when new tuple is aggregated,
    // the new values are updated in ae1 itself, causing following check to fail.
    //Assert.assertEquals("clicks", ae1.clicks + ae2.clicks, r.data.get(0).clicks);
    Assert.assertEquals("clicks", 30L, r.data.get(0).get("clicks"));
    Assert.assertEquals("clicks", eventSchema.getValue(ae3, "clicks"), r.data.get(1).get("clicks"));
  }


  /* Test if queries with different key combination works */
  @Test
  public void testQueryLessKeys1() throws Exception {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    EventSchema eventSchema = GenericAggregateSerializerTest.getEventSchema();

    String[] dimensionSpecs = new String[] {
        "time=" + TimeUnit.MINUTES,
        "time=" + TimeUnit.MINUTES + ":adUnit",
        "time=" + TimeUnit.MINUTES + ":adId",
        "time=" + TimeUnit.MINUTES + ":pubId",
        "time=" + TimeUnit.MINUTES + ":adId:adUnit",
        "time=" + TimeUnit.MINUTES + ":pubId:adUnit",
        "time=" + TimeUnit.MINUTES + ":pubId:adId",
        "time=" + TimeUnit.MINUTES + ":pubId:adId:adUnit"
    };

    GenericAggregator[] aggregators = new GenericAggregator[dimensionSpecs.length];
    for (int i = dimensionSpecs.length; i-- > 0;) {
      GenericAggregator aggregator = new GenericAggregator(eventSchema);
      aggregator.init(dimensionSpecs[i]);
      aggregators[i] = aggregator;
    }

    DimensionStoreOperator hdsOut = new DimensionStoreOperator() {
      @Override
      public void setup(OperatorContext arg0)
      {
        super.setup(arg0);
        super.writeExecutor = super.queryExecutor = MoreExecutors.sameThreadExecutor(); // synchronous processing
      }
    };
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsOut.setFileStore(hdsFile);
    hdsFile.setBasePath(testInfo.getDir());
    //GenericAggregator aggregator = new GenericAggregator(eventSchema);
    //aggregator.init("time=MINUTES:pubId:adId:adUnit");
    hdsOut.setEventSchemaJSON(GenericAggregateSerializerTest.TEST_SCHEMA_JSON);
    hdsOut.setAggregator(aggregators[0]);
    hdsOut.setMaxCacheSize(100);
    hdsOut.setFlushIntervalCount(100);
    hdsOut.setup(null);

    CollectorTestSink<DimensionStoreOperator.HDSRangeQueryResult> queryResults = new CollectorTestSink<DimensionStoreOperator.HDSRangeQueryResult>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink) queryResults;
    hdsOut.queryResult.setSink(tmp);


    long baseTime = System.currentTimeMillis();
    long baseMinute = TimeUnit.MILLISECONDS.convert(TimeUnit.MINUTES.convert(baseTime, TimeUnit.MILLISECONDS), TimeUnit.MINUTES);

    int numMinutes = 5;
    int wid = 1;
    for(int i = 0; i < numMinutes; i++) {
      hdsOut.beginWindow(wid);

      long timestamp = baseMinute - TimeUnit.MINUTES.toMillis(i);

      // Check aggregation for ae1 and ae2 as they have same key.
      Map<String, Object> eventMap = Maps.newHashMap();
      eventMap.put("timestamp", timestamp);
      eventMap.put("pubId", 1);
      eventMap.put("adId", 2);
      eventMap.put("adUnit", 3);
      eventMap.put("clicks", 10L);

      GenericEvent ge = eventSchema.convertMapToGenericEvent(eventMap);

      int aggrIdx = 0;
      for (GenericAggregator aggregator : aggregators) {
        GenericAggregate aggr = aggregator.getGroup(ge, aggrIdx);
        aggregator.aggregate(aggr, ge);
        hdsOut.input.process(aggr);
        aggrIdx++;
      }
      hdsOut.endWindow();
      wid++;
    }

    hdsOut.beginWindow(wid);

    int pubId = 1;
    int adId = 2;
    int adUnit = 3;

    for(int i = 0; i < 8; i++) {
      JSONObject keys = new JSONObject();
      if ((i & 0x1) != 0)
        keys.put("pubId", pubId);
      if ((i & 0x2) != 0)
        keys.put("adId", adId);
      if ((i & 0x4) != 0)
        keys.put("adUnit", adUnit);

      JSONObject query = new JSONObject();
      query.put("keys", keys);
      query.put("id", "query" + i);
      query.put("startTime", baseMinute - TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES));
      query.put("endTime", baseMinute);

      hdsOut.query.process(query.toString());
    }
    Assert.assertEquals("timeSeriesQueries " + hdsOut.rangeQueries, 8, hdsOut.rangeQueries.size());
    hdsOut.endWindow();

    Assert.assertEquals("queryResults " + queryResults.collectedTuples, 8, queryResults.collectedTuples.size());
    System.out.println("basetime " + baseMinute);

    for(HDSRangeQueryResult r : queryResults.collectedTuples) {
      Assert.assertEquals("result points " + r, Math.min(numMinutes, 20), r.data.size());
      for(Object o : r.data)
        System.out.println(o);
    }
  }
}
