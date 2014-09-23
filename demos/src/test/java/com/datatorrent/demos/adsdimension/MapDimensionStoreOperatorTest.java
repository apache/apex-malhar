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
import com.datatorrent.demos.adsdimension.MapDimensionStoreOperator.HDSRangeQueryResult;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.google.common.util.concurrent.MoreExecutors;

public class MapDimensionStoreOperatorTest
{
  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  @Test
  public void testQueryFromHDS() throws Exception {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    MapDimensionStoreOperator hdsOut = new MapDimensionStoreOperator() {
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
    EventSchema eventDesc = GenericEventSerializerTest.getDataDesc();
    MapAggregator aggregator = new MapAggregator(eventDesc);
    aggregator.init("time=MINUTES:pubId:adId:adUnit");
    GenericEventSerializer serializer = new GenericEventSerializer(eventDesc);
    hdsOut.setEventDesc(eventDesc);
    hdsOut.setAggregator(aggregator);
    hdsOut.setMaxCacheSize(1);
    hdsOut.setFlushIntervalCount(0);
    hdsOut.setup(null);

    hdsOut.setDebug(false);

    CollectorTestSink<MapDimensionStoreOperator.HDSRangeQueryResult> queryResults = new CollectorTestSink<MapDimensionStoreOperator.HDSRangeQueryResult>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink) queryResults;
    hdsOut.queryResult.setSink(tmp);

    hdsOut.beginWindow(1);

    long baseTime = System.currentTimeMillis();
    long baseMinute = TimeUnit.MILLISECONDS.convert(TimeUnit.MINUTES.convert(baseTime, TimeUnit.MILLISECONDS), TimeUnit.MINUTES);

    // Check aggregation for ae1 and ae2 as they have same key.
    MapAggregate ae1 = new MapAggregate(0);
    ae1.setTimestamp(baseMinute);
    ae1.keys.put("pubId", 1);
    ae1.keys.put("adId", 2);
    ae1.keys.put("adUnit", 3);
    ae1.fields.put("clicks", 10L);
    hdsOut.input.process(ae1);

    MapAggregate ae2 = new MapAggregate(0);
    ae2.setTimestamp(baseMinute);
    ae2.keys.put("pubId", 1);
    ae2.keys.put("adId", 2);
    ae2.keys.put("adUnit", 3);
    ae2.fields.put("clicks", 20L);
    hdsOut.input.process(ae2);

    MapAggregate ae3 = new MapAggregate(0);
    ae3.setTimestamp(baseMinute + TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
    ae3.keys.put("pubId", 1);
    ae3.keys.put("adId", 2);
    ae3.keys.put("adUnit", 3);
    ae3.fields.put("clicks", 10L);
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

    Assert.assertEquals("rangeQueries " + hdsOut.rangeQueries, 1, hdsOut.rangeQueries.size());
    MapDimensionStoreOperator.HDSRangeQuery aq = hdsOut.rangeQueries.values().iterator().next();
    Assert.assertEquals("numTimeUnits " + hdsOut.rangeQueries, baseMinute, aq.startTime);

    hdsOut.endWindow();

    Assert.assertEquals("queryResults " + queryResults.collectedTuples, 1, queryResults.collectedTuples.size());
    HDSRangeQueryResult r = queryResults.collectedTuples.iterator().next();
    Assert.assertEquals("result points " + r, 2, r.data.size());

    // ae1 object is stored as referenced in cache, and when new tuple is aggregated,
    // the new values are updated in ae1 itself, causing following check to fail.
    //Assert.assertEquals("clicks", ae1.clicks + ae2.clicks, r.data.get(0).clicks);
    Assert.assertEquals("clicks", 30L, r.data.get(0).get("clicks"));
    Assert.assertEquals("clicks", ae3.fields.get("clicks"), r.data.get(1).get("clicks"));
  }


  @Test
  public void testQueryFromHDSWithSubsetKeys() throws Exception {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    MapDimensionStoreOperator hdsOut = new MapDimensionStoreOperator() {
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
    EventSchema eventDesc = GenericEventSerializerTest.getDataDesc();
    MapAggregator aggregator = new MapAggregator(eventDesc);
    aggregator.init("time=MINUTES:pubId:adId:adUnit");
    GenericEventSerializer serializer = new GenericEventSerializer(eventDesc);
    hdsOut.setEventDesc(eventDesc);
    hdsOut.setAggregator(aggregator);
    hdsOut.setMaxCacheSize(1);
    hdsOut.setFlushIntervalCount(0);
    hdsOut.setup(null);

    hdsOut.setDebug(false);

    CollectorTestSink<MapDimensionStoreOperator.HDSRangeQueryResult> queryResults = new CollectorTestSink<MapDimensionStoreOperator.HDSRangeQueryResult>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink) queryResults;
    hdsOut.queryResult.setSink(tmp);

    hdsOut.beginWindow(1);

    long baseTime = System.currentTimeMillis();
    long baseMinute = TimeUnit.MILLISECONDS.convert(TimeUnit.MINUTES.convert(baseTime, TimeUnit.MILLISECONDS), TimeUnit.MINUTES);

    // Check aggregation for ae1 and ae2 as they have same key.
    MapAggregate ae1 = new MapAggregate(0);
    ae1.setTimestamp(baseMinute);
    ae1.keys.put("pubId", 1);
    ae1.keys.put("adUnit", 3);
    ae1.fields.put("clicks", 10L);
    hdsOut.input.process(ae1);

    MapAggregate ae2 = new MapAggregate(0);
    ae2.setTimestamp(baseMinute);
    ae2.keys.put("pubId", 1);
    ae2.keys.put("adUnit", 3);
    ae2.fields.put("clicks", 20L);
    hdsOut.input.process(ae2);

    MapAggregate ae3 = new MapAggregate(0);
    ae3.setTimestamp(baseMinute + TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
    ae3.keys.put("pubId", 1);
    ae3.keys.put("adUnit", 3);
    ae3.fields.put("clicks", 10L);
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

    Assert.assertEquals("rangeQueries " + hdsOut.rangeQueries, 1, hdsOut.rangeQueries.size());
    MapDimensionStoreOperator.HDSRangeQuery aq = hdsOut.rangeQueries.values().iterator().next();
    Assert.assertEquals("numTimeUnits " + hdsOut.rangeQueries, baseMinute, aq.startTime);

    hdsOut.endWindow();

    Assert.assertEquals("queryResults " + queryResults.collectedTuples, 1, queryResults.collectedTuples.size());
    HDSRangeQueryResult r = queryResults.collectedTuples.iterator().next();
    Assert.assertEquals("result points " + r, 2, r.data.size());

    // ae1 object is stored as referenced in cache, and when new tuple is aggregated,
    // the new values are updated in ae1 itself, causing following check to fail.
    //Assert.assertEquals("clicks", ae1.clicks + ae2.clicks, r.data.get(0).clicks);
    Assert.assertEquals("clicks", 30L, r.data.get(0).get("clicks"));
    Assert.assertEquals("clicks", ae3.fields.get("clicks"), r.data.get(1).get("clicks"));
    // when data is returned from HDS, all keys are part of response,
    // not present keys will have 0 values.
    Assert.assertEquals("from HDS", 0, r.data.get(0).get("adId"));
    // when data is returned from Cache, not specified keys will not
    // be present in the map.
    Assert.assertEquals("from cache", null, r.data.get(1).get("adId"));
  }


  @Test
  public void testQueryLessKeys() throws Exception {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    MapDimensionStoreOperator hdsOut = new MapDimensionStoreOperator() {
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
    EventSchema eventDesc = GenericEventSerializerTest.getDataDesc();
    MapAggregator aggregator = new MapAggregator(eventDesc);
    aggregator.init("time=MINUTES:pubId:adId:adUnit");
    GenericEventSerializer serializer = new GenericEventSerializer(eventDesc);
    hdsOut.setEventDesc(eventDesc);
    hdsOut.setAggregator(aggregator);
    hdsOut.setMaxCacheSize(100);
    hdsOut.setFlushIntervalCount(100);
    hdsOut.setup(null);

    hdsOut.setDebug(false);

    CollectorTestSink<MapDimensionStoreOperator.HDSRangeQueryResult> queryResults = new CollectorTestSink<MapDimensionStoreOperator.HDSRangeQueryResult>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink) queryResults;
    hdsOut.queryResult.setSink(tmp);

    hdsOut.beginWindow(1);

    long baseTime = System.currentTimeMillis();
    long baseMinute = TimeUnit.MILLISECONDS.convert(TimeUnit.MINUTES.convert(baseTime, TimeUnit.MILLISECONDS), TimeUnit.MINUTES);

    // Check aggregation for ae1 and ae2 as they have same key.
    MapAggregate ae1 = new MapAggregate(0);
    ae1.setTimestamp(baseMinute);
    ae1.keys.put("pubId", 1);
    ae1.keys.put("adId", 2);
    ae1.fields.put("clicks", 10L);
    hdsOut.input.process(ae1);

    MapAggregate ae2 = new MapAggregate(0);
    ae2.setTimestamp(baseMinute);
    ae2.keys.put("pubId", 1);
    ae2.keys.put("adId", 2);
    ae2.fields.put("clicks", 20L);
    hdsOut.input.process(ae2);

    MapAggregate ae3 = new MapAggregate(0);
    ae3.setTimestamp(baseMinute + TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
    ae3.keys.put("pubId", 1);
    ae3.keys.put("adId", 2);
    ae3.fields.put("clicks", 10L);
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

    Assert.assertEquals("rangeQueries " + hdsOut.rangeQueries, 1, hdsOut.rangeQueries.size());
    MapDimensionStoreOperator.HDSRangeQuery aq = hdsOut.rangeQueries.values().iterator().next();
    Assert.assertEquals("numTimeUnits " + hdsOut.rangeQueries, baseMinute, aq.startTime);

    hdsOut.endWindow();

    Assert.assertEquals("queryResults " + queryResults.collectedTuples, 1, queryResults.collectedTuples.size());
    HDSRangeQueryResult r = queryResults.collectedTuples.iterator().next();
    Assert.assertEquals("result points " + r, 2, r.data.size());

    // ae1 object is stored as referenced in cache, and when new tuple is aggregated,
    // the new values are updated in ae1 itself, causing following check to fail.
    //Assert.assertEquals("clicks", ae1.clicks + ae2.clicks, r.data.get(0).clicks);
    Assert.assertEquals("clicks", 30L, r.data.get(0).get("clicks"));
    Assert.assertEquals("clicks", ae3.fields.get("clicks"), r.data.get(1).get("clicks"));
  }


  /* Test if queries with different key combination works */
  @Test
  public void testQueryLessKeys1() throws Exception {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    EventSchema eventDesc = GenericEventSerializerTest.getDataDesc();

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

    MapAggregator[] aggregators = new MapAggregator[dimensionSpecs.length];
    for (int i = dimensionSpecs.length; i-- > 0;) {
      MapAggregator aggregator = new MapAggregator(eventDesc);
      aggregator.init(dimensionSpecs[i]);
      aggregators[i] = aggregator;
    }

    MapDimensionStoreOperator hdsOut = new MapDimensionStoreOperator() {
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
    //MapAggregator aggregator = new MapAggregator(eventDesc);
    //aggregator.init("time=MINUTES:pubId:adId:adUnit");
    GenericEventSerializer serializer = new GenericEventSerializer(eventDesc);
    hdsOut.setEventDesc(eventDesc);
    hdsOut.setAggregator(aggregators[0]);
    hdsOut.setMaxCacheSize(100);
    hdsOut.setFlushIntervalCount(100);
    hdsOut.setup(null);

    hdsOut.setDebug(false);

    CollectorTestSink<MapDimensionStoreOperator.HDSRangeQueryResult> queryResults = new CollectorTestSink<MapDimensionStoreOperator.HDSRangeQueryResult>();
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
      Map<String, Object> e1 = Maps.newHashMap();
      e1.put("timestamp", timestamp);
      e1.put("pubId", 1);
      e1.put("adId", 2);
      e1.put("adUnit", 3);
      e1.put("clicks", 10L);

      int aggrIdx = 0;
      for (MapAggregator aggregator : aggregators) {
        MapAggregate aggr = aggregator.getGroup(e1, aggrIdx);
        aggregator.aggregate(aggr, e1);
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
    Assert.assertEquals("rangeQueries " + hdsOut.rangeQueries, 8, hdsOut.rangeQueries.size());
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
