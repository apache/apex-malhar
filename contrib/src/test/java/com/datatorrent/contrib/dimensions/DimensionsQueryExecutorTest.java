/*
 * Copyright (c) 2015 DataTorrent
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
package com.datatorrent.contrib.dimensions;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;

import org.apache.commons.lang3.mutable.MutableLong;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.DataResultDimensional;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsAggregatable;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.util.TestUtils.TestInfo;

import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHTTest.InterruptClear;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHTTest.StoreFSTestWatcher;
import com.datatorrent.contrib.hdht.HDHTReader.HDSQuery;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;

import com.datatorrent.netlet.util.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DimensionsQueryExecutorTest
{
  public enum TestType
  {
    IN_RANGE, OUT_OF_RANGE, PARTIALLY_OUT_OF_RANGE;
  }

  @Rule
  public TestInfo testMeta = new StoreFSTestWatcher();

  @Rule
  public TestWatcher interruptClear = new InterruptClear();

  @Test
  public void simpleQueryExecutorTest()
  {
    simpleQueryCountHelper(1);
  }

  @Test
  public void simpleRollingQueryExecutorTest()
  {
    simpleQueryCountHelper(5);
  }

  private void simpleQueryCountHelper(int rollingCount)
  {
    final String publisher = "google";
    final String advertiser = "safeway";

    final long impressions = 10L;
    final double cost = 1.0;

    String eventSchemaString = SchemaUtils.jarResourceFileToString("dimensionsTestSchema.json");

    String basePath = testMeta.getDir();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath(basePath);

    AppDataSingleSchemaDimensionStoreHDHT store = new AppDataSingleSchemaDimensionStoreHDHT();

    store.setCacheWindowDuration(2);
    store.setConfigurationSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    store.setup(null);

    DimensionalConfigurationSchema eventSchema = store.configurationSchema;
    DimensionsQueryExecutor dqe = new DimensionsQueryExecutor(store, store.schemaRegistry);

    store.beginWindow(0L);

    long currentTime = 0L;

    List<Map<String, HDSQuery>> hdsQueries = Lists.newArrayList();
    List<Map<String, EventKey>> eventKeys = Lists.newArrayList();

    for (int rollingCounter = 0;; currentTime += TimeUnit.MINUTES.toMillis(1L)) {
      Aggregate aggregate = AppDataSingleSchemaDimensionStoreHDHTTest.createEvent(eventSchema,
                                                                                  publisher,
                                                                                  advertiser,
                                                                                  currentTime,
                                                                                  TimeBucket.MINUTE,
                                                                                  impressions,
                                                                                  cost);

      store.input.put(aggregate);

      issueHDSQuery(store, aggregate.getEventKey());

      Map<String, HDSQuery> aggregatorToQuery = Maps.newHashMap();
      aggregatorToQuery.put("SUM", store.getQueries().values().iterator().next());
      hdsQueries.add(aggregatorToQuery);

      Map<String, EventKey> aggregatorToEventKey = Maps.newHashMap();
      aggregatorToEventKey.put("SUM", aggregate.getEventKey());
      eventKeys.add(aggregatorToEventKey);

      rollingCounter++;

      if (rollingCounter == rollingCount) {
        break;
      }
    }

    QueryMeta queryMeta = new QueryMeta();
    queryMeta.setHdsQueries(hdsQueries);
    queryMeta.setEventKeys(eventKeys);

    GPOMutable keys = AppDataSingleSchemaDimensionStoreHDHTTest.createQueryKey(eventSchema, publisher, advertiser);
    Map<String, Set<String>> fieldToAggregators = Maps.newHashMap();
    fieldToAggregators.put("impressions", Sets.newHashSet("SUM"));
    fieldToAggregators.put("cost", Sets.newHashSet("SUM"));

    FieldsAggregatable fieldsAggregatable = new FieldsAggregatable(fieldToAggregators);

    DataQueryDimensional query = new DataQueryDimensional("1",
                                                          DataQueryDimensional.TYPE,
                                                          currentTime,
                                                          currentTime,
                                                          TimeBucket.MINUTE,
                                                          keys,
                                                          fieldsAggregatable,
                                                          true);
    query.setSlidingAggregateSize(rollingCount);

    DataResultDimensional drd = (DataResultDimensional)dqe.executeQuery(query, queryMeta, new MutableLong(1L));

    store.endWindow();

    Assert.assertEquals(1, drd.getValues().size());
    Assert.assertEquals(impressions * rollingCount, drd.getValues().get(0).get("SUM").getFieldLong("impressions"));

    store.teardown();
  }

  public static void issueHDSQuery(DimensionsStoreHDHT store, EventKey eventKey)
  {
    LOG.debug("Issued QUERY");
    Slice key = new Slice(store.getEventKeyBytesGAE(eventKey));
    HDSQuery hdsQuery = new HDSQuery();
    hdsQuery.bucketKey = AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID;
    hdsQuery.key = key;
    store.addQuery(hdsQuery);
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsQueryExecutorTest.class);
}
