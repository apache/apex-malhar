/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema;
import com.datatorrent.lib.dimensions.AggregatorIncrementalType;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperatorTest.FSTestWatcher;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.lib.util.TestUtils.TestInfo;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppDataSingleSchemaDimensionStoreHDHTTest
{
  private static final Logger logger = LoggerFactory.getLogger(AppDataSingleSchemaDimensionStoreHDHTTest.class);

  @Rule
  public TestInfo testMeta = new FSTestWatcher() {
    @Override
    protected void starting(Description descriptor)
    {
      super.starting(descriptor);

      try {
        FileUtils.deleteDirectory(new File(getDir()));
      }
      catch(IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    protected void finished(Description description)
    {
      try {
        FileUtils.deleteDirectory(new File(getDir()));
      }
      catch(IOException ex) {
        throw new RuntimeException(ex);
      }

      super.finished(description);
    }
  };

  @Test
  public void storeFormatTest() throws Exception
  {
    String eventSchemaString = SchemaUtils.jarResourceFileToString("dimensionsTestSchema.json");

    String basePath = testMeta.getDir();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath(basePath);

    AppDataSingleSchemaDimensionStoreHDHT store = new AppDataSingleSchemaDimensionStoreHDHT();

    store.setEventSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    long windowId = 0L;
    store.setup(null);
    store.beginWindow(windowId);
    store.endWindow();
    store.checkpointed(windowId);
    store.committed(windowId);

    windowId++;
    store.beginWindow(windowId);

    byte[] storeFormat = store.load(AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID,
                                    DimensionsStoreHDHT.STORE_FORMAT_KEY);
    Assert.assertEquals(DimensionsStoreHDHT.STORE_FORMAT_VERSION, GPOUtils.deserializeInt(storeFormat));
    store.endWindow();
    store.teardown();
  }

  @Test
  public void storeWindowIDTest()
  {
    String eventSchemaString = SchemaUtils.jarResourceFileToString("dimensionsTestSchema.json");

    String basePath = testMeta.getDir();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath(basePath);

    AppDataSingleSchemaDimensionStoreHDHT store = new AppDataSingleSchemaDimensionStoreHDHT();

    store.setEventSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    long windowId = 0L;
    store.setup(null);
    store.beginWindow(windowId);
    byte[] windowIDBytes = store.load(AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID,
                                      DimensionsStoreHDHT.WINDOW_ID_KEY);
    Assert.assertEquals(null, windowIDBytes);
    store.endWindow();
    store.checkpointed(windowId);
    store.committed(windowId);

    for(int windowCounter = 0;
        windowCounter < 2;
        windowCounter++) {
      windowId++;
      store.beginWindow(windowId);
      windowIDBytes = store.load(AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID,
                                 DimensionsStoreHDHT.WINDOW_ID_KEY);
      Assert.assertEquals(windowId - 1L, GPOUtils.deserializeLong(windowIDBytes));
      store.endWindow();
      store.checkpointed(windowId);
      store.committed(windowId);
    }

    store.teardown();
  }

  @Test
  public void serializationTest() throws Exception
  {
    String eventSchemaString = SchemaUtils.jarResourceFileToString("dimensionsTestSchema.json");
    String basePath = testMeta.getDir();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath(basePath);

    AppDataSingleSchemaDimensionStoreHDHT store = new AppDataSingleSchemaDimensionStoreHDHT();

    store.setEventSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    store.setup(null);
    TestUtils.clone(new Kryo(), store);
    store.beginWindow(0L);
    store.endWindow();
    store.teardown();
  }

  @Test
  public void dataSerializationTest()
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

    store.setEventSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);

    store.setup(null);

    //Aggregate Event
    DimensionalConfigurationSchema eventSchema = store.eventSchema;
    Aggregate ae = createEvent(eventSchema,
                                    publisher,
                                    advertiser,
                                    60000L,
                                    TimeBucket.MINUTE,
                                    impressions,
                                    cost);

    //Key bytes
    byte[] keyBytes = store.getKeyBytesGAE(ae);
    //Value bytes
    byte[] valueBytes = store.getValueBytesGAE(ae);

    Aggregate deserializedAE = store.fromKeyValueGAE(new Slice(keyBytes), valueBytes);
    deserializedAE.getEventKey().getKey().setFieldDescriptor(ae.getEventKey().getKey().getFieldDescriptor());
    deserializedAE.getAggregates().setFieldDescriptor(ae.getAggregates().getFieldDescriptor());

    Assert.assertEquals("Test aggregates", ae.getAggregates(), deserializedAE.getAggregates());
    Assert.assertEquals("event keys must be equal", ae.getEventKey(), deserializedAE.getEventKey());

    store.beginWindow(0L);
    store.endWindow();
    store.teardown();
  }

  @Test
  public void putReadTest() throws Exception
  {
    readTestHelper(true);
  }

  @Test
  public void portReadTest() throws Exception
  {
    readTestHelper(false);
  }

  private void readTestHelper(boolean useHDHTPut) throws Exception
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

    store.setEventSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    long windowId = 0;
    store.setup(null);
    //STARTING WINDOW 0
    store.beginWindow(windowId);

    DimensionalConfigurationSchema eventSchema = store.eventSchema;

    //Aggregate Event
    Aggregate ae = createEvent(eventSchema,
                                    publisher,
                                    advertiser,
                                    60000L,
                                    TimeBucket.MINUTE,
                                    impressions,
                                    cost);

    if(!useHDHTPut) {
      store.input.put(ae);
      Assert.assertEquals("The item must be in the cache.", ae, store.cache.get(ae.getEventKey()));
    }
    else {
      store.put(AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID,
                new Slice(store.getKeyBytesGAE(ae)),
                store.getValueBytesGAE(ae));
      Assert.assertEquals("The item must be in the cache.", ae, store.load(ae.getEventKey()));
    }

    store.endWindow();
    store.checkpointed(windowId);
    store.committed(windowId);
    //STARTING WINDOW 1
    windowId++;
    store.beginWindow(windowId);
    store.endWindow();
    store.checkpointed(windowId);
    store.committed(windowId);
    //STARTING WINDOW 2
    windowId++;
    store.beginWindow(windowId);

    byte[] keyBytes = store.getKeyBytesGAE(ae);
    byte[] valueBytes = store.getUncommitted(AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID, new Slice(keyBytes));

    if(valueBytes == null) {
      valueBytes = store.get(AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID, new Slice(keyBytes));
    }

    Aggregate aeDeserialized = store.fromKeyValueGAE(new Slice(keyBytes), valueBytes);

    aeDeserialized.getKeys().setFieldDescriptor(ae.getKeys().getFieldDescriptor());
    aeDeserialized.getAggregates().setFieldDescriptor(ae.getAggregates().getFieldDescriptor());
    Assert.assertEquals("The values must be equal", ae, aeDeserialized);

    store.endWindow();
    store.teardown();
  }

  @Test
  public void cacheFlushTest()
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

    store.setCacheWindowDuration(1);
    store.setEventSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    store.setup(null);

    DimensionalConfigurationSchema eventSchema = store.eventSchema;

    //Aggregate Event
    Aggregate ae = createEvent(eventSchema,
                                    publisher,
                                    advertiser,
                                    60000L,
                                    TimeBucket.MINUTE,
                                    impressions,
                                    cost);

    long windowId = 0L;
    store.beginWindow(windowId);
    Assert.assertEquals(0, store.cache.size());
    store.input.put(ae);
    Assert.assertEquals(1, store.cache.size());
    store.endWindow();
    store.checkpointed(windowId);
    store.committed(windowId);
    windowId++;

    store.beginWindow(windowId);
    Assert.assertEquals(0, store.cache.size());
    Assert.assertEquals(ae, store.load(ae.getEventKey()));
    store.endWindow();
    store.checkpointed(windowId);
    store.committed(windowId);
    windowId++;

    store.teardown();
  }

  @Test
  public void aggregationTest()
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
    store.setEventSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    store.setup(null);

    DimensionalConfigurationSchema eventSchema = store.eventSchema;

    Aggregate expectedDouble = createEvent(eventSchema,
                                                publisher,
                                                advertiser,
                                                60000L,
                                                TimeBucket.MINUTE,
                                                2 * impressions,
                                                2.0 * cost);

    Aggregate expectedTriple = createEvent(eventSchema,
                                                publisher,
                                                advertiser,
                                                60000L,
                                                TimeBucket.MINUTE,
                                                3 * impressions,
                                                3.0 * cost);

    //Aggregate Event
    Aggregate ae = createEvent(eventSchema,
                                    publisher,
                                    advertiser,
                                    60000L,
                                    TimeBucket.MINUTE,
                                    impressions,
                                    cost);

    long windowId = 1L;
    store.beginWindow(windowId);
    store.input.put(ae);
    ae = createEvent(eventSchema,
                    publisher,
                    advertiser,
                    60000L,
                    TimeBucket.MINUTE,
                    impressions,
                    cost);
    store.input.put(ae);
    Assert.assertEquals(expectedDouble, store.cache.get(ae.getEventKey()));
    store.endWindow();
    store.checkpointed(windowId);
    store.committed(windowId);
    windowId++;

    store.beginWindow(windowId);
    ae = createEvent(eventSchema,
                     publisher,
                     advertiser,
                     60000L,
                     TimeBucket.MINUTE,
                     impressions,
                     cost);
    store.input.put(ae);
    Assert.assertEquals(expectedTriple, store.cache.get(ae.getEventKey()));
    store.endWindow();
    store.checkpointed(windowId);
    store.committed(windowId);

    store.teardown();
  }

  @Test
  public void faultToleranceTest() throws Exception
  {
    final String publisher = "google";
    final String advertiser = "safeway";
    final long impressions = 10L;
    final double cost = 1.0;

    final String publisher1 = "twitter";
    final String advertiser1 = "safeway";
    final long impressions1 = 15L;
    final double cost1 = 2.0;

    String eventSchemaString = SchemaUtils.jarResourceFileToString("dimensionsTestSchema.json");

    String basePath = testMeta.getDir();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath(basePath);

    AppDataSingleSchemaDimensionStoreHDHT store = new AppDataSingleSchemaDimensionStoreHDHT();

    store.setCacheWindowDuration(2);
    store.setEventSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    store.setup(null);

    DimensionalConfigurationSchema eventSchema = store.eventSchema;

    //Aggregate Event
    Aggregate ae = createEvent(eventSchema,
                                    publisher,
                                    advertiser,
                                    60000L,
                                    TimeBucket.MINUTE,
                                    impressions,
                                    cost);

    Aggregate ae1 = createEvent(eventSchema,
                                     publisher1,
                                     advertiser1,
                                     60000L,
                                     TimeBucket.MINUTE,
                                     impressions1,
                                     cost1);

    long windowId = 1L;
    store.beginWindow(windowId);
    store.input.put(ae);
    store.endWindow();
    store.checkpointed(windowId);
    store.committed(windowId);
    windowId++;

    store.beginWindow(windowId);
    store.input.put(ae1);
    Assert.assertEquals(2, store.cache.size());
    store.endWindow();
    Assert.assertEquals(0, store.cache.size());
    store.checkpointed(windowId);
    store.committed(windowId);
    windowId++;

    //Simulate failure

    store.readMetaData = false;
    store.beginWindow(windowId);
    store.currentWindowID = windowId - 1L;
    Assert.assertEquals(1, store.futureBuckets.size());
    Assert.assertEquals(Sets.newHashSet(AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID),
                        store.futureBuckets.keySet());
    Assert.assertEquals(2L,
                        (long) store.futureBuckets.get(AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID));
    Assert.assertEquals(ae1, store.load(ae1.getEventKey()));
    store.input.put(ae1);
    Assert.assertEquals(0, store.cache.size());
    store.endWindow();
    Assert.assertEquals(0, store.futureBuckets.size());
    store.checkpointed(windowId);
    store.committed(windowId);

    Assert.assertEquals(ae, store.load(ae.getEventKey()));
    Assert.assertEquals(ae1, store.load(ae1.getEventKey()));

    store.teardown();
  }

  private Aggregate createEvent(DimensionalConfigurationSchema eventSchema,
                                     String publisher,
                                     String advertiser,
                                     long timestamp,
                                     TimeBucket timeBucket,
                                     long impressions,
                                     double cost)
  {
    int schemaID = AbstractDimensionsComputationFlexibleSingleSchema.DEFAULT_SCHEMA_ID;
    int dimensionDescriptorID = 0;
    int aggregatorID = eventSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID().get(AggregatorIncrementalType.SUM.name());

    FieldsDescriptor fdKey = eventSchema.getDdIDToKeyDescriptor().get(dimensionDescriptorID);
    FieldsDescriptor fdValue = eventSchema.getDdIDToAggIDToOutputAggDescriptor().get(dimensionDescriptorID).get(aggregatorID);
    DimensionsDescriptor dd = eventSchema.getDdIDToDD().get(dimensionDescriptorID);

    GPOMutable key = new GPOMutable(fdKey);

    key.setField("publisher", publisher);
    key.setField("advertiser", advertiser);
    key.setField(DimensionsDescriptor.DIMENSION_TIME, timeBucket.roundDown(timestamp));
    key.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, timeBucket.ordinal());

    EventKey eventKey = new EventKey(schemaID,
                                     dimensionDescriptorID,
                                     aggregatorID,
                                     key);

    GPOMutable value = new GPOMutable(fdValue);

    value.setField("impressions", impressions);
    value.setField("cost", cost);

    //Aggregate Event
    return new Aggregate(eventKey,
                              value);
  }
}
