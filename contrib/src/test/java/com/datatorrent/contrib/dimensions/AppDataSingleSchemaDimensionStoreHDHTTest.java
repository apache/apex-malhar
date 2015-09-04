/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import java.io.File;
import java.io.IOException;

import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.aggregator.AggregatorIncrementalType;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperatorTest.FSTestWatcher;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.lib.util.TestUtils.TestInfo;

import com.datatorrent.contrib.hdht.tfile.TFileImpl;

import com.datatorrent.netlet.util.Slice;

public class AppDataSingleSchemaDimensionStoreHDHTTest
{
  @Rule
  public TestInfo testMeta = new StoreFSTestWatcher();

  public static class StoreFSTestWatcher extends FSTestWatcher {
    public StoreFSTestWatcher()
    {
    }

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

      Thread.interrupted();
      super.finished(description);
    }
  }

  @Rule
  public TestWatcher interruptClear = new InterruptClear();

  public static class InterruptClear extends TestWatcher
  {
    @Override
    protected void starting(Description description)
    {
      Thread.interrupted();
    }

    @Override
    protected void finished(Description description)
    {
      Thread.interrupted();
    }
  }

  @Test
  public void storeFormatTest() throws Exception
  {
    String eventSchemaString = SchemaUtils.jarResourceFileToString("dimensionsTestSchema.json");

    String basePath = testMeta.getDir();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath(basePath);

    AppDataSingleSchemaDimensionStoreHDHT store = new AppDataSingleSchemaDimensionStoreHDHT();

    store.setConfigurationSchemaJSON(eventSchemaString);
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

    store.setConfigurationSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    long windowId = 0L;
    store.setup(null);
    store.beginWindow(windowId);
    byte[] windowIDBytes = store.load(AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID,
                                      DimensionsStoreHDHT.WINDOW_ID_KEY);
    Assert.assertArrayEquals(null, windowIDBytes);
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

    store.setConfigurationSchemaJSON(eventSchemaString);
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

    store.setConfigurationSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);

    store.setup(null);

    //Aggregate Event
    DimensionalConfigurationSchema eventSchema = store.configurationSchema;
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

    store.setConfigurationSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    long windowId = 0;
    store.setup(null);
    //STARTING WINDOW 0
    store.beginWindow(windowId);

    DimensionalConfigurationSchema eventSchema = store.configurationSchema;

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

    LOG.debug("value bytes size {}", valueBytes.length);

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
    store.setConfigurationSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    store.setup(null);

    DimensionalConfigurationSchema eventSchema = store.configurationSchema;

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
    store.setConfigurationSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    store.setup(null);

    DimensionalConfigurationSchema eventSchema = store.configurationSchema;

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
    store.setConfigurationSchemaJSON(eventSchemaString);
    store.setFileStore(hdsFile);
    store.setFlushIntervalCount(1);
    store.setFlushSize(0);

    store.setup(null);

    DimensionalConfigurationSchema eventSchema = store.configurationSchema;

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

  public static Aggregate createEvent(DimensionalConfigurationSchema eventSchema,
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

    FieldsDescriptor fdKey = eventSchema.getDimensionsDescriptorIDToKeyDescriptor().get(dimensionDescriptorID);
    FieldsDescriptor fdValue = eventSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(dimensionDescriptorID).get(aggregatorID);

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

  public static GPOMutable createQueryKey(DimensionalConfigurationSchema eventSchema,
                                          String publisher,
                                          String advertiser)
  {
    FieldsDescriptor fdKey = eventSchema.getDimensionsDescriptorIDToKeyDescriptor().get(0);

    Map<String, Type> fieldToType = Maps.newHashMap();
    fieldToType.put("publisher", fdKey.getType("publisher"));
    fieldToType.put("advertiser", fdKey.getType("advertiser"));

    GPOMutable key = new GPOMutable(new FieldsDescriptor(fieldToType));

    key.setField("publisher", publisher);
    key.setField("advertiser", advertiser);

    return key;
  }

  public static Aggregate createEvent1(DimensionalConfigurationSchema eventSchema,
                                       String publisher,
                                       String advertiser,
                                       long timestamp,
                                       TimeBucket timeBucket,
                                       long impressions,
                                       double cost)
  {
    int schemaID = AbstractDimensionsComputationFlexibleSingleSchema.DEFAULT_SCHEMA_ID;
    int dimensionDescriptorID = 1;
    int aggregatorID = eventSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID().get(AggregatorIncrementalType.CUM_SUM.name());

    FieldsDescriptor fdKey = eventSchema.getDimensionsDescriptorIDToKeyDescriptor().get(dimensionDescriptorID);
    FieldsDescriptor fdValue = eventSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(dimensionDescriptorID).get(aggregatorID);
    DimensionsDescriptor dd = eventSchema.getDimensionsDescriptorIDToDimensionsDescriptor().get(dimensionDescriptorID);

    GPOMutable key = new GPOMutable(fdKey);

    key.setField("publisher", publisher);
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

  private static final Logger LOG = LoggerFactory.getLogger(AppDataSingleSchemaDimensionStoreHDHTTest.class);
}
