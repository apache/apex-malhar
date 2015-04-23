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
import com.datatorrent.lib.appdata.dimensions.AggregateEvent;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent.EventKey;
import com.datatorrent.lib.appdata.dimensions.AggregatorStaticType;
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DimensionalEventSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperatorTest.FSTestWatcher;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.lib.util.TestUtils.TestInfo;
import com.esotericsoftware.kryo.Kryo;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppDataSingleSchemaDimensionStoreHDHTTest
{
  private static final Logger logger = LoggerFactory.getLogger(AppDataSingleSchemaDimensionStoreHDHTTest.class);

  @Rule public TestInfo testMeta = new FSTestWatcher();

  @Test
  public void serializationTest() throws Exception
  {
    AppDataSingleSchemaDimensionStoreHDHT store = new AppDataSingleSchemaDimensionStoreHDHT();
    AppDataSingleSchemaDimensionStoreHDHT store1 = TestUtils.clone(new Kryo(), store);
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

    DimensionalEventSchema eventSchema = store.eventSchema;

    int schemaID = AppDataSingleSchemaDimensionStoreHDHT.SCHEMA_ID;
    int dimensionDescriptorID = 0;
    int aggregatorID = eventSchema.getAggregatorInfo().getStaticAggregatorNameToID().get(AggregatorStaticType.SUM.name());

    FieldsDescriptor fdKey = eventSchema.getDdIDToKeyDescriptor().get(dimensionDescriptorID);
    FieldsDescriptor fdValue = eventSchema.getDdIDToAggIDToOutputAggDescriptor().get(dimensionDescriptorID).get(aggregatorID);
    DimensionsDescriptor dd = eventSchema.getDdIDToDD().get(dimensionDescriptorID);

    GPOMutable key = new GPOMutable(fdKey);

    key.setField("publisher", publisher);
    key.setField("advertiser", advertiser);
    key.setField(DimensionsDescriptor.DIMENSION_TIME, 0L);
    key.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, dd.getTimeBucket().ordinal());

    EventKey eventKey = new EventKey(schemaID,
                                     dimensionDescriptorID,
                                     aggregatorID,
                                     key);

    GPOMutable value = new GPOMutable(fdValue);

    value.setField("impressions", impressions);
    value.setField("cost", cost);

    //Aggregate Event
    AggregateEvent ae = new AggregateEvent(eventKey,
                                           value);

    //Key bytes
    byte[] keyBytes = store.getKeyBytesGAE(ae);
    //Value bytes
    byte[] valueBytes = store.getValueBytesGAE(ae);

    AggregateEvent deserializedAE = store.fromKeyValueGAE(new Slice(keyBytes), valueBytes);
    deserializedAE.getEventKey().getKey().setFieldDescriptor(fdKey);
    deserializedAE.getAggregates().setFieldDescriptor(fdValue);

    Assert.assertEquals("Test aggregates", value, deserializedAE.getAggregates());

    Assert.assertEquals("event keys must be equal", eventKey, deserializedAE.getEventKey());
    Assert.assertEquals("value keys must be equal", value, deserializedAE.getAggregates());
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

    DimensionalEventSchema eventSchema = store.eventSchema;

    int schemaID = AppDataSingleSchemaDimensionStoreHDHT.SCHEMA_ID;
    int dimensionDescriptorID = 0;
    int aggregatorID = eventSchema.getAggregatorInfo().getStaticAggregatorNameToID().get(AggregatorStaticType.SUM.name());

    FieldsDescriptor fdKey = eventSchema.getDdIDToKeyDescriptor().get(dimensionDescriptorID);
    FieldsDescriptor fdValue = eventSchema.getDdIDToAggIDToOutputAggDescriptor().get(dimensionDescriptorID).get(aggregatorID);
    DimensionsDescriptor dd = eventSchema.getDdIDToDD().get(dimensionDescriptorID);

    GPOMutable key = new GPOMutable(fdKey);

    key.setField("publisher", publisher);
    key.setField("advertiser", advertiser);
    key.setField(DimensionsDescriptor.DIMENSION_TIME, 0L);
    key.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, dd.getTimeBucket().ordinal());

    EventKey eventKey = new EventKey(schemaID,
                                     dimensionDescriptorID,
                                     aggregatorID,
                                     key);

    GPOMutable value = new GPOMutable(fdValue);

    value.setField("impressions", impressions);
    value.setField("cost", cost);

    //Aggregate Event
    AggregateEvent ae = new AggregateEvent(eventKey,
                                           value);

    if(!useHDHTPut) {
      store.input.put(ae);
    }
    else {
      store.put(AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID,
                new Slice(store.getKeyBytesGAE(ae)),
                store.getValueBytesGAE(ae));
    }

    Assert.assertEquals("The item must be in the cache.", ae, store.cache.getIfPresent(ae.getEventKey()));

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

    AggregateEvent aeDeserialized = store.fromKeyValueGAE(new Slice(keyBytes), valueBytes);

    aeDeserialized.getKeys().setFieldDescriptor(fdKey);
    aeDeserialized.getAggregates().setFieldDescriptor(fdValue);
    Assert.assertEquals("The values must be equal", ae, aeDeserialized);
  }
}
