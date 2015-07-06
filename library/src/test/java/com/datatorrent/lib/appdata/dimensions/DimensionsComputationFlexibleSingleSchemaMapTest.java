/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.dimensions;

import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaMap;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.aggregator.AggregatorIncrementalType;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

public class DimensionsComputationFlexibleSingleSchemaMapTest
{
  @Before
  public void setup()
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
  }

  @Test
  public void simpleTest() throws Exception
  {
    Map<String, Object> ai1 = createAdInfoEvent1();
    Map<String, Object> ai2 = createAdInfoEvent2();

    int schemaID = AbstractDimensionsComputationFlexibleSingleSchema.DEFAULT_SCHEMA_ID;
    int dimensionsDescriptorID = 0;
    int aggregatorID = AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.
                       getIncrementalAggregatorNameToID().
                       get(AggregatorIncrementalType.SUM.name());

    String eventSchema = SchemaUtils.jarResourceFileToString("adsGenericEventSimple.json");
    DimensionalConfigurationSchema schema = new DimensionalConfigurationSchema(eventSchema,
                                                               AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    FieldsDescriptor keyFD = schema.getDimensionsDescriptorIDToKeyDescriptor().get(0);
    FieldsDescriptor valueFD = schema.getDimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor().get(0).get(aggregatorID);

    GPOMutable keyGPO = new GPOMutable(keyFD);
    keyGPO.setField("publisher", "google");
    keyGPO.setField(DimensionsDescriptor.DIMENSION_TIME,
                    TimeBucket.MINUTE.roundDown(300));
    keyGPO.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET,
                    TimeBucket.MINUTE.ordinal());

    EventKey eventKey = new EventKey(0,
                                     schemaID,
                                     dimensionsDescriptorID,
                                     aggregatorID,
                                     keyGPO);

    GPOMutable valueGPO = new GPOMutable(valueFD);
    valueGPO.setField("clicks", ((Long) ai1.get("clicks")) + ((Long) ai2.get("clicks")));
    valueGPO.setField("impressions", ((Long) ai1.get("impressions")) + ((Long) ai2.get("impressions")));
    valueGPO.setField("revenue", ((Double) ai1.get("revenue")) + ((Double) ai2.get("revenue")));
    valueGPO.setField("cost", ((Double) ai1.get("cost")) + ((Double) ai2.get("cost")));

    Aggregate expectedAE = new Aggregate(eventKey, valueGPO);

    DimensionsComputationFlexibleSingleSchemaMap dimensions = new DimensionsComputationFlexibleSingleSchemaMap();
    dimensions.setConfigurationSchemaJSON(SchemaUtils.jarResourceFileToString("adsGenericEventSimple.json"));

    CollectorTestSink<DimensionsEvent> sink = new CollectorTestSink<DimensionsEvent>();
    TestUtils.setSink(dimensions.output, sink);

    DimensionsComputationFlexibleSingleSchemaMap dimensionsClone =
    TestUtils.clone(new Kryo(), dimensions);

    dimensions.setup(null);

    dimensions.beginWindow(0L);
    dimensions.input.put(ai1);
    dimensions.input.put(ai2);
    dimensions.endWindow();

    LOG.debug("Expected aggregates: {}", expectedAE.getAggregates());
    LOG.debug("Actual aggregates  : {}", sink.collectedTuples.get(0).getAggregates());
    LOG.debug("Expected keys: {}", expectedAE.getKeys());
    LOG.debug("Actual keys  : {}", keyGPO);
    LOG.debug("expected: {} {} {}", schemaID, dimensionsDescriptorID, aggregatorID);
    LOG.debug("actual  : {} {} {}", sink.collectedTuples.get(0).getEventKey().getSchemaID(),
                                    sink.collectedTuples.get(0).getEventKey().getDimensionDescriptorID(),
                                    sink.collectedTuples.get(0).getEventKey().getAggregatorID());

    LOG.debug("{}", expectedAE.getAggregates().equals(sink.collectedTuples.get(0).getAggregates()));
    LOG.debug("{}", expectedAE.getEventKey().equals(sink.collectedTuples.get(0).getEventKey()));

    Assert.assertEquals("Expected only 1 tuple", 1, sink.collectedTuples.size());
    Assert.assertEquals(expectedAE, sink.collectedTuples.get(0));
    Assert.assertEquals(expectedAE.getAggregates(), sink.collectedTuples.get(0).getAggregates());
  }

  private Map<String, Object> createAdInfoEvent1()
  {
    Map<String, Object> ai = Maps.newHashMap();

    ai.put("publisher", "google");
    ai.put("advertiser", "starbucks");
    ai.put("location", "SKY");

    ai.put("clicks", 100L);
    ai.put("impressions", 1000L);
    ai.put("revenue", 10.0);
    ai.put("cost", 5.5);
    ai.put("time", 300L);

    return ai;
  }

  private Map<String, Object> createAdInfoEvent2()
  {
    Map<String, Object> ai = Maps.newHashMap();

    ai.put("publisher", "google");
    ai.put("advertiser", "starbucks");
    ai.put("location", "SKY");

    ai.put("clicks", 150L);
    ai.put("impressions", 100L);
    ai.put("revenue", 5.0);
    ai.put("cost", 3.50);
    ai.put("time", 300L);

    return ai;
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsComputationFlexibleSingleSchemaMapTest.class);
}
