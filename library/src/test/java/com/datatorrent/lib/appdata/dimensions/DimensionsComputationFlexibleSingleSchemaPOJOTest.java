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

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema;
import com.datatorrent.lib.dimensions.aggregator.AggregatorIncrementalType;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DimensionsComputationFlexibleSingleSchemaPOJOTest
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionsComputationFlexibleSingleSchemaPOJOTest.class);

  @Before
  public void setup()
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
  }

  @Test
  public void simpleTest() throws Exception
  {
    AdInfo ai = createTestAdInfoEvent1();
    AdInfo ai2 = createTestAdInfoEvent2();

    int schemaID = AbstractDimensionsComputationFlexibleSingleSchema.DEFAULT_SCHEMA_ID;
    int dimensionsDescriptorID = 0;
    int aggregatorID = AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.
                       getIncrementalAggregatorNameToID().
                       get(AggregatorIncrementalType.SUM.name());

    String eventSchema = SchemaUtils.jarResourceFileToString("adsGenericEventSimple.json");
    DimensionalConfigurationSchema schema = new DimensionalConfigurationSchema(eventSchema,
                                                               AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    FieldsDescriptor keyFD = schema.getDdIDToKeyDescriptor().get(0);
    FieldsDescriptor valueFD = schema.getDdIDToAggIDToInputAggDescriptor().get(0).get(aggregatorID);

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
    valueGPO.setField("clicks", ai.getClicks() + ai2.getClicks());
    valueGPO.setField("impressions", ai.getImpressions() + ai2.getImpressions());
    valueGPO.setField("revenue", ai.getRevenue() + ai2.getRevenue());
    valueGPO.setField("cost", ai.getCost() + ai2.getCost());

    Aggregate expectedAE = new Aggregate(eventKey, valueGPO);

    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = createDimensionsComputationOperator("adsGenericEventSimple.json");

    CollectorTestSink<DimensionsEvent> sink = new CollectorTestSink<DimensionsEvent>();
    TestUtils.setSink(dimensions.output, sink);

    DimensionsComputationFlexibleSingleSchemaPOJO dimensionsClone =
    TestUtils.clone(new Kryo(), dimensions);

    dimensions.setup(null);

    dimensions.beginWindow(0L);
    dimensions.inputEvent.put(ai);
    dimensions.inputEvent.put(ai2);
    dimensions.endWindow();

    Assert.assertEquals("Expected only 1 tuple", 1, sink.collectedTuples.size());
    Assert.assertEquals(expectedAE, sink.collectedTuples.get(0));
  }

  @Test
  public void complexOutputTest()
  {
    AdInfo ai = createTestAdInfoEvent1();

    DimensionsComputationFlexibleSingleSchemaPOJO dcss = createDimensionsComputationOperator("adsGenericEventSchemaAdditional.json");

    CollectorTestSink<DimensionsEvent> sink = new CollectorTestSink<DimensionsEvent>();
    TestUtils.setSink(dcss.output, sink);

    dcss.setup(null);
    dcss.beginWindow(0L);
    dcss.inputEvent.put(ai);
    dcss.endWindow();

    Assert.assertEquals(60, sink.collectedTuples.size());
  }

  @Test
  public void aggregationsTest()
  {
    AdInfo ai = createTestAdInfoEvent1();

    DimensionsComputationFlexibleSingleSchemaPOJO dcss = createDimensionsComputationOperator("adsGenericEventSchemaAggregations.json");

    CollectorTestSink<DimensionsEvent> sink = new CollectorTestSink<DimensionsEvent>();
    TestUtils.setSink(dcss.output, sink);

    dcss.setup(null);
    dcss.beginWindow(0L);
    dcss.inputEvent.put(ai);
    dcss.endWindow();

    Assert.assertEquals(6, sink.collectedTuples.size());
  }

  public static DimensionsComputationFlexibleSingleSchemaPOJO createDimensionsComputationOperator(String eventSchema)
  {
    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = new DimensionsComputationFlexibleSingleSchemaPOJO();
    dimensions.setEventSchemaJSON(SchemaUtils.jarResourceFileToString(eventSchema));

    Map<String, String> fieldToExpressionKey = Maps.newHashMap();
    fieldToExpressionKey.put("publisher", "getPublisher()");
    fieldToExpressionKey.put("advertiser", "getAdvertiser()");
    fieldToExpressionKey.put("location", "getLocation()");
    fieldToExpressionKey.put("time", "getTime()");

    dimensions.setKeyToExpression(fieldToExpressionKey);

    Map<String, String> fieldToExpressionAggregate = Maps.newHashMap();
    fieldToExpressionAggregate.put("cost", "getCost()");
    fieldToExpressionAggregate.put("revenue", "getRevenue()");
    fieldToExpressionAggregate.put("impressions", "getImpressions()");
    fieldToExpressionAggregate.put("clicks", "getClicks()");

    dimensions.setAggregateToExpression(fieldToExpressionAggregate);

    return dimensions;
  }

  private AdInfo createTestAdInfoEvent1()
  {
    AdInfo ai = new AdInfo();
    ai.setPublisher("google");
    ai.setAdvertiser("starbucks");
    ai.setLocation("SKY");

    ai.setClicks(100L);
    ai.setImpressions(1000L);
    ai.setRevenue(10.0);
    ai.setCost(5.5);
    ai.setTime(300L);

    return ai;
  }

  private AdInfo createTestAdInfoEvent2()
  {
    AdInfo ai2 = new AdInfo();
    ai2.setPublisher("google");
    ai2.setAdvertiser("starbucks");
    ai2.setLocation("SKY");

    ai2.setClicks(150L);
    ai2.setImpressions(100L);
    ai2.setRevenue(5.0);
    ai2.setCost(3.50);
    ai2.setTime(300L);

    return ai2;
  }
}
