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

import com.datatorrent.lib.appbuilder.convert.pojo.PojoFieldRetrieverExpression;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent.EventKey;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DimensionalEventSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DimensionsComputationSingleSchemaPOJOTest
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionsComputationSingleSchemaPOJOTest.class);

  @Test
  public void simpleTest() throws Exception
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

    AdInfo ai2 = new AdInfo();
    ai2.setPublisher("google");
    ai2.setAdvertiser("starbucks");
    ai2.setLocation("SKY");

    ai2.setClicks(150L);
    ai2.setImpressions(100L);
    ai2.setRevenue(5.0);
    ai2.setCost(3.50);
    ai.setTime(300L);

    int schemaID = 0;
    int dimensionsDescriptorID = 0;
    int aggregatorID = AggregatorUtils.DEFAULT_AGGREGATOR_INFO.
                       getStaticAggregatorNameToID().
                       get(AggregatorStaticType.SUM.name());

    String eventSchema = SchemaUtils.jarResourceFileToString("adsGenericEventSimple.json");
    DimensionalEventSchema schema = new DimensionalEventSchema(eventSchema,
                                                               AggregatorUtils.DEFAULT_AGGREGATOR_INFO);

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

    AggregateEvent expectedAE = new AggregateEvent(eventKey, valueGPO);

    DimensionsComputationSingleSchemaPOJO dimensions = new DimensionsComputationSingleSchemaPOJO();

    dimensions.setEventSchemaJSON(eventSchema);

    PojoFieldRetrieverExpression pfre = new PojoFieldRetrieverExpression();
    pfre.setFQClassName(AdInfo.class.getName());
    Map<String, String> fieldToExpression = Maps.newHashMap();
    fieldToExpression.put("publisher", "getPublisher()");
    fieldToExpression.put("advertiser", "getAdvertiser()");
    fieldToExpression.put("location", "getLocation()");
    fieldToExpression.put("cost", "getCost()");
    fieldToExpression.put("revenue", "getRevenue()");
    fieldToExpression.put("impressions", "getImpressions()");
    fieldToExpression.put("clicks", "getClicks()");
    fieldToExpression.put("time", "getTime()");
    pfre.setFieldToExpression(fieldToExpression);
    dimensions.getConverter().setPojoFieldRetriever(pfre);

    CollectorTestSink<AggregateEvent> sink = new CollectorTestSink<AggregateEvent>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sinkObj = (CollectorTestSink) sink;

    DimensionsComputationSingleSchemaPOJO dimensionsClone =
    TestUtils.clone(new Kryo(), dimensions);

    dimensions.aggregateOutput.setSink(sinkObj);

    dimensions.setup(null);

    dimensions.beginWindow(0L);
    dimensions.inputEvent.put(ai);
    dimensions.inputEvent.put(ai2);
    dimensions.endWindow();

    Assert.assertEquals("Expected only 1 tuple", 1, sinkObj.collectedTuples.size());
    Assert.assertEquals(expectedAE, sinkObj.collectedTuples.get(0));

    sinkObj.collectedTuples.clear();

    //Multi Window Test

    dimensionsClone.setAggregationWindowCount(2);
    dimensionsClone.aggregateOutput.setSink(sinkObj);

    dimensionsClone.beginWindow(0L);
    dimensionsClone.inputEvent.put(ai);
    dimensionsClone.endWindow();

    Assert.assertEquals("Expected no tuples", 0, sinkObj.collectedTuples.size());

    dimensionsClone.beginWindow(1L);
    dimensionsClone.inputEvent.put(ai2);
    dimensionsClone.endWindow();

    Assert.assertEquals("Expected only 1 tuple", 1, sinkObj.collectedTuples.size());
    Assert.assertEquals(expectedAE, sinkObj.collectedTuples.get(0));
  }
}
