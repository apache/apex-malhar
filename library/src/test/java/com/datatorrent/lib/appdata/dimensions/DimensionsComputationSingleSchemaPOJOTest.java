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
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Maps;
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

    DimensionsComputationSingleSchemaPOJO dimensions = new DimensionsComputationSingleSchemaPOJO();

    String eventSchema = SchemaUtils.jarResourceFileToString("adsGenericEventSimple.json");

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

    dimensions = TestUtils.clone(new Kryo(), dimensions);

    dimensions.setup(null);

    dimensions.beginWindow(0L);
    dimensions.inputEvent.put(ai);
    dimensions.endWindow();
  }
}
