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
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DimensionsPojoConverterTest
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionsPojoConverterTest.class);

  @Test
  public void simpleTest()
  {
    final int schemaID = 1;
    final int dimensionDescriptorID = 1;
    final int aggregatorID = 1;

    final String advertiser = "advertiser";
    final String publisher = "publisher";

    final String impressions = "impressions";
    final String revenue = "revenue";

    final String expectedAdvertiser = "starbucks";
    final String exprectedPublisher = "google";
    final long expectedImpressions = 100L;
    final double expectedRevenue = 53.0;

    DimensionsDescriptor dd = new DimensionsDescriptor(TimeBucket.MINUTE,
                                                        new Fields(Sets.newHashSet(advertiser,
                                                                                   publisher)));

    Map<String, Type> keyFTT = Maps.newHashMap();
    keyFTT.put(advertiser, Type.STRING);
    keyFTT.put(publisher, Type.STRING);

    FieldsDescriptor keyFD = new FieldsDescriptor(keyFTT);

    Map<String, Type> valueFTT = Maps.newHashMap();
    valueFTT.put(impressions, Type.LONG);
    valueFTT.put(revenue, Type.DOUBLE);

    FieldsDescriptor valueFD = new FieldsDescriptor(valueFTT);

    DimensionsConversionContext dcc = new DimensionsConversionContext();
    dcc.schemaID = schemaID;
    dcc.dimensionDescriptorID = dimensionDescriptorID;
    dcc.aggregatorID = aggregatorID;

    dcc.dd = dd;
    dcc.keyFieldsDescriptor = keyFD;
    dcc.aggregateDescriptor = valueFD;

    TestEvent event = new TestEvent();
    event.advertiser = expectedAdvertiser;
    event.publisher = exprectedPublisher;
    event.impressions = expectedImpressions;
    event.revenue = expectedRevenue;

    Map<String, Type> allFieldToType = Maps.newHashMap();
    allFieldToType.putAll(keyFTT);
    allFieldToType.putAll(valueFTT);

    logger.debug("{}", allFieldToType);

    Map<String, String> fieldToExpression = Maps.newHashMap();
    fieldToExpression.put(advertiser, "advertiser");
    fieldToExpression.put(publisher, "publisher");
    fieldToExpression.put(impressions, "impressions");
    fieldToExpression.put(revenue, "revenue");

    PojoFieldRetrieverExpression pfre = new PojoFieldRetrieverExpression();
    pfre.setFQClassName(TestEvent.class.getCanonicalName());
    pfre.setFieldToType(allFieldToType);
    pfre.setFieldToExpression(fieldToExpression);

    DimensionsPOJOConverter pojoConverter = new DimensionsPOJOConverter();
    pojoConverter.setPojoFieldRetriever(pfre);

    AggregateEvent ae = pojoConverter.convert(event, dcc);

    Assert.assertEquals("schemaIDs must equal", schemaID, ae.getEventKey().getSchemaID());
    Assert.assertEquals("dimension IDs must equal", dimensionDescriptorID, ae.getEventKey().getDimensionDescriptorID());
    Assert.assertEquals("aggregator IDs must equa", aggregatorID, ae.getEventKey().getAggregatorID());

    GPOMutable key = ae.getEventKey().getKey();
    GPOMutable agg = ae.getAggregates();

    Assert.assertEquals("advertisers must equal", expectedAdvertiser, key.getFieldString(advertiser));
    Assert.assertEquals("publisher must equal", exprectedPublisher, key.getFieldString(publisher));
    Assert.assertEquals("impressions must equal", expectedImpressions, agg.getFieldLong(impressions));
    Assert.assertEquals("revenue must equal", expectedRevenue, agg.getFieldDouble(revenue), 0.0);
  }

  public static class TestEvent
  {
    public String advertiser;
    public String publisher;
    public long impressions;
    public double revenue;
  }
}
