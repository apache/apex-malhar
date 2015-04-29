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
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
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
  public static final String ADVERTISER = "advertiser";
  public static final String PUBLISHER = "publisher";
  public static final String LOCATION = "location";

  public static final String IMPRESSIONS = "impressions";
  public static final String CLICKS = "clicks";
  public static final String REVENUE = "revenue";
  public static final String COST = "cost";

  private static final Logger logger = LoggerFactory.getLogger(DimensionsPojoConverterTest.class);

  @Test
  public void simpleTest()
  {
    final int schemaID = 1;
    final int dimensionDescriptorID = 1;
    final int aggregatorID = 1;

    final String inputLocation = "SKY";
    final long inputTimestamp = 1430093437546L;

    final String expectedAdvertiser = "starbucks";
    final String exprectedPublisher = "google";
    final long expectedImpressions = 100L;
    final long expectedClicks = 250L;
    final double expectedRevenue = 53.0;
    final double expectedCost = 3.50;
    final long expectedTimestamp = TimeBucket.MINUTE.roundDown(inputTimestamp);
    final int expectedTimeBucket = TimeBucket.MINUTE.ordinal();

    DimensionsDescriptor dd = new DimensionsDescriptor(TimeBucket.MINUTE,
                                                        new Fields(Sets.newHashSet(ADVERTISER,
                                                                                   PUBLISHER)));

    Map<String, Type> keyFTT = Maps.newHashMap();
    keyFTT.put("advertiser", Type.STRING);
    keyFTT.put("publisher", Type.STRING);
    keyFTT.put(DimensionsDescriptor.DIMENSION_TIME, DimensionsDescriptor.DIMENSION_TIME_TYPE);
    keyFTT.put(DimensionsDescriptor.DIMENSION_TIME_BUCKET, DimensionsDescriptor.DIMENSION_TIME_BUCKET_TYPE);

    FieldsDescriptor keyFD = new FieldsDescriptor(keyFTT);

    Map<String, Type> valueFTT = Maps.newHashMap();
    valueFTT.put("impressions", Type.LONG);
    valueFTT.put("clicks", Type.LONG);
    valueFTT.put("revenue", Type.DOUBLE);
    valueFTT.put("cost", Type.DOUBLE);

    FieldsDescriptor valueFD = new FieldsDescriptor(valueFTT);

    DimensionsConversionContext dcc = new DimensionsConversionContext();
    dcc.schemaID = schemaID;
    dcc.dimensionDescriptorID = dimensionDescriptorID;
    dcc.aggregatorID = aggregatorID;

    dcc.dd = dd;
    dcc.keyFieldsDescriptor = keyFD;
    dcc.aggregateDescriptor = valueFD;

    AdInfo event = new AdInfo();
    event.setAdvertiser(expectedAdvertiser);
    event.setPublisher(exprectedPublisher);
    event.setLocation(inputLocation);
    event.setImpressions(expectedImpressions);
    event.setRevenue(expectedRevenue);
    event.setCost(expectedCost);
    event.setClicks(expectedClicks);
    event.setTime(inputTimestamp);

    Map<String, Type> allFieldToType = Maps.newHashMap();
    allFieldToType.putAll(keyFTT);
    allFieldToType.putAll(valueFTT);
    allFieldToType.remove(DimensionsDescriptor.DIMENSION_TIME_BUCKET);

    logger.debug("{}", allFieldToType);

    Map<String, String> fieldToExpression = Maps.newHashMap();
    fieldToExpression.put(ADVERTISER, "getAdvertiser()");
    fieldToExpression.put(PUBLISHER, "getPublisher()");
    fieldToExpression.put(LOCATION, "getLocation()");

    fieldToExpression.put(IMPRESSIONS, "getImpressions()");
    fieldToExpression.put(REVENUE, "getRevenue()");
    fieldToExpression.put(CLICKS, "getClicks()");
    fieldToExpression.put(COST, "getCost()");
    fieldToExpression.put(DimensionsDescriptor.DIMENSION_TIME, "getTime()");

    PojoFieldRetrieverExpression pfre = new PojoFieldRetrieverExpression();
    pfre.setFQClassName(AdInfo.class.getCanonicalName());
    pfre.setFieldToType(SchemaUtils.convertFieldToType(allFieldToType));
    pfre.setFieldToExpression(fieldToExpression);
    pfre.setup();

    DimensionsPOJOConverter pojoConverter = new DimensionsPOJOConverter();
    pojoConverter.setPojoFieldRetriever(pfre);

    AggregateEvent ae = pojoConverter.convert(event, dcc);

    Assert.assertEquals("schemaIDs must equal", schemaID, ae.getEventKey().getSchemaID());
    Assert.assertEquals("dimension IDs must equal", dimensionDescriptorID, ae.getEventKey().getDimensionDescriptorID());
    Assert.assertEquals("aggregator IDs must equa", aggregatorID, ae.getEventKey().getAggregatorID());

    GPOMutable key = ae.getEventKey().getKey();
    GPOMutable agg = ae.getAggregates();

    Assert.assertEquals("advertisers must equal", expectedAdvertiser, key.getFieldString(ADVERTISER));
    Assert.assertEquals("publisher must equal", exprectedPublisher, key.getFieldString(PUBLISHER));
    Assert.assertEquals("time must equal", expectedTimestamp, key.getFieldLong(DimensionsDescriptor.DIMENSION_TIME));
    Assert.assertEquals("time must equal", expectedTimeBucket, key.getFieldInt(DimensionsDescriptor.DIMENSION_TIME_BUCKET));
    Assert.assertEquals("bucket must equal", exprectedPublisher, key.getFieldString(PUBLISHER));
    Assert.assertEquals("impressions must equal", expectedImpressions, agg.getFieldLong(IMPRESSIONS));
    Assert.assertEquals("revenue must equal", expectedRevenue, agg.getFieldDouble(REVENUE), 0.0);

  }
}
