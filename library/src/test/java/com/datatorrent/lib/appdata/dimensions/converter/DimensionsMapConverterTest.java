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

package com.datatorrent.lib.appdata.dimensions.converter;

import com.datatorrent.lib.appdata.dimensions.AggregateEvent;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent.EventKey;
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DimensionsMapConverterTest
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionsMapConverterTest.class);

  @Test
  public void simpleConversionTest()
  {
    final Map<String, Type> fieldToTypeKey = Maps.newHashMap();

    fieldToTypeKey.put("advertiser", Type.STRING);
    fieldToTypeKey.put("publisher", Type.STRING);

    FieldsDescriptor keyFieldsDescriptor = new FieldsDescriptor(fieldToTypeKey);

    DimensionsDescriptor dd = new DimensionsDescriptor(TimeBucket.ALL,
                                                       new Fields(fieldToTypeKey.keySet()));

    final Map<String, Type> fieldToTypeAgg = Maps.newHashMap();

    fieldToTypeAgg.put("impressions", Type.LONG);
    fieldToTypeAgg.put("cost", Type.FLOAT);

    FieldsDescriptor aggFieldsDescriptor = new FieldsDescriptor(fieldToTypeAgg);

    DimensionsConversionContext context = new DimensionsConversionContext();

    context.schemaID = 0;
    context.dimensionDescriptorID = 0;
    context.aggregatorID = 0;
    context.dd = dd;
    context.keyFieldsDescriptor = keyFieldsDescriptor;
    context.aggregateDescriptor = aggFieldsDescriptor;

    DimensionsMapConverter mapConverter = new DimensionsMapConverter();

    Map<String, Object> inputMap = Maps.newHashMap();
    inputMap.put("advertiser", "Target");
    inputMap.put("publisher", "google");
    inputMap.put("impressions", 10L);
    inputMap.put("cost", 2.0f);

    AggregateEvent ae = mapConverter.convert(inputMap, context);
    EventKey eventKey = ae.getEventKey();

    GPOMutable gpoKey = eventKey.getKey();
    GPOMutable gpoAggregate = ae.getAggregates();

    Assert.assertEquals("schemaIDs should equal", 0, eventKey.getSchemaID());
    Assert.assertEquals("dimension descriptor IDs should equal", 0, eventKey.getDimensionDescriptorID());
    Assert.assertEquals("aggregatorIds should equal", 0, eventKey.getAggregatorID());
    Assert.assertEquals("key FieldsDescriptor equals", keyFieldsDescriptor, eventKey.getKey().getFieldDescriptor());
    Assert.assertEquals("key FieldsDescriptor equals", aggFieldsDescriptor, ae.getAggregates().getFieldDescriptor());

    Assert.assertEquals("Advertiser equals", "Target", gpoKey.getFieldString("advertiser"));
    Assert.assertEquals("Publisher equals", "google", gpoKey.getFieldString("publisher"));
    Assert.assertEquals("Impressions equals", 10L, gpoAggregate.getFieldLong("impressions"));
    Assert.assertEquals("Cost equals", (Float) 2.0f, (Float) gpoAggregate.getFieldFloat("cost"));
  }
}
