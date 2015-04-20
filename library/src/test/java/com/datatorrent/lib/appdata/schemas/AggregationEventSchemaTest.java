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

package com.datatorrent.lib.appdata.schemas;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

public class AggregationEventSchemaTest
{
  @Test
  public void simpleAggregationEventSchemaTest()
  {
    final Set<String> expectedAggs1 = ImmutableSet.of("SUM", "MIN");
    final Set<String> expectedAggs2 = ImmutableSet.of("SUM", "MAX");

    final Map<String, Type> fieldToType = Maps.newHashMap();
    fieldToType.put("valueName1", Type.FLOAT);
    fieldToType.put("valueName2", Type.LONG);
    final FieldsDescriptor fd = new FieldsDescriptor(fieldToType);
    final Set<TimeBucket> timeBucketExpected = Sets.newHashSet(TimeBucket.MINUTE, TimeBucket.HOUR, TimeBucket.DAY);

    String eventSchemaString = "{\"timeBuckets\":[\"1m\",\"1h\",\"1d\"],\n" +
                               "\"values\":\n" +
                               " [{\"name\":\"valueName1\",\"type\":\"float\",\"aggregators\":[\"SUM\",\"MIN\"]},\n" +
                               "  {\"name\":\"valueName2\",\"type\":\"long\",\"aggregators\":[\"SUM\",\"MAX\"]}]\n" +
                               "}";

    AggregationEventSchema eventSchema = new AggregationEventSchema(eventSchemaString);

    Set<TimeBucket> timeBuckets = eventSchema.getTimeBuckets();
    Assert.assertEquals("The timebuckets should equal.", timeBucketExpected, timeBuckets);

    Set<String> aggs1 = eventSchema.getAllValueToAggregator().get("valueName1");
    Assert.assertEquals("The aggregators should equal.", expectedAggs1, aggs1);

    Set<String> aggs2 = eventSchema.getAllValueToAggregator().get("valueName2");
    Assert.assertEquals("The aggregators should equal.", expectedAggs2, aggs2);

    Assert.assertEquals("The field descriptors should equal.", eventSchema.getInputValuesDescriptor(), fd);
  }
}
