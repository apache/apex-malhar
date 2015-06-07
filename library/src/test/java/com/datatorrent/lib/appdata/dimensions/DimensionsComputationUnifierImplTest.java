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
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.aggregator.Aggregator;
import com.datatorrent.lib.dimensions.aggregator.AggregatorCount;
import com.datatorrent.lib.dimensions.aggregator.AggregatorIncrementalType;
import com.datatorrent.lib.dimensions.DimensionsComputationUnifierImpl;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate.AggregateHashingStrategy;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.lib.statistics.DimensionsComputation.AggregateEvent;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class DimensionsComputationUnifierImplTest
{
  @Test
  public void simpleTest() throws Exception
  {
    final int bucketID = 0;
    final int schemaID = 0;
    final int ddID = 0;
    final int aggregatorID =
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.getIncrementalAggregatorNameToID().get(AggregatorIncrementalType.SUM.name());

    Map<String, Type> fieldToTypeKey = Maps.newHashMap();
    fieldToTypeKey.put("publisher", Type.STRING);
    FieldsDescriptor fdKey = new FieldsDescriptor(fieldToTypeKey);

    Map<String, Type> fieldToTypeAgg = Maps.newHashMap();
    fieldToTypeAgg.put("count", Type.LONG);
    FieldsDescriptor fdAgg = new FieldsDescriptor(fieldToTypeAgg);

    GPOMutable key = new GPOMutable(fdKey);
    key.setField("publisher", "google");

    GPOMutable value = new GPOMutable(fdAgg);
    value.setField("count", 5L);

    GPOMutable value1 = new GPOMutable(fdAgg);
    value1.setField("count", 6L);

    GPOMutable expectedVal = new GPOMutable(fdAgg);
    expectedVal.setField("count", 11L);

    DimensionsComputationUnifierImpl<InputEvent, Aggregate> unifier =
    new DimensionsComputationUnifierImpl<InputEvent, Aggregate>();

    @SuppressWarnings({"unchecked", "rawtypes"})
    Aggregator<InputEvent,Aggregate>[] aggregators = new Aggregator[1];
    aggregators[0] = AggregatorCount.INSTANCE;
    unifier.setAggregators(aggregators);
    unifier.setHashingStrategy(new AggregateHashingStrategy());

    unifier.setup(null);

    Aggregate aeA = new Aggregate(key,
                                  value,
                                  bucketID,
                                  schemaID,
                                  ddID,
                                  aggregatorID);
    aeA.setAggregateIndex(0);

    Aggregate aeB = new Aggregate(key,
                                  value1,
                                  bucketID,
                                  schemaID,
                                  ddID,
                                  aggregatorID);
    aeB.setAggregateIndex(0);

    Aggregate expected = new Aggregate(key,
                                       expectedVal,
                                       bucketID,
                                       schemaID,
                                       ddID,
                                       aggregatorID);
    expected.setAggregateIndex(0);

    CollectorTestSink<AggregateEvent> sink = new CollectorTestSink<AggregateEvent>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tempSink = (CollectorTestSink) sink;

    unifier.output.setSink(tempSink);
    unifier.setup(null);

    unifier.beginWindow(1L);
    unifier.process(aeA);
    unifier.process(aeB);

    //Test serialization
    TestUtils.clone(new Kryo(), unifier);

    unifier.endWindow();

    Assert.assertEquals("The number of collected tuple is 1.", 1, sink.collectedTuples.size());
    Assert.assertEquals("The aggregate events should equal", expected, (Aggregate) sink.collectedTuples.get(0));
  }
}
