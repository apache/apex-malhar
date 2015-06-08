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

package com.datatorrent.demos.dimensions.ads.custom;

import com.datatorrent.demos.dimensions.ads.AdInfo;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregateEvent;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoSumAggregator;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdsDimensionsCombination;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.DimensionsCombination;
import com.datatorrent.lib.dimensions.DimensionsComputationCustom;
import com.datatorrent.lib.dimensions.aggregator.Aggregator;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class AdsDimensionsComputationTest
{
  @Test
  public void simpleTest()
  {
    DimensionsComputationCustom<AdInfo, AdInfo.AdInfoAggregateEvent> dimensions = new DimensionsComputationCustom<AdInfo, AdInfo.AdInfoAggregateEvent>();

    String[] dimensionSpecs = new String[] {
      "time=" + TimeUnit.MINUTES,
      "time=" + TimeUnit.MINUTES + ":location",
      "time=" + TimeUnit.MINUTES + ":advertiser",
      "time=" + TimeUnit.MINUTES + ":publisher",
      "time=" + TimeUnit.MINUTES + ":advertiser:location",
      "time=" + TimeUnit.MINUTES + ":publisher:location",
      "time=" + TimeUnit.MINUTES + ":publisher:advertiser",
      "time=" + TimeUnit.MINUTES + ":publisher:advertiser:location"
    };


    //Set Dimensions properties
    LinkedHashMap<String, DimensionsCombination<AdInfo, AdInfo.AdInfoAggregateEvent>> dimensionsCombinations =
    Maps.newLinkedHashMap();

    LinkedHashMap<String, List<Aggregator<AdInfo, AdInfo.AdInfoAggregateEvent>>> dimensionsAggregators =
    Maps.newLinkedHashMap();

    for(int index = 0;
        index < dimensionSpecs.length;
        index++) {
      String dimensionSpec = dimensionSpecs[index];
      AdsDimensionsCombination dimensionsCombination = new AdsDimensionsCombination();
      dimensionsCombination.init(dimensionSpec, index);
      dimensionsCombinations.put(dimensionSpec, dimensionsCombination);

      List<Aggregator<AdInfo, AdInfo.AdInfoAggregateEvent>> aggregators = Lists.newArrayList();
      AdInfoSumAggregator adInfoSumAggregator = new AdInfoSumAggregator();
      aggregators.add(adInfoSumAggregator);
      dimensionsAggregators.put(dimensionSpec, aggregators);
    }

    dimensions.setDimensionsCombinations(dimensionsCombinations);
    dimensions.setAggregators(dimensionsAggregators);

    CollectorTestSink<AdInfoAggregateEvent> sink = new CollectorTestSink<AdInfoAggregateEvent>();
    TestUtils.setSink(dimensions.output, sink);

    AdInfo adInfo = new AdInfo("twitter",
                               "safeway",
                               "LREC",
                                1.0,
                                1.0,
                                1L,
                                1L,
                                100L);

    dimensions.setup(null);
    dimensions.beginWindow(0L);
    dimensions.data.put(adInfo);
    dimensions.endWindow();

    Assert.assertEquals(8, sink.collectedTuples.size());

    Set<Integer> dimensionDescriptorIDs = Sets.newHashSet();
    Set<Integer> expectedDimensionDescriptorIDs = Sets.newHashSet();

    int id = 0;
    for(AdInfoAggregateEvent aae: sink.collectedTuples) {
      dimensionDescriptorIDs.add(aae.getDimensionsDescriptorID());
      expectedDimensionDescriptorIDs.add(id);
      id++;
    }

    Assert.assertEquals(expectedDimensionDescriptorIDs, dimensionDescriptorIDs);
  }
}
