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

import com.datatorrent.lib.dimensions.OTFAggregator;
import com.datatorrent.lib.dimensions.AggregatorSum;
import com.datatorrent.lib.dimensions.AggregatorAverage;
import com.datatorrent.lib.dimensions.DimensionsIncrementalAggregator;
import com.datatorrent.lib.dimensions.AggregatorCount;
import com.datatorrent.lib.dimensions.AggregatorRegistry;
import com.datatorrent.lib.dimensions.AggregatorUtils;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class AggregatorInfoTest
{
  @Test
  public void serializationTest() throws Exception
  {
    TestUtils.clone(new Kryo(), AggregatorUtils.DEFAULT_AGGREGATOR_REGISTRY);
  }

  @Test
  public void metaDataTest()
  {
    Map<String, DimensionsAggregator> nameToAggregator = Maps.newHashMap();
    nameToAggregator.put("SUM", new AggregatorSum());
    nameToAggregator.put("COUNT", new AggregatorCount());
    nameToAggregator.put("AVG", new AggregatorAverage());

    Map<String, Integer> nameToID = Maps.newHashMap();
    nameToID.put("SUM", 0);
    nameToID.put("COUNT", 1);

    AggregatorRegistry aggInfo = AggregatorRegistryInfo(nameToAggregator,
                                                nameToID);

    aggInfo.setup();

    Map<Class<? extends DimensionsIncrementalAggregator>, String> classToStaticAggregator =
    aggInfo.getClassToIncrementalAggregatorName();

    Assert.assertEquals("Incorrect number of elements.", 2, classToStaticAggregator.size());
    Assert.assertEquals(classToStaticAggregator.get(AggregatorSum.class), "SUM");
    Assert.assertEquals(classToStaticAggregator.get(AggregatorCount.class), "COUNT");

    Map<String, DimensionsOTFAggregatorFAggregator = aggInfo.getNameToOTFAggregators();

    Assert.assertEquals(AggregatorAverage.class, nameToOTFAggregator.get("AVG").getClass());

    Map<String, List<String>> otfAggregatorToStaticAggregators = aggInfo.getOTFAggregatorToStaticAggregators();

    Assert.assertEquals("Only 1 OTF aggregator", 1, otfAggregatorToStaticAggregators.size());
    Assert.assertEquals(otfAggregatorToStaticAggregators.get("AVG"), Lists.newArrayList("SUM","COUNT"));

    Map<Integer, DimensionsIncrementalAggregator> staticAggregatorIDToAggregator = aggInfo.getIncrementalAggregatorIDToAggregator();

    Assert.assertEquals("Incorrect ID To Aggregator Mapping", AggregatorSum.class, staticAggregatorIDToAggregator.get(0).getClass());
    Assert.assertEquals("Incorrect ID To Aggregator Mapping", AggregatorCount.class, staticAggregatorIDToAggregator.get(1).getClass());

    Map<String, Integer> staticAggregatorNameToID = aggInfo.getStaticAggregatorNameToID();

    Assert.assertEquals(2, staticAggregatorNameToID.size());
    Assert.assertEquals((Integer) 0, staticAggregatorNameToID.get("SUM"));
    Assert.assertEquals((Integer) 1, staticAggregatorNameToID.get("COUNT"));

    Map<String, DimensionsIncrementalAggregator> staticAggregatorNameToStaticAggregator = aggInfo.getStaticAggregatorNameToStaticAggregator();

    Assert.assertEquals(AggregatorSum.class, staticAggregatorNameToStaticAggregator.get("SUM").getClass());
    Assert.assertEquals(AggregatorCount.class, staticAggregatorNameToStaticAggregator.get("COUNT").getClass());
  }
}
