/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.dimensions.aggregator;

import java.util.List;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.dimensions.aggregator.AggregatorAverage;
import com.datatorrent.lib.dimensions.aggregator.AggregatorCount;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.lib.dimensions.aggregator.AggregatorSum;
import com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator;
import com.datatorrent.lib.dimensions.aggregator.OTFAggregator;
import com.datatorrent.lib.util.TestUtils;

public class AggregatorRegistryTest
{
  @Test
  public void serializationTest() throws Exception
  {
    TestUtils.clone(new Kryo(), AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);
  }

  @Test
  public void metaDataTest()
  {
    Map<String, IncrementalAggregator> nameToIncrementalAggregator = Maps.newHashMap();
    nameToIncrementalAggregator.put("SUM", new AggregatorSum());
    nameToIncrementalAggregator.put("COUNT", new AggregatorCount());

    Map<String, OTFAggregator> nameToOTFAggregator = Maps.newHashMap();
    nameToOTFAggregator.put("AVG", AggregatorAverage.INSTANCE);

    Map<String, Integer> nameToID = Maps.newHashMap();
    nameToID.put("SUM", 0);
    nameToID.put("COUNT", 1);

    AggregatorRegistry aggregatorRegistry = new AggregatorRegistry(nameToIncrementalAggregator,
                                                                   nameToOTFAggregator,
                                                                   nameToID);

    aggregatorRegistry.setup();

    Map<Class<? extends IncrementalAggregator>, String> classToStaticAggregator =
    aggregatorRegistry.getClassToIncrementalAggregatorName();

    Assert.assertEquals("Incorrect number of elements.", 2, classToStaticAggregator.size());
    Assert.assertEquals(classToStaticAggregator.get(AggregatorSum.class), "SUM");
    Assert.assertEquals(classToStaticAggregator.get(AggregatorCount.class), "COUNT");

    Assert.assertEquals(AggregatorAverage.class, nameToOTFAggregator.get("AVG").getClass());

    Map<String, List<String>> otfAggregatorToStaticAggregators =
    aggregatorRegistry.getOTFAggregatorToIncrementalAggregators();

    Assert.assertEquals("Only 1 OTF aggregator", 1, otfAggregatorToStaticAggregators.size());
    Assert.assertEquals(otfAggregatorToStaticAggregators.get("AVG"), Lists.newArrayList("SUM","COUNT"));
  }
}
