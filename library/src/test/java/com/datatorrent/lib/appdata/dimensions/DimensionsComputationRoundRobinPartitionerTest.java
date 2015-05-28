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

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.api.Partitioner.PartitioningContext;
import com.datatorrent.lib.appdata.dimensions.DimensionsComputationCustomTest.AdInfoResult;
import com.datatorrent.lib.dimensions.DimensionsComputationCustom;
import com.datatorrent.lib.dimensions.DimensionsComputationRoundRobinPartitioner;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.datatorrent.lib.appdata.dimensions.DimensionsComputationCustomTest.createDimensionsComputationComplex;

public class DimensionsComputationRoundRobinPartitionerTest
{
  @Test
  public void testSerialization() throws Exception
  {
    TestUtils.clone(new Kryo(),
                    new DimensionsComputationRoundRobinPartitioner<AdInfo, AdInfoResult, DimensionsComputationCustom<AdInfo, AdInfoResult>>());
  }

  @Test
  public void sameSizeTest()
  {
    Collection<Partition<DimensionsComputationCustom<AdInfo, AdInfoResult>>> collections =
    initializeOperators();

    DimensionsComputationRoundRobinPartitioner<AdInfo, AdInfoResult, DimensionsComputationCustom<AdInfo, AdInfoResult>> partitioner =
    new DimensionsComputationRoundRobinPartitioner<AdInfo, AdInfoResult, DimensionsComputationCustom<AdInfo, AdInfoResult>>();

    partitioner.setPartitionCount(3);
    Assert.assertEquals(collections.size(), partitioner.definePartitions(collections, new MockPartitioningContext()).size());
  }

  @Test
  public void smmallerSizeTest()
  {
    Collection<Partition<DimensionsComputationCustom<AdInfo, AdInfoResult>>> collections =
    initializeOperators();

    DimensionsComputationRoundRobinPartitioner<AdInfo, AdInfoResult, DimensionsComputationCustom<AdInfo, AdInfoResult>> partitioner =
    new DimensionsComputationRoundRobinPartitioner<AdInfo, AdInfoResult, DimensionsComputationCustom<AdInfo, AdInfoResult>>();

    partitioner.setPartitionCount(2);

    Collection<Partition<DimensionsComputationCustom<AdInfo, AdInfoResult>>> repartitionedCollections =
    partitioner.definePartitions(collections, new MockPartitioningContext());

    Assert.assertEquals(2, repartitionedCollections.size());

    Iterator<Partition<DimensionsComputationCustom<AdInfo, AdInfoResult>>> iterator = repartitionedCollections.iterator();
    DimensionsComputationCustom<AdInfo, AdInfoResult> dc1 = iterator.next().getPartitionedInstance();
    DimensionsComputationCustom<AdInfo, AdInfoResult> dc2 = iterator.next().getPartitionedInstance();

    AdInfoResult google1Max = new AdInfoResult("google",
                                               null,
                                               null,
                                               3.0,
                                               3.0,
                                               3L,
                                               3L,
                                               0);

    AdInfoResult google1Sum = new AdInfoResult("google",
                                               null,
                                               null,
                                               5.0,
                                               5.0,
                                               5L,
                                               5L,
                                               0);
    google1Sum.setAggregateIndex(1);

    AdInfoResult google2Max = new AdInfoResult("google",
                                               null,
                                               null,
                                               3.0,
                                               3.0,
                                               3L,
                                               3L,
                                               0);

    AdInfoResult google2Sum = new AdInfoResult("google",
                                               null,
                                               null,
                                               4.0,
                                               4.0,
                                               4L,
                                               4L,
                                               0);
    google2Sum.setAggregateIndex(1);

    AdInfoResult twitterMax = new AdInfoResult("twitter",
                                               null,
                                               null,
                                               1.0,
                                               1.0,
                                               1L,
                                               1L,
                                               0);

    AdInfoResult twitterSum = new AdInfoResult("twitter",
                                               null,
                                               null,
                                               1.0,
                                               1.0,
                                               1L,
                                               1L,
                                               0);
    twitterSum.setAggregateIndex(1);

    AdInfoResult facebookMax = new AdInfoResult("facebook",
                                                null,
                                                null,
                                                3.0,
                                                3.0,
                                                3L,
                                                3L,
                                                0);

    AdInfoResult facebookSum = new AdInfoResult("facebook",
                                                null,
                                                null,
                                                3.0,
                                                3.0,
                                                3L,
                                                3L,
                                                0);
    facebookSum.setAggregateIndex(1);


    Set<AdInfoResult> results1Max = Sets.newHashSet(google1Max,
                                                    twitterMax,
                                                    facebookMax);

    Set<AdInfoResult> results1Sum = Sets.newHashSet(google1Sum,
                                                    twitterSum,
                                                    facebookSum);

    Set<AdInfoResult> results2Max = Sets.newHashSet(google2Max,
                                                    facebookMax);

    Set<AdInfoResult> results2Sum = Sets.newHashSet(google2Sum,
                                                    facebookSum);

    Assert.assertEquals(results1Max, Sets.newHashSet(dc1.maps[0].values()));
    Assert.assertEquals(results1Sum, Sets.newHashSet(dc1.maps[1].values()));
    Assert.assertEquals(results2Max, Sets.newHashSet(dc2.maps[0].values()));
    Assert.assertEquals(results2Sum, Sets.newHashSet(dc2.maps[1].values()));
  }

  @Test
  public void largerSizeTest()
  {
    Collection<Partition<DimensionsComputationCustom<AdInfo, AdInfoResult>>> collections =
    initializeOperators();

    DimensionsComputationRoundRobinPartitioner<AdInfo, AdInfoResult, DimensionsComputationCustom<AdInfo, AdInfoResult>> partitioner =
    new DimensionsComputationRoundRobinPartitioner<AdInfo, AdInfoResult, DimensionsComputationCustom<AdInfo, AdInfoResult>>();

    partitioner.setPartitionCount(4);

    Collection<Partition<DimensionsComputationCustom<AdInfo, AdInfoResult>>> repartitionedCollections =
    partitioner.definePartitions(collections, new MockPartitioningContext());

    Assert.assertEquals(4, repartitionedCollections.size());

    Iterator<Partition<DimensionsComputationCustom<AdInfo, AdInfoResult>>> iterator = repartitionedCollections.iterator();
    DimensionsComputationCustom<AdInfo, AdInfoResult> dc1 = iterator.next().getPartitionedInstance();
    DimensionsComputationCustom<AdInfo, AdInfoResult> dc2 = iterator.next().getPartitionedInstance();
    DimensionsComputationCustom<AdInfo, AdInfoResult> dc3 = iterator.next().getPartitionedInstance();
    DimensionsComputationCustom<AdInfo, AdInfoResult> dc4 = iterator.next().getPartitionedInstance();

    checkSizes(dc1, dc2, dc3);

    Assert.assertEquals(null, (Object) dc4.maps);
  }

  private List<Partition<DimensionsComputationCustom<AdInfo, AdInfoResult>>> initializeOperators()
  {
    DimensionsComputationRoundRobinPartitioner<AdInfo, AdInfoResult, DimensionsComputationCustom<AdInfo, AdInfoResult>> partitioner =
    new DimensionsComputationRoundRobinPartitioner<AdInfo, AdInfoResult, DimensionsComputationCustom<AdInfo, AdInfoResult>>();

    DimensionsComputationCustom<AdInfo, AdInfoResult> dc1 =
    createDimensionsComputationComplex();

    DimensionsComputationCustom<AdInfo, AdInfoResult> dc2 =
    createDimensionsComputationComplex();

    DimensionsComputationCustom<AdInfo, AdInfoResult> dc3 =
    createDimensionsComputationComplex();

    AdInfo a = new AdInfo("google",
                          "safeway",
                          "panda",
                          1.0,
                          1.0,
                          1L,
                          1L,
                          100L);

    AdInfo a1 = new AdInfo("google",
                           "albertsons",
                           "bear",
                           3.0,
                           3.0,
                           3L,
                           3L,
                           100L);

    AdInfo a2 = new AdInfo("twitter",
                          "safeway",
                          "panda",
                          1.0,
                          1.0,
                          1L,
                          1L,
                          100L);

    AdInfo a3 = new AdInfo("facebook",
                           "safeway",
                           "bear",
                           3.0,
                           3.0,
                           3L,
                           3L,
                           100L);

    dc1.setup(null);
    dc1.beginWindow(0L);
    dc1.data.put(a);
    dc1.data.put(a1);
    dc1.data.put(a2);

    dc2.setup(null);
    dc2.beginWindow(0L);
    dc2.data.put(a);
    dc2.data.put(a1);
    dc2.data.put(a3);

    dc3.setup(null);
    dc3.beginWindow(0L);
    dc3.data.put(a);
    dc3.data.put(a3);

    checkSizes(dc1,
               dc2,
               dc3);

    List<Partition<DimensionsComputationCustom<AdInfo, AdInfoResult>>> collection = Lists.newArrayList();

    collection.add((Partition<DimensionsComputationCustom<AdInfo, AdInfoResult>>)
                   new DefaultPartition<DimensionsComputationCustom<AdInfo, AdInfoResult>>(dc1));
    collection.add((Partition<DimensionsComputationCustom<AdInfo, AdInfoResult>>)
                   new DefaultPartition<DimensionsComputationCustom<AdInfo, AdInfoResult>>(dc2));
    collection.add((Partition<DimensionsComputationCustom<AdInfo, AdInfoResult>>)
                   new DefaultPartition<DimensionsComputationCustom<AdInfo, AdInfoResult>>(dc3));

    return collection;
  }

  private void checkSizes(DimensionsComputationCustom<AdInfo, AdInfoResult> dc1,
                          DimensionsComputationCustom<AdInfo, AdInfoResult> dc2,
                          DimensionsComputationCustom<AdInfo, AdInfoResult> dc3)
  {
    Assert.assertEquals(2, dc1.maps[0].size());
    Assert.assertEquals(2, dc1.maps[1].size());
    Assert.assertEquals(3, dc1.maps[2].size());
    Assert.assertEquals(3, dc1.maps[3].size());
    Assert.assertEquals(3, dc1.maps[4].size());

    Assert.assertEquals(2, dc2.maps[0].size());
    Assert.assertEquals(2, dc2.maps[1].size());
    Assert.assertEquals(3, dc2.maps[2].size());
    Assert.assertEquals(3, dc2.maps[3].size());
    Assert.assertEquals(3, dc2.maps[4].size());

    Assert.assertEquals(2, dc3.maps[0].size());
    Assert.assertEquals(2, dc3.maps[1].size());
    Assert.assertEquals(2, dc3.maps[2].size());
    Assert.assertEquals(2, dc3.maps[3].size());
    Assert.assertEquals(2, dc3.maps[4].size());
  }

  public static class MockPartitioningContext implements PartitioningContext
  {
    public MockPartitioningContext()
    {
    }

    @Override
    public int getParallelPartitionCount()
    {
      return 0;
    }

    @Override
    public List<InputPort<?>> getInputPorts()
    {
      return null;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsComputationRoundRobinPartitionerTest.class);
}
