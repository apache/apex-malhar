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

import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.DimensionsCombination;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.UnifiableAggregate;
import com.datatorrent.lib.dimensions.DimensionsComputationCustom;
import com.datatorrent.lib.dimensions.DimensionsComputationUnifierImpl;
import com.datatorrent.lib.dimensions.aggregator.Aggregator;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.Serializable;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public class DimensionsComputationCustomTest
{
  @Test
  public void testConfiguringUnifier()
  {
    DimensionsComputationCustom<AdInfo, AdInfoResult> dimensions = createDimensionsComputationComplex();
    dimensions.setUnifier(new DimensionsComputationUnifierImpl<AdInfo, AdInfoResult>());
    Aggregator<AdInfo, AdInfoResult>[] aggregators = dimensions.configureDimensionsComputationUnifier();

    Assert.assertEquals(AdInfoMaxAggregator.class, aggregators[0].getClass());
    Assert.assertEquals(AdInfoSumAggregator.class, aggregators[1].getClass());
    Assert.assertEquals(AdInfoMaxAggregator.class, aggregators[2].getClass());
    Assert.assertEquals(AdInfoMinAggregator.class, aggregators[3].getClass());
    Assert.assertEquals(AdInfoCountAggregator.class, aggregators[4].getClass());
    Assert.assertEquals(5, aggregators.length);
  }

  @Test
  public void serializationTest() throws Exception
  {
    DimensionsComputationCustom<AdInfo, AdInfoResult> dimensionsComputation = createSimpleDimensionsComputation();

    AdInfo a = new AdInfo("google",
                          "safeway",
                          "panda",
                          1.0,
                          1.0,
                          1L,
                          1L,
                          100L);

    AdInfo b = new AdInfo("google",
                          "albertsons",
                          "penguin",
                          2.0,
                          2.0,
                          2L,
                          2L,
                          100L);

    dimensionsComputation.setup(null);

    LOG.debug("{}", dimensionsComputation.maps[0].getAggregator());

    dimensionsComputation = TestUtils.clone(new Kryo(), dimensionsComputation);
    LOG.debug("{}", dimensionsComputation.maps[0].getAggregator());

    dimensionsComputation.beginWindow(0L);
    dimensionsComputation.data.put(a);

    Assert.assertEquals(1, dimensionsComputation.maps[0].size());
    dimensionsComputation = TestUtils.clone(new Kryo(), dimensionsComputation);
    Assert.assertEquals(1, dimensionsComputation.maps[0].size());

    dimensionsComputation.data.put(b);

    Assert.assertEquals(1, dimensionsComputation.maps[0].size());

    dimensionsComputation.endWindow();

    Assert.assertEquals(0, dimensionsComputation.maps[0].size());
  }

  @Test
  public void singleAggregatorTest()
  {
    DimensionsComputationCustom<AdInfo, AdInfoResult> dimensionsComputation = createSimpleDimensionsComputation();

    AdInfo a = new AdInfo("google",
                          "safeway",
                          "panda",
                          1.0,
                          1.0,
                          1L,
                          1L,
                          100L);

    AdInfo b = new AdInfo("google",
                          "albertsons",
                          "penguin",
                          2.0,
                          2.0,
                          2L,
                          2L,
                          100L);

    AdInfoResult expected = new AdInfoResult("google",
                                             null,
                                             null,
                                             3.0,
                                             3.0,
                                             3L,
                                             3L,
                                             0L);
    expected.setAggregateIndex(0);

    CollectorTestSink<AdInfoResult> sink = new CollectorTestSink<AdInfoResult>();
    TestUtils.setSink(dimensionsComputation.output, sink);

    dimensionsComputation.setup(null);

    dimensionsComputation.beginWindow(0L);
    dimensionsComputation.data.put(a);
    dimensionsComputation.data.put(b);
    dimensionsComputation.endWindow();

    Assert.assertEquals(1, sink.collectedTuples.size());
    Assert.assertEquals(expected, sink.collectedTuples.get(0));
  }

  @Test
  public void multipleAggregatorTest() throws Exception
  {
    DimensionsComputationCustom<AdInfo, AdInfoResult> dimensionsComputation = createDimensionsComputationComplex();
    dimensionsComputation.setup(null);

    CollectorTestSink<AdInfoResult> sink = new CollectorTestSink<AdInfoResult>();
    TestUtils.setSink(dimensionsComputation.output, sink);

    AdInfo a = new AdInfo("google",
                          "safeway",
                          "panda",
                          1.0,
                          1.0,
                          1L,
                          1L,
                          100L);

    AdInfo a1 = new AdInfo("google",
                           "safeway",
                           "bear",
                           3.0,
                           3.0,
                           3L,
                           3L,
                           100L);

    AdInfo b = new AdInfo("google",
                          "albertsons",
                          "penguin",
                          2.0,
                          2.0,
                          2L,
                          2L,
                          100L);

    AdInfo b1 = new AdInfo("google",
                           "albertsons",
                           "penguin",
                           3.0,
                           3.0,
                           3L,
                           3L,
                           100L);


    AdInfoResult publisherGoogleMax =
    new AdInfoResult("google",
                     null,
                     null,
                     3.0,
                     3.0,
                     3L,
                     3L,
                     0L);
    publisherGoogleMax.setAggregateIndex(0);

    AdInfoResult publisherGoogleSum =
    new AdInfoResult("google",
                     null,
                     null,
                     9.0,
                     9.0,
                     9L,
                     9L,
                     0L);
    publisherGoogleSum.setAggregateIndex(1);

    AdInfoResult publisherAdvertiserGoogleSafewayMax =
    new AdInfoResult("google",
                     "safeway",
                     null,
                     3.0,
                     3.0,
                     3L,
                     3L,
                     0L);
    publisherAdvertiserGoogleSafewayMax.setAggregateIndex(2);

    AdInfoResult publisherAdvertiserGoogleSafewayMin =
    new AdInfoResult("google",
                     "safeway",
                     null,
                     1.0,
                     1.0,
                     1L,
                     1L,
                     0L);
    publisherAdvertiserGoogleSafewayMin.setAggregateIndex(3);

    AdInfoResult publisherAdvertiserGoogleSafewayCount =
    new AdInfoResult("google",
                     "safeway",
                     null,
                     0.0,
                     0.0,
                     0L,
                     0L,
                     0L);
    publisherAdvertiserGoogleSafewayCount.setCount(2);
    publisherAdvertiserGoogleSafewayCount.setAggregateIndex(4);

    AdInfoResult publisherAdvertiserGoogleAlbertsonsMax =
    new AdInfoResult("google",
                     "albertsons",
                     null,
                     3.0,
                     3.0,
                     3L,
                     3L,
                     0L);
    publisherAdvertiserGoogleAlbertsonsMax.setAggregateIndex(2);

    AdInfoResult publisherAdvertiserGoogleAlbertsonsMin =
    new AdInfoResult("google",
                     "albertsons",
                     null,
                     2.0,
                     2.0,
                     2L,
                     2L,
                     0L);
    publisherAdvertiserGoogleAlbertsonsMin.setAggregateIndex(3);

    AdInfoResult publisherAdvertiserGoogleAlbertsonsCount =
    new AdInfoResult("google",
                     "albertsons",
                     null,
                     0.0,
                     0.0,
                     0L,
                     0L,
                     0L);
    publisherAdvertiserGoogleAlbertsonsCount.setCount(2);
    publisherAdvertiserGoogleAlbertsonsCount.setAggregateIndex(4);

    dimensionsComputation.beginWindow(0L);
    dimensionsComputation.data.put(a);
    dimensionsComputation.data.put(a1);
    dimensionsComputation.data.put(b);
    dimensionsComputation.data.put(b1);

    TestUtils.clone(new Kryo(), dimensionsComputation);

    dimensionsComputation.endWindow();

    Assert.assertEquals(8, sink.collectedTuples.size());

    Set<AdInfoResult> results = Sets.newHashSet(publisherGoogleMax,
                                                publisherGoogleSum,
                                                publisherAdvertiserGoogleSafewayMax,
                                                publisherAdvertiserGoogleSafewayMin,
                                                publisherAdvertiserGoogleSafewayCount,
                                                publisherAdvertiserGoogleAlbertsonsMax,
                                                publisherAdvertiserGoogleAlbertsonsMin,
                                                publisherAdvertiserGoogleAlbertsonsCount);

    Assert.assertEquals(results, Sets.newHashSet(sink.collectedTuples));
  }

  public static DimensionsComputationCustom<AdInfo, AdInfoResult> createSimpleDimensionsComputation()
  {
    DimensionsComputationCustom<AdInfo, AdInfoResult> dimensionsComputation =
    new DimensionsComputationCustom<AdInfo, AdInfoResult>();

    List<Aggregator<AdInfo, AdInfoResult>> aggregatorsSum = Lists.newArrayList();
    aggregatorsSum.add(new AdInfoSumAggregator());

    LinkedHashMap<String, DimensionsCombination<AdInfo, AdInfoResult>> dimensionsCombinations =
    Maps.newLinkedHashMap();
    dimensionsCombinations.put("publisher", new PublisherCombination());

    LinkedHashMap<String, List<Aggregator<AdInfo, AdInfoResult>>> aggList =
    Maps.newLinkedHashMap();
    aggList.put("publisher", aggregatorsSum);

    dimensionsComputation.setDimensionsCombinations(dimensionsCombinations);
    dimensionsComputation.setAggregators(aggList);

    return dimensionsComputation;
  }

  public static DimensionsComputationCustom<AdInfo, AdInfoResult> createDimensionsComputationComplex()
  {
    DimensionsComputationCustom<AdInfo, AdInfoResult> dimensionsComputation =
    new DimensionsComputationCustom<AdInfo, AdInfoResult>();

    List<Aggregator<AdInfo, AdInfoResult>> aggregatorsSum = Lists.newArrayList();
    aggregatorsSum.add(new AdInfoMaxAggregator());
    aggregatorsSum.add(new AdInfoSumAggregator());
    List<Aggregator<AdInfo, AdInfoResult>> aggregatorsMin = Lists.newArrayList();
    aggregatorsMin.add(new AdInfoMaxAggregator());
    aggregatorsMin.add(new AdInfoMinAggregator());
    aggregatorsMin.add(new AdInfoCountAggregator());

    LinkedHashMap<String, DimensionsCombination<AdInfo, AdInfoResult>> dimensionsCombinations =
    Maps.newLinkedHashMap();
    dimensionsCombinations.put("publisher", new PublisherCombination());
    dimensionsCombinations.put("publisher and advertiser", new PublisherAdvertiserCombination());
    dimensionsComputation.setDimensionsCombinations(dimensionsCombinations);

    LinkedHashMap<String, List<Aggregator<AdInfo, AdInfoResult>>> aggList =
    Maps.newLinkedHashMap();
    aggList.put("publisher", aggregatorsSum);
    aggList.put("publisher and advertiser", aggregatorsMin);
    dimensionsComputation.setAggregators(aggList);

    return dimensionsComputation;
  }

  public static class PublisherCombination implements DimensionsCombination<AdInfo, AdInfoResult>
  {
    private static final long serialVersionUID = 201505230532L;

    public PublisherCombination()
    {
    }

    @Override
    public int computeHashCode(AdInfo t)
    {
      return t.publisher.hashCode();
    }

    @Override
    public boolean equals(AdInfo t, AdInfo t1)
    {
      return t.publisher.equals(t1.publisher);
    }

    @Override
    public void setKeys(AdInfo aggregatorInput, AdInfoResult aggregate)
    {
      aggregate.publisher = aggregatorInput.publisher;
    }
  }

  public static class PublisherAdvertiserCombination implements DimensionsCombination<AdInfo, AdInfoResult>
  {
    private static final long serialVersionUID = 201505230533L;

    public PublisherAdvertiserCombination()
    {
    }

    @Override
    public int computeHashCode(AdInfo t)
    {
      return t.publisher.hashCode() ^
             t.advertiser.hashCode();
    }

    @Override
    public boolean equals(AdInfo t, AdInfo t1)
    {
      return t.publisher.equals(t1.publisher) &&
             t.advertiser.equals(t1.advertiser);
    }

    @Override
    public void setKeys(AdInfo aggregatorInput, AdInfoResult aggregate)
    {
      aggregate.publisher = aggregatorInput.publisher;
      aggregate.advertiser = aggregatorInput.advertiser;
    }
  }

  public static class AdInfoCountAggregator implements Aggregator<AdInfo, AdInfoResult>
  {
    public AdInfoCountAggregator()
    {
    }

    @Override
    public void aggregate(AdInfoResult dest, AdInfo src)
    {
      dest.count++;
    }

    @Override
    public void aggregate(AdInfoResult dest, AdInfoResult src)
    {
      dest.count += src.count;
    }

    @Override
    public AdInfoResult createDest(AdInfo first)
    {
      AdInfoResult adInfoResult = new AdInfoResult();

      adInfoResult.count = 1;

      return adInfoResult;
    }
  }

  public static class AdInfoSumAggregator implements Aggregator<AdInfo, AdInfoResult>
  {
    public AdInfoSumAggregator()
    {
    }

    @Override
    public void aggregate(AdInfoResult dest, AdInfo src)
    {
      dest.clicks += src.clicks;
      dest.cost += src.cost;
      dest.impressions += src.impressions;
      dest.revenue += src.revenue;
    }

    @Override
    public void aggregate(AdInfoResult dest, AdInfoResult src)
    {
      dest.clicks += src.clicks;
      dest.cost += src.cost;
      dest.impressions += src.impressions;
      dest.revenue += src.revenue;
    }

    @Override
    public AdInfoResult createDest(AdInfo first)
    {
      return new AdInfoResult(first);
    }
  }

  public static class AdInfoMinAggregator implements Aggregator<AdInfo, AdInfoResult>
  {
    public AdInfoMinAggregator()
    {
    }

    @Override
    public void aggregate(AdInfoResult dest, AdInfo src)
    {
      dest.clicks = Math.min(dest.clicks, src.clicks);
      dest.cost = Math.min(dest.cost, src.cost);
      dest.impressions = Math.min(dest.impressions, src.impressions);
      dest.revenue = Math.min(dest.revenue, src.revenue);
    }

    @Override
    public void aggregate(AdInfoResult dest, AdInfoResult src)
    {
      dest.clicks = Math.min(dest.clicks, src.clicks);
      dest.cost = Math.min(dest.cost, src.cost);
      dest.impressions = Math.min(dest.impressions, src.impressions);
      dest.revenue = Math.min(dest.revenue, src.revenue);
    }

    @Override
    public AdInfoResult createDest(AdInfo first)
    {
      return new AdInfoResult(first);
    }
  }

  public static class AdInfoMaxAggregator implements Aggregator<AdInfo, AdInfoResult>
  {
    public AdInfoMaxAggregator()
    {
    }

    @Override
    public void aggregate(AdInfoResult dest, AdInfo src)
    {
      dest.clicks = Math.max(dest.clicks, src.clicks);
      dest.cost = Math.max(dest.cost, src.cost);
      dest.impressions = Math.max(dest.impressions, src.impressions);
      dest.revenue = Math.max(dest.revenue, src.revenue);
    }

    @Override
    public void aggregate(AdInfoResult dest, AdInfoResult src)
    {
      dest.clicks = Math.max(dest.clicks, src.clicks);
      dest.cost = Math.max(dest.cost, src.cost);
      dest.impressions = Math.max(dest.impressions, src.impressions);
      dest.revenue = Math.max(dest.revenue, src.revenue);
    }

    @Override
    public AdInfoResult createDest(AdInfo first)
    {
      return new AdInfoResult(first);
    }
  }

  public static class AdInfoResult implements UnifiableAggregate, Serializable
  {
    private static final long serialVersionUID = 201505241048L;

    public String publisher;
    public String advertiser;
    public String location;
    public double cost = 0.0;
    public double revenue = 0.0;
    public long impressions = 0;
    public long clicks = 0;
    public long time = 0;
    private int aggregateIndex;
    private long count;

    public AdInfoResult()
    {
    }

    public AdInfoResult(AdInfo adInfo)
    {
      this.cost = adInfo.getCost();
      this.revenue = adInfo.getRevenue();
      this.impressions = adInfo.getImpressions();
      this.clicks = adInfo.getClicks();
    }

    public AdInfoResult(String publisher,
                        String advertiser,
                        String location,
                        double cost,
                        double revenue,
                        long impressions,
                        long clicks,
                        long time)
    {
      this.publisher = publisher;
      this.advertiser = advertiser;
      this.location = location;
      this.cost = cost;
      this.revenue = revenue;
      this.impressions = impressions;
      this.clicks = clicks;
      this.time = time;
    }

    public AdInfoResult(double cost,
                        double revenue,
                        long impressions,
                        long clicks,
                        long time)
    {
      this.cost = cost;
      this.revenue = revenue;
      this.impressions = impressions;
      this.clicks = clicks;
      this.time = time;
    }

    /**
     * @return the publisher
     */
    public String getPublisher()
    {
      return publisher;
    }

    /**
     * @param publisher the publisher to set
     */
    public void setPublisher(String publisher)
    {
      this.publisher = publisher;
    }

    /**
     * @return the advertiser
     */
    public String getAdvertiser()
    {
      return advertiser;
    }

    /**
     * @param advertiser the advertiser to set
     */
    public void setAdvertiser(String advertiser)
    {
      this.advertiser = advertiser;
    }

    /**
     * @return the location
     */
    public String getLocation()
    {
      return location;
    }

    /**
     * @param location the location to set
     */
    public void setLocation(String location)
    {
      this.location = location;
    }

    /**
     * @return the cost
     */
    public double getCost()
    {
      return cost;
    }

    /**
     * @param cost the cost to set
     */
    public void setCost(double cost)
    {
      this.cost = cost;
    }

    /**
     * @return the revenue
     */
    public double getRevenue()
    {
      return revenue;
    }

    /**
     * @param revenue the revenue to set
     */
    public void setRevenue(double revenue)
    {
      this.revenue = revenue;
    }

    /**
     * @return the impressions
     */
    public long getImpressions()
    {
      return impressions;
    }

    /**
     * @param impressions the impressions to set
     */
    public void setImpressions(long impressions)
    {
      this.impressions = impressions;
    }

    /**
     * @return the clicks
     */
    public long getClicks()
    {
      return clicks;
    }

    /**
     * @param clicks the clicks to set
     */
    public void setClicks(long clicks)
    {
      this.clicks = clicks;
    }

    /**
     * @return the time
     */
    public long getTime()
    {
      return time;
    }

    /**
     * @param time the time to set
     */
    public void setTime(long time)
    {
      this.time = time;
    }

    @Override
    public int getAggregateIndex()
    {
      return aggregateIndex;
    }

    @Override
    public void setAggregateIndex(int aggregateIndex)
    {
      this.aggregateIndex = aggregateIndex;
    }

    /**
     * @return the count
     */
    public long getCount()
    {
      return count;
    }

    /**
     * @param count the count to set
     */
    public void setCount(long count)
    {
      this.count = count;
    }

    @Override
    public int hashCode()
    {
      int hash = 7;
      hash = 19 * hash + (this.publisher != null ? this.publisher.hashCode() : 0);
      hash = 19 * hash + (this.advertiser != null ? this.advertiser.hashCode() : 0);
      hash = 19 * hash + (this.location != null ? this.location.hashCode() : 0);
      hash = 19 * hash + (int)(Double.doubleToLongBits(this.cost) ^ (Double.doubleToLongBits(this.cost) >>> 32));
      hash = 19 * hash + (int)(Double.doubleToLongBits(this.revenue) ^ (Double.doubleToLongBits(this.revenue) >>> 32));
      hash = 19 * hash + (int)(this.impressions ^ (this.impressions >>> 32));
      hash = 19 * hash + (int)(this.clicks ^ (this.clicks >>> 32));
      hash = 19 * hash + (int)(this.time ^ (this.time >>> 32));
      hash = 19 * hash + this.aggregateIndex;
      hash = 19 * hash + (int)(this.count ^ (this.count >>> 32));
      return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
      if(obj == null) {
        return false;
      }
      if(getClass() != obj.getClass()) {
        return false;
      }
      final AdInfoResult other = (AdInfoResult)obj;
      if((this.publisher == null) ? (other.publisher != null) : !this.publisher.equals(other.publisher)) {
        return false;
      }
      if((this.advertiser == null) ? (other.advertiser != null) : !this.advertiser.equals(other.advertiser)) {
        return false;
      }
      if((this.location == null) ? (other.location != null) : !this.location.equals(other.location)) {
        return false;
      }
      if(Double.doubleToLongBits(this.cost) != Double.doubleToLongBits(other.cost)) {
        return false;
      }
      if(Double.doubleToLongBits(this.revenue) != Double.doubleToLongBits(other.revenue)) {
        return false;
      }
      if(this.impressions != other.impressions) {
        return false;
      }
      if(this.clicks != other.clicks) {
        return false;
      }
      if(this.time != other.time) {
        return false;
      }
      if(this.aggregateIndex != other.aggregateIndex) {
        return false;
      }
      if(this.count != other.count) {
        return false;
      }
      return true;
    }

    @Override
    public String toString()
    {
      return "AdInfoResult{" + "publisher=" + publisher + ", advertiser=" + advertiser + ", location=" + location + ", cost=" + cost + ", revenue=" + revenue + ", impressions=" + impressions + ", clicks=" + clicks + ", time=" + time + ", aggregateIndex=" + aggregateIndex + ", count=" + count + '}';
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsComputationCustomTest.class);
}
