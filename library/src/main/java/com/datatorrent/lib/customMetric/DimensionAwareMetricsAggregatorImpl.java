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
package com.datatorrent.lib.customMetric;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.CustomMetric;

public class DimensionAwareMetricsAggregatorImpl implements DimensionAwareMetricsAggregator, Serializable
{
  protected final MetricsAggregator metricsAggregator;
  protected final Map<String, TimeBasedAggregatorMeta> aggregateKeyToMeta;

  public DimensionAwareMetricsAggregatorImpl()
  {
    metricsAggregator = new MetricsAggregator();
    aggregateKeyToMeta = Maps.newHashMap();
  }

  @Override
  public Map<String, Object> aggregate(long windowId, Collection<CustomMetric.PhysicalMetricsContext> physicalMetrics)
  {
    return metricsAggregator.aggregate(windowId, physicalMetrics);
  }

  public void addAggregators(@NotNull String metric, @NotNull SingleMetricAggregator[] aggregators,
                             @NotNull String[][] timeAggregators)
  {
    Preconditions.checkNotNull(metric, "metric");
    Preconditions.checkNotNull(aggregators, "aggregators");
    Preconditions.checkNotNull(timeAggregators, "time aggregators");
    addAggregatorsHelper(metric, aggregators, timeAggregators, null);
  }

  public void addAggregators(@NotNull String metric, @NotNull SingleMetricAggregator[] aggregators,
                             @NotNull String[][] timeAggregators,
                             @NotNull String[] aggregateKeys)
  {
    Preconditions.checkNotNull(metric, "metric");
    Preconditions.checkNotNull(aggregators, "aggregators");
    Preconditions.checkNotNull(timeAggregators, "time aggregators");
    Preconditions.checkNotNull(aggregateKeys, "result metric keys");
    Preconditions.checkArgument(aggregators.length == aggregateKeys.length && aggregators.length == timeAggregators.length,
      "different length aggregators, aggregateKeys  & time aggregators");

    addAggregatorsHelper(metric, aggregators, timeAggregators, aggregateKeys);
  }

  public String[] getDimensionAggregators(String aggregateKey)
  {
    return aggregateKeyToMeta.get(aggregateKey).dimensionAggregators;
  }

  private void addAggregatorsHelper(String metric, SingleMetricAggregator[] aggregators,
                                    String[][] timeAggregators, String[] aggregateKeys)
  {
    List<MetricsAggregator.AggregatorMeta> laggregators = metricsAggregator.metricsToAggregators.get(metric);
    if (laggregators == null) {
      laggregators = Lists.newArrayList();
      metricsAggregator.metricsToAggregators.put(metric, laggregators);
    }
    for (int i = 0; i < aggregators.length; i++) {
      String resultKey = (aggregateKeys == null || aggregateKeys[i] == null) ?
        metricsAggregator.deriveAggregateMetricKey(metric, aggregators[i]) : aggregateKeys[i];

      TimeBasedAggregatorMeta tbam =  new TimeBasedAggregatorMeta(aggregators[i], resultKey, timeAggregators[i]);
      laggregators.add(tbam);
      aggregateKeyToMeta.put(resultKey, tbam);
    }
  }

  public static class TimeBasedAggregatorMeta extends MetricsAggregator.AggregatorMeta
  {

    private String[] dimensionAggregators;

    protected TimeBasedAggregatorMeta(@NotNull SingleMetricAggregator aggregator, @NotNull String aggregateKey,
                                      @NotNull String[] dimensionAggregators)
    {
      super(aggregator, aggregateKey);
      this.dimensionAggregators = Preconditions.checkNotNull(dimensionAggregators, "time aggregators");
    }
  }

  private static final long serialVersionUID = 201504231704L;
}
