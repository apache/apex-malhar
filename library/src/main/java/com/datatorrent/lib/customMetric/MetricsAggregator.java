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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import com.datatorrent.api.CustomMetric;
import com.datatorrent.api.annotation.Name;

public class MetricsAggregator implements CustomMetric.Aggregator, Serializable
{
  protected static final String DEFAULT_SEPARATOR = "-";

  protected final Map<String, List<AggregatorMeta>> metricsToAggregators;
  protected String aggregatorMetricSeparator;

  public MetricsAggregator()
  {
    metricsToAggregators = Maps.newHashMap();
    aggregatorMetricSeparator = DEFAULT_SEPARATOR;
  }

  @Override
  public Map<String, Object> aggregate(long windowId, Collection<CustomMetric.PhysicalMetricsContext> physicalMetrics)
  {
    Multimap<String, Object> metricValues = ArrayListMultimap.create();

    for (CustomMetric.PhysicalMetricsContext pmCtx : physicalMetrics) {
      for (Map.Entry<String, Object> entry : pmCtx.getCustomMetrics().entrySet()) {
        metricValues.put(entry.getKey(), entry.getValue());
      }
    }

    Map<String, Object> aggregates = Maps.newHashMap();
    for (String metric : metricValues.keySet()) {
      List<AggregatorMeta> aggregatorMetas = metricsToAggregators.get(metric);
      if (aggregatorMetas != null) {
        for (AggregatorMeta aggregatorMeta : aggregatorMetas) {

          Object aggregatedVal = aggregatorMeta.aggregator.aggregate(metricValues.get(metric));
          aggregates.put(aggregatorMeta.aggregateKey, aggregatedVal);
        }
      }
    }
    return aggregates;
  }

  /**
   * This can be overridden to change name of aggregated key.
   *
   * @param metric     metric name
   * @param aggregator aggregator
   * @return key of the aggregated value in the logical metrics.
   */
  protected String deriveAggregateMetricKey(String metric, SingleMetricAggregator aggregator)
  {
    Name aggregatorName = aggregator.getClass().getAnnotation(Name.class);
    String aggregatorDesc;
    if (aggregatorName == null) {
      aggregatorDesc = aggregator.getClass().getName();
    }
    else {
      aggregatorDesc = aggregatorName.value();
    }
    return aggregatorDesc + aggregatorMetricSeparator + metric;
  }

  public void addAggregators(@NotNull String metric, @NotNull SingleMetricAggregator[] aggregators)
  {
    Preconditions.checkNotNull(metric, "metric");
    Preconditions.checkNotNull(aggregators, "aggregators");
    addAggregatorsHelper(metric, aggregators, null);
  }

  public void addAggregators(@NotNull String metric, @NotNull SingleMetricAggregator[] aggregators,
                             @NotNull String[] aggregateKeys)
  {
    Preconditions.checkNotNull(metric, "metric");
    Preconditions.checkNotNull(aggregators, "aggregators");
    Preconditions.checkNotNull(aggregateKeys, "result metric keys");
    Preconditions.checkArgument(aggregators.length == aggregateKeys.length, "different length aggregators and aggregateKeys");
    addAggregatorsHelper(metric, aggregators, aggregateKeys);
  }

  private void addAggregatorsHelper(String metric, SingleMetricAggregator[] aggregators, String[] aggregateKeys)
  {
    List<AggregatorMeta> laggregators = metricsToAggregators.get(metric);
    if (laggregators == null) {
      laggregators = Lists.newArrayList();
      metricsToAggregators.put(metric, laggregators);
    }
    for (int i = 0; i < aggregators.length; i++) {

      laggregators.add(new AggregatorMeta(aggregators[i], (aggregateKeys == null || aggregateKeys[i] == null) ?
        deriveAggregateMetricKey(metric, aggregators[i]) : aggregateKeys[i]));
    }
  }

  public String getAggregatorMetricSeparator()
  {
    return aggregatorMetricSeparator;
  }

  public void setAggregatorMetricSeparator(String aggregatorMetricSeparator)
  {
    this.aggregatorMetricSeparator = aggregatorMetricSeparator;
  }

  public static class AggregatorMeta implements Serializable
  {
    private SingleMetricAggregator aggregator;

    private String aggregateKey;

    private AggregatorMeta(@NotNull SingleMetricAggregator aggregator, @NotNull String aggregateKey)
    {
      this.aggregator = Preconditions.checkNotNull(aggregator, "aggregator");
      this.aggregateKey = Preconditions.checkNotNull(aggregateKey, "result metric key");
    }

    public SingleMetricAggregator getAggregator()
    {
      return aggregator;
    }

    private void setAggregator(SingleMetricAggregator aggregator)
    {
      this.aggregator = aggregator;
    }

    public String getAggregateKey()
    {
      return aggregateKey;
    }

    private void setAggregateKey(String aggregateKey)
    {
      this.aggregateKey = aggregateKey;
    }

    private static final long serialVersionUID = 201604231340L;
  }

  private static final long serialVersionUID = 201604231337L;
}
