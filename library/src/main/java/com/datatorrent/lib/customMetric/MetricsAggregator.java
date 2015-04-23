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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import com.datatorrent.api.CustomMetric;
import com.datatorrent.api.annotation.Name;

public class MetricsAggregator implements CustomMetric.Aggregator
{
  protected static final String DEFAULT_SEPARATOR = "-";

  protected Map<String, List<SingleMetricAggregator>> metricsToAggregators;
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

    for(CustomMetric.PhysicalMetricsContext pmCtx : physicalMetrics){
      for(Map.Entry<String, Object> entry : pmCtx.getCustomMetrics().entrySet()){
        metricValues.put(entry.getKey(), entry.getValue());
      }
    }

    Map<String, Object> aggregates = Maps.newHashMap();
    for(String metric : metricValues.keySet()){
      List<SingleMetricAggregator> aggregators =  metricsToAggregators.get(metric);
      if(aggregators!=null){
        for (SingleMetricAggregator aggregator : aggregators){
          Object aggregatedVal = aggregator.aggregate( metricValues.get(metric));
          String aggregateKey = deriveAggregateMetricKey(metric, aggregator);
          aggregates.put(aggregateKey, aggregatedVal);
        }
      }
    }
    return aggregates;
  }

  /**
   * This can be overridden to change name of aggregated key.
   *
   * @param metric      metric name
   * @param aggregator  aggregator
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

  public void addAggregator(String metricKey, SingleMetricAggregator aggregator)
  {
    List<SingleMetricAggregator> aggregators = metricsToAggregators.get(metricKey);
    if(aggregators == null){
      aggregators = Lists.newArrayList();
      metricsToAggregators.put(metricKey, aggregators);
    }
    aggregators.add(aggregator);
  }

  public void setMetricsToAggregators(Map<String, List<SingleMetricAggregator>> metricsToAggregators)
  {
    this.metricsToAggregators = metricsToAggregators;
  }

  public Map<String, List<SingleMetricAggregator>> getMetricsToAggregators()
  {
    return Collections.unmodifiableMap(metricsToAggregators);
  }

  public String getAggregatorMetricSeparator()
  {
    return aggregatorMetricSeparator;
  }

  public void setAggregatorMetricSeparator(String aggregatorMetricSeparator)
  {
    this.aggregatorMetricSeparator = aggregatorMetricSeparator;
  }
}
