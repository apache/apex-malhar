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
import java.util.LinkedHashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;

import com.datatorrent.api.CustomMetric;

/**
 * An implementation of {@link PhysicalAggregations} and {@link CustomMetric.BucketAggregations} that
 * requires a map from metrics to a sub map that links a physical aggregator to a collection of bucket aggregations.
 */
public class PhysicalBucketAggregations implements CustomMetric.PhysicalAggregations,
  CustomMetric.BucketAggregations, Serializable
{
  //metric -> (physical aggregator -> collection of bucket aggregations)
  private Map<String, LinkedHashMap<CustomMetric.PhysicalAggregator<?>, Collection<String>>> aggregations;

  @Override
  public Collection<String> getBucketAggregatorsFor(String metricKey, CustomMetric.PhysicalAggregator<?> aggregator)
  {
    if (aggregations == null) {
      return null;
    }
    LinkedHashMap<CustomMetric.PhysicalAggregator<?>, Collection<String>> aggregatorBuckets = aggregations.get(metricKey);
    if (aggregatorBuckets == null) {
      return null;
    }
    return aggregatorBuckets.get(aggregator);
  }

  @Override
  public Collection<CustomMetric.PhysicalAggregator<?>> getAggregatorsFor(String metricKey)
  {
    if (aggregations == null) {
      return null;
    }
    return aggregations.get(metricKey).keySet();
  }

  public void setAggregations(@NotNull Map<String,
    LinkedHashMap<CustomMetric.PhysicalAggregator<?>, Collection<String>>> aggregations)
  {
    aggregations = Preconditions.checkNotNull(aggregations, "aggregations");
  }

  public Map<String, LinkedHashMap<CustomMetric.PhysicalAggregator<?>, Collection<String>>> getAggregations()
  {
    return aggregations;
  }

  private static final long serialVersionUID = 201604171747L;
}
