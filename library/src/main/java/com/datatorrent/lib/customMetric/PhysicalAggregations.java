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
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;

import com.datatorrent.api.CustomMetric;

/**
 * An implementation of {@link CustomMetric.PhysicalAggregations} which requires a map from metrics to a collection of
 * aggregators.
 */
public class PhysicalAggregations implements CustomMetric.PhysicalAggregations, Serializable
{
  //metric key -> collection of physical aggregators
  private Map<String, Collection<CustomMetric.Aggregator<?>>> aggregations;

  @Override
  public Collection<CustomMetric.Aggregator<?>> getAggregatorsFor(@NotNull String metricKey)
  {
    if (aggregations == null) {
      return null;
    }
    return aggregations.get(metricKey);
  }

  public void setAggregations(@NotNull Map<String, Collection<CustomMetric.Aggregator<?>>> aggregations)
  {
    this.aggregations = Preconditions.checkNotNull(aggregations, "aggregations");
  }

  public Map<String, Collection<CustomMetric.Aggregator<?>>> getAggregations()
  {
    return this.aggregations;
  }

  private static final long serialVersionUID = 201604171718L;
}
