/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.statistics;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 * A {@link Unifier} implementation for {@link DimensionsComputation}.<br/>
 *
 * @param <EVENT>
 */
public class DimensionsComputationUnifierImpl<EVENT> extends BaseOperator implements Operator.Unifier<DimensionsComputation.AggregateEvent>
{
  @Nonnull
  private DimensionsComputation.Aggregator<EVENT>[] aggregators;
  @Nonnull
  private final Map<DimensionsComputation.AggregateEvent, DimensionsComputation.AggregateEvent> aggregates;

  public final transient DefaultOutputPort<DimensionsComputation.AggregateEvent> output = new DefaultOutputPort<DimensionsComputation.AggregateEvent>();

  public DimensionsComputationUnifierImpl()
  {
    /** for kryo serialization */
    aggregators = null;
    aggregates = Maps.newHashMap();
  }

  /**
   * Sets the aggregators.
   *
   * @param aggregators
   */
  public void setAggregators(@Nonnull DimensionsComputation.Aggregator<EVENT>[] aggregators)
  {
    this.aggregators = aggregators;
  }

  @Override
  public void process(DimensionsComputation.AggregateEvent tuple)
  {
    DimensionsComputation.AggregateEvent destination = aggregates.get(tuple);
    if (destination == null) {
      aggregates.put(tuple, tuple);
    }
    else {
      int aggregatorIndex = tuple.getAggregatorIndex();
      aggregators[aggregatorIndex].aggregate(destination, tuple);
    }
  }

  public void endWindow()
  {
    for (DimensionsComputation.AggregateEvent value : aggregates.values()) {
      output.emit(value);
    }
    aggregates.clear();
  }
}
