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
 * @since 0.9.4
 */
public class DimensionsComputationUnifierImpl<EVENT, AGGREGATE extends DimensionsComputation.AggregateEvent> extends BaseOperator implements Operator.Unifier<AGGREGATE>
{
  @Nonnull
  private DimensionsComputation.Aggregator<EVENT, AGGREGATE>[] aggregators;
  @Nonnull
  private final Map<AGGREGATE, AGGREGATE> aggregates;

  public final transient DefaultOutputPort<AGGREGATE> output = new DefaultOutputPort<AGGREGATE>();

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
  public void setAggregators(@Nonnull DimensionsComputation.Aggregator<EVENT, AGGREGATE>[] aggregators)
  {
    this.aggregators = aggregators;
  }

  @Override
  public void process(AGGREGATE tuple)
  {
    AGGREGATE destination = aggregates.get(tuple);
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
    for (AGGREGATE value : aggregates.values()) {
      output.emit(value);
    }
    aggregates.clear();
  }
}
