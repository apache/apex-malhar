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

package com.datatorrent.lib.dimensions;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.DTHashingStrategy;
import com.datatorrent.lib.dimensions.DimensionsComputation.UnifiableAggregate;
import com.datatorrent.lib.dimensions.aggregator.Aggregator;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.base.Preconditions;
import gnu.trove.map.hash.TCustomHashMap;

/**
 * A unifier for dimensions computation operators.
 * @param <AGGREGATE_INPUT> The input type for aggregators which will be set on this unifier.
 * @param <AGGREGATE> The type of the data which the unifier receives, unifies and emits.
 */
public class DimensionsComputationUnifierImpl<AGGREGATE_INPUT, AGGREGATE extends UnifiableAggregate> implements
             DimensionsComputationUnifier<AGGREGATE_INPUT, AGGREGATE>
{
  /**
   * The map which holds the aggregates being unified.
   */
  @Bind(JavaSerializer.class) //This is required otherwise the unifier does not serialize.
  private TCustomHashMap<AGGREGATE, AGGREGATE> aggregateMap;
  /**
   * The hashing strategy used to determine what aggregates to unify together.
   */
  private DTHashingStrategy<AGGREGATE> hashingStrategy;
  /**
   * The aggregators to use during unification.
   */
  private Aggregator<AGGREGATE_INPUT, AGGREGATE>[] aggregators;
  /**
   * The output port for unified aggregates.
   */
  public transient final DefaultOutputPort<AGGREGATE> output = new DefaultOutputPort<AGGREGATE>();

  /**
   * Constructor for creating a unifier.
   */
  public DimensionsComputationUnifierImpl()
  {
    //Do nothing
  }

  @Override
  public void setAggregators(Aggregator<AGGREGATE_INPUT, AGGREGATE>[] aggregators)
  {
    this.aggregators = Preconditions.checkNotNull(aggregators);
  }

  @Override
  public void setHashingStrategy(DTHashingStrategy <AGGREGATE> hashingStrategy)
  {
    this.hashingStrategy = Preconditions.checkNotNull(hashingStrategy);
  }

  @Override
  public DTHashingStrategy<AGGREGATE> getHashingStrategy()
  {
    return hashingStrategy;
  }

  @Override
  public void setup(OperatorContext context)
  {
    aggregateMap = new TCustomHashMap<AGGREGATE, AGGREGATE>(hashingStrategy);
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void process(AGGREGATE tuple)
  {
    AGGREGATE aggregate = aggregateMap.get(tuple);

    if(aggregate == null) {
      aggregateMap.put(tuple, tuple);
    }
    else {
      aggregators[tuple.getAggregateIndex()].aggregate(aggregate, tuple);
    }
  }

  @Override
  public void endWindow()
  {
    for(AGGREGATE aggregate: aggregateMap.values()) {
      output.emit(aggregate);
    }

    aggregateMap.clear();
  }

  @Override
  public void teardown()
  {
  }
}
