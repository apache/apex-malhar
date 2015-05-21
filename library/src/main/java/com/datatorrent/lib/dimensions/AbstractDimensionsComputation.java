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
import com.datatorrent.api.Operator;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.UnifiableAggregate;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.base.Preconditions;
import gnu.trove.map.hash.TCustomHashMap;
import gnu.trove.strategy.HashingStrategy;

public abstract class AbstractDimensionsComputation<AGGREGATOR_INPUT, AGGREGATE extends UnifiableAggregate> implements Operator
{
  @Bind(JavaSerializer.class)
  protected AggregateMap<AGGREGATOR_INPUT, AGGREGATE>[] maps;

  private Unifier<AGGREGATE> unifier;

  public void setUnifier(Unifier<AGGREGATE> unifier)
  {
    this.unifier = unifier;
  }

  public final transient DefaultOutputPort<AGGREGATE> output = new DefaultOutputPort<AGGREGATE>()
  {
    @Override
    public Unifier<AGGREGATE> getUnifier()
    {
      if (unifier == null) {
        return super.getUnifier();
      }
      else {
        return unifier;
      }
    }
  };

  public AbstractDimensionsComputation()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  protected static class AggregateMap<AGGREGATOR_INPUT, AGGREGATE extends UnifiableAggregate> extends TCustomHashMap<AGGREGATOR_INPUT, AGGREGATE>
  {
    private static final long serialVersionUID = 201505200427L;
    private Aggregator<AGGREGATOR_INPUT, AGGREGATE> aggregator;
    private int aggregateIndex;

    public AggregateMap()
    {
      //For Serialization
      super();
      aggregator = null;
    }

    public AggregateMap(Aggregator<AGGREGATOR_INPUT, AGGREGATE> aggregator,
                        HashingStrategy<AGGREGATOR_INPUT> hashingStrategy,
                        int initialCapacity,
                        int aggregateIndex)
    {
      super(hashingStrategy, initialCapacity);

      this.aggregator = Preconditions.checkNotNull(aggregator);
      this.aggregateIndex = aggregateIndex;
    }

    public void add(AGGREGATOR_INPUT aggregatorInput)
    {
      AGGREGATE aggregate = get(aggregatorInput);

      if(aggregate == null) {
        aggregate = aggregator.createDest(aggregatorInput);
        aggregate.setAggregateIndex(aggregateIndex);
        put(aggregatorInput, aggregate);
      }
      else {
        aggregator.aggregate(aggregate, aggregatorInput);
      }
    }

    @Override
    public int hashCode()
    {
      int hash = 7;
      hash = 23 * hash + (this.aggregator != null ? this.aggregator.hashCode() : 0);
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
      final AggregateMap<?, ?> other = (AggregateMap<?, ?>)obj;
      if(this.aggregator != other.aggregator && (this.aggregator == null || !this.aggregator.equals(other.aggregator))) {
        return false;
      }
      return true;
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    for(AggregateMap<AGGREGATOR_INPUT, AGGREGATE> dimension: maps) {
      for(AGGREGATE value: dimension.values()) {
        output.emit(value);
      }

      dimension.clear();
    }
  }

  @Override
  public void teardown()
  {
  }

  public interface AggregateResult {}

  public interface UnifiableAggregate extends AggregateResult
  {
    public int getAggregateIndex();
    public void setAggregateIndex(int aggregateIndex);
  }
}
