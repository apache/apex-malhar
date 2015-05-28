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
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate.AggregateHashingStrategy;
import com.datatorrent.lib.dimensions.DimensionsEvent.DimensionsEventDimensionsCombination;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.dimensions.aggregator.Aggregator;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public abstract class AbstractDimensionsComputationFlexible<INPUT> extends AbstractDimensionsComputation<InputEvent, Aggregate>
{
  protected AggregatorRegistry aggregatorRegistry = AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY;
  protected Int2IntOpenHashMap aggregatorIdToAggregateIndex;

  public transient final DefaultInputPort<INPUT> inputEvent = new DefaultInputPort<INPUT>() {
    @Override
    public void process(INPUT tuple)
    {
      processInputEvent(tuple);
    }
  };

  public AbstractDimensionsComputationFlexible()
  {
    unifierHashingStrategy = new AggregateHashingStrategy();
  }

  public abstract void processInputEvent(INPUT tuple);

  public abstract InputEvent convertInput(INPUT input,
                                          DimensionsConversionContext conversionContext);

  @Override
  @SuppressWarnings({"rawtypes","unchecked"})
  public void setup(OperatorContext context)
  {
    super.setup(context);

    aggregatorRegistry.setup();

    if(maps == null) {
      Map<Integer, IncrementalAggregator> idToAggregator =
      aggregatorRegistry.getIncrementalAggregatorIDToAggregator();

      List<Integer> ids = Lists.newArrayList(idToAggregator.keySet());
      maps = new AggregateMap[ids.size()];
      aggregatorIdToAggregateIndex = new Int2IntOpenHashMap();
      Collections.sort(ids);

      for(int aggregateIndex = 0;
          aggregateIndex < ids.size();
          aggregateIndex++) {
        int aggregatorId = ids.get(aggregateIndex);

        IncrementalAggregator aggregator = idToAggregator.get(aggregatorId);
        AggregateMap<InputEvent, Aggregate> aggregateMap
                = new AggregateMap<InputEvent, Aggregate>(aggregator,
                                                          DimensionsEventDimensionsCombination.INSTANCE,
                                                          aggregateIndex);
        maps[aggregateIndex] = aggregateMap;

        aggregatorIdToAggregateIndex.put(aggregatorId, aggregateIndex);
      }
    }
  }

  @Override
  @VisibleForTesting
  public Aggregator<InputEvent, Aggregate>[] configureDimensionsComputationUnifier()
  {
    aggregatorIdToAggregateIndex = new Int2IntOpenHashMap();

    computeAggregatorIdToAggregateIndex();

    @SuppressWarnings({"unchecked","rawtypes"})
    Aggregator<InputEvent, Aggregate>[] aggregators = new Aggregator[aggregatorIdToAggregateIndex.size()];

    for(Entry<Integer, Integer> entry: aggregatorIdToAggregateIndex.entrySet()) {
      Integer aggregatorId = entry.getKey();
      Integer aggregatorIndex = entry.getValue();

      Aggregator<InputEvent, Aggregate> aggregator =
      aggregatorRegistry.getIncrementalAggregatorIDToAggregator().get(aggregatorId);

      aggregators[aggregatorIndex] = aggregator;
    }

    unifier.setAggregators(aggregators);

    if(this.unifierHashingStrategy != null &&
       unifier.getHashingStrategy() == null) {
      unifier.setHashingStrategy(this.unifierHashingStrategy);
    }

    return aggregators;
  }

  private void computeAggregatorIdToAggregateIndex()
  {
    aggregatorRegistry.setup();

    Map<Integer, IncrementalAggregator> idToAggregator
            = aggregatorRegistry.getIncrementalAggregatorIDToAggregator();

    List<Integer> ids = Lists.newArrayList(idToAggregator.keySet());
    Collections.sort(ids);

    for(int aggregateIndex = 0;
        aggregateIndex < ids.size();
        aggregateIndex++) {
      int aggregatorId = ids.get(aggregateIndex);
      aggregatorIdToAggregateIndex.put(aggregatorId, aggregateIndex);
    }
  }

  /**
   * @return the aggregatorRegistry
   */
  public AggregatorRegistry getAggregatorRegistry()
  {
    return aggregatorRegistry;
  }

  /**
   * @param aggregatorRegistry the aggregatorRegistry to set
   */
  public void setAggregatorRegistry(AggregatorRegistry aggregatorRegistry)
  {
    this.aggregatorRegistry = aggregatorRegistry;
  }

  public class DimensionsConversionContext
  {
    public int schemaID;
    public int dimensionDescriptorID;
    public int aggregatorID;
    public int ddID;
    public DimensionsDescriptor dd;
    public FieldsDescriptor keyFieldsDescriptor;
    public FieldsDescriptor aggregateDescriptor;

    public DimensionsConversionContext()
    {
    }
  }
}
