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
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.UnifiableAggregate;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.AggregateMap;
import gnu.trove.strategy.HashingStrategy;
import javax.validation.constraints.NotNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DimensionsComputationCustom<EVENT, AGGREGATE extends UnifiableAggregate> extends AbstractDimensionsComputation<EVENT, AGGREGATE>
{
  @NotNull
  private LinkedHashMap<String, DimensionsCombination<EVENT>> dimensionsCombinations;
  @NotNull
  private LinkedHashMap<String, List<Aggregator<EVENT, AGGREGATE>>> aggregators;

  public DimensionsComputationCustom()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    int size = 0;

    for(Map.Entry<String, List<Aggregator<EVENT,AGGREGATE>>> entry: aggregators.entrySet()) {
      size += entry.getValue().size();
    }

    this.maps = new AggregateMap[size];

    int aggregateIndex = 0;

    for(Map.Entry<String, DimensionsCombination<EVENT>> entry: dimensionsCombinations.entrySet()) {
      String dimensionName = entry.getKey();
      DimensionsCombination<EVENT> combination = entry.getValue();
      List<Aggregator<EVENT, AGGREGATE>> tempAggregators = aggregators.get(dimensionName);

      for(Aggregator<EVENT, AGGREGATE> aggregator: tempAggregators) {
        maps[aggregateIndex] = new AggregateMap(aggregator,
                                                combination,
                                                10000,
                                                aggregateIndex);
        aggregateIndex++;
      }
    }
  }

  protected void processInputTuple(EVENT tuple)
  {
    for (int i = 0; i < this.maps.length; i++) {
      maps[i].add(tuple);
    }
  }

  /**
   * Input data port that takes an event.
   */
  public final transient DefaultInputPort<EVENT> data = new DefaultInputPort<EVENT>()
  {
    @Override
    public void process(EVENT tuple)
    {
      processInputTuple(tuple);
    }
  };

  /**
   * @return the dimensionsCombinations
   */
  public LinkedHashMap<String, DimensionsCombination<EVENT>> getDimensionsCombinations()
  {
    return dimensionsCombinations;
  }

  /**
   * @param dimensionsCombinations the dimensionsCombinations to set
   */
  public void setDimensionsCombinations(LinkedHashMap<String, DimensionsCombination<EVENT>> dimensionsCombinations)
  {
    this.dimensionsCombinations = dimensionsCombinations;
  }

  /**
   * @return the aggregators
   */
  public LinkedHashMap<String, List<Aggregator<EVENT, AGGREGATE>>> getAggregators()
  {
    return aggregators;
  }

  /**
   * @param aggregators the aggregators to set
   */
  public void setAggregators(LinkedHashMap<String, List<Aggregator<EVENT, AGGREGATE>>> aggregators)
  {
    this.aggregators = aggregators;
  }

  public static interface DimensionsCombination<AGGREGATOR_INPUT> extends HashingStrategy<AGGREGATOR_INPUT> {}
}
