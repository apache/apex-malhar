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

import com.datatorrent.lib.dimensions.aggregator.Aggregator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.AggregateMap;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.UnifiableAggregate;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DimensionsComputationCustom<EVENT, AGGREGATE extends UnifiableAggregate> extends AbstractDimensionsComputation<EVENT, AGGREGATE>
{
  @NotNull
  private LinkedHashMap<String, DimensionsCombination<EVENT, AGGREGATE>> dimensionsCombinations;
  @NotNull
  private LinkedHashMap<String, List<Aggregator<EVENT, AGGREGATE>>> aggregators;

  /**
   * Input data port that takes an event.
   */
  public final transient DefaultInputPort<EVENT> data = new DefaultInputPort<EVENT>(){

    @Override
    public void process(EVENT tuple)
    {
      processInputTuple(tuple);
    }
  };

  public DimensionsComputationCustom()
  {
    unifierHashingStrategy = new DirectDimensionsCombination<AGGREGATE, AGGREGATE>();
  }

  @Override
  @SuppressWarnings({"unchecked","rawtypes"})
  public void setup(OperatorContext context)
  {
    int size = computeNumAggregators();

    if(this.maps == null) {
      maps = new AggregateMap[size];
      int aggregateIndex = 0;
      List<String> combinationNames = Lists.newArrayList();
      combinationNames.addAll(dimensionsCombinations.keySet());
      Collections.sort(combinationNames);

      for(String combinationName: combinationNames) {
        DimensionsCombination<EVENT, AGGREGATE> combination = dimensionsCombinations.get(combinationName);
        List<Aggregator<EVENT, AGGREGATE>> tempAggregators = aggregators.get(combinationName);

        for(Aggregator<EVENT, AGGREGATE> aggregator: tempAggregators) {
          maps[aggregateIndex] = new AggregateMap<EVENT, AGGREGATE>(aggregator,
                                                                    combination,
                                                                    aggregateIndex);
          aggregateIndex++;
        }
      }
    }
  }

  protected void processInputTuple(EVENT tuple)
  {
    for (int i = 0; i < this.maps.length; i++) {
      maps[i].aggregate(tuple);
    }
  }

  /**
   * @return the dimensionsCombinations
   */
  public LinkedHashMap<String, DimensionsCombination<EVENT, AGGREGATE>> getDimensionsCombinations()
  {
    return dimensionsCombinations;
  }

  /**
   * @param dimensionsCombinations the dimensionsCombinations to set
   */
  public void setDimensionsCombinations(LinkedHashMap<String, DimensionsCombination<EVENT, AGGREGATE>> dimensionsCombinations)
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

  @Override
  public Aggregator<EVENT, AGGREGATE>[] configureDimensionsComputationUnifier()
  {
    int numAggregators = computeNumAggregators();
    @SuppressWarnings({"unchecked","rawtypes"})
    Aggregator<EVENT, AGGREGATE>[] aggregatorsArray = new Aggregator[numAggregators];

    int aggregateIndex = 0;
    List<String> combinationNames = Lists.newArrayList();
    combinationNames.addAll(dimensionsCombinations.keySet());
    Collections.sort(combinationNames);

    for(String combinationName: combinationNames) {
      List<Aggregator<EVENT, AGGREGATE>> tempAggregators = aggregators.get(combinationName);

      for(Aggregator<EVENT, AGGREGATE> aggregator: tempAggregators) {
        aggregatorsArray[aggregateIndex] = aggregator;
        aggregateIndex++;
      }
    }

    unifier.setAggregators(aggregatorsArray);
    unifier.setHashingStrategy(unifierHashingStrategy);

    return aggregatorsArray;
  }

  public void setUnifierHashingStrategy(@NotNull DTHashingStrategy<AGGREGATE> dimensionsCombination)
  {
    this.unifierHashingStrategy = Preconditions.checkNotNull(dimensionsCombination);
  }

  private int computeNumAggregators()
  {
    int size = 0;

    for(Map.Entry<String, List<Aggregator<EVENT, AGGREGATE>>> entry: aggregators.entrySet()) {
      size += entry.getValue().size();
    }

    return size;
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsComputationCustom.class);
}
