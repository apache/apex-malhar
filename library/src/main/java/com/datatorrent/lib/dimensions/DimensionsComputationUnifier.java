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

import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.DTHashingStrategy;
import com.datatorrent.lib.dimensions.DimensionsComputation.UnifiableAggregate;
import com.datatorrent.lib.dimensions.aggregator.Aggregator;

/**
 * This interface is implemented by dimensions computation unifiers. Dimensions computation unifiers unify aggregates
 * using the aggregator that corresponds to the aggregate index which is set on the incoming AGGREGATE objects.
 * @param <AGGREGATE_INPUT> The input type for aggregators which will be set on this unifier. <b>Note</b> that
 * this input type is not actually used in the unifier itself, it is required for defining the generic type for the aggregators
 * that can be set on this unifier.
 * @param <AGGREGATE> The type of the data which the unifier receives, unifies and emits.
 */
public interface DimensionsComputationUnifier<AGGREGATE_INPUT, AGGREGATE extends UnifiableAggregate> extends Unifier<AGGREGATE>
{
  /**
   * Sets the aggregators used by this unifier.
   * @param aggregators The aggregators used by this unifier. <b>Note</b> that the order of the aggregators
   * should be the same as the order of the aggregators defined according to the aggregateIndex in the upstream
   * dimensions computation operator.
   */
  public void setAggregators(Aggregator<AGGREGATE_INPUT, AGGREGATE>[] aggregators);
  /**
   * Sets the hashing strategy for the unifier. The hashing strategy is used by the unifier in order to determine
   * which input aggregates should be unified together.
   * @param dimensionsCombination The hashing strategy for the unifier.
   */
  public void setHashingStrategy(DTHashingStrategy<AGGREGATE> dimensionsCombination);
  /**
   * Gets the hashing strategy used by this unifier.
   * @return The hashing strategy used by this unifier.
   */
  public DTHashingStrategy<AGGREGATE> getHashingStrategy();
}
