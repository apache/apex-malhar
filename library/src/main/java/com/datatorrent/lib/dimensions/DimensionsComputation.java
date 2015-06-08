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

/**
 * This is a container interface for other interfaces used for dimensions computation.
 */
public interface DimensionsComputation
{
  /**
   * This is a marker interface that is required to satisfy the generics of the {@link Aggregator}
   * interface.
   */
  public interface AggregateResult {}

  /**
   * This represents an aggregate that can be unified by a unifier which implements the {@link DimensionsComputationUnifier}
   * interface.
   */
  public interface UnifiableAggregate extends AggregateResult
  {
    /**
     * Returns the aggregateIndex for this aggregate.
     * @return The aggregateIndex for this aggregate.
     */
    public int getAggregateIndex();
    /**
     * Sets the aggregateIndex for this aggregate.
     * @param aggregateIndex The aggregateIndex for this aggregate.
     */
    public void setAggregateIndex(int aggregateIndex);
  }
}
