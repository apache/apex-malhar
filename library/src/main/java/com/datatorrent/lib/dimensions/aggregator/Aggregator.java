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

package com.datatorrent.lib.dimensions.aggregator;

import com.datatorrent.lib.dimensions.DimensionsComputation.AggregateResult;
import java.io.Serializable;

/**
 * <p>
 * This is an interface for an {@link Aggregator}. An {@link Aggregator} is a function
 * which takes input of events of type EVENT and performs a cumulative function on them to
 * create a value of type AGGREGATE. An {@link Aggregator} performs three main functions. This
 * interface extends serializable because aggregators can be set as properties on some operators
 * and properties must be java serializable.
 * </p>
 * <p>
 * Currently {@link Aggregator} objects are utilized in serveral places. They are used in the
 * subclasses of {@link AbstractDimensionsComputation} operators in order to perform dimensional
 * computation, in implementations of {@link DimensionsComputationUnifier} in order to aggregate
 * aggregates, and they are used in the subclasses of {@link DimensionsStoreHDHT} in order to
 * aggregate new aggregates with stored aggregations.
 * </p>
 * @param <EVENT> The type of an input event.
 * @param <AGGREGATE> The type of the output event.
 */
public interface Aggregator<EVENT, AGGREGATE extends AggregateResult> extends Serializable
{
  public static final long serialVersionUID = 201505240659L;

  /**
   * This method is used to aggregate an input event to an existing aggregate.
   * @param dest The destination aggregate to aggregate an input event to.
   * @param src The input event to aggregate to a destination aggregate.
   */
  public void aggregate(AGGREGATE dest, EVENT src);
  /**
   * This method is used to aggregate two existing aggregations together.
   * @param dest This is the destination aggregate to aggregate an aggregate to.
   * @param src This is another aggregate that will be aggregated to the destination aggregate.
   */
  public void aggregate(AGGREGATE dest, AGGREGATE src);
  /**
   * This is used to initialize an aggregation. When the first input event is received, there
   * is no existing aggregation, so this aggregation must be created. The way in which this
   * aggregation is initialized depends on the aggregator. For example {@link AggregatorCount} would
   * want to initialize the aggregation to 1 when recieving the first input event, but {@link AggregatorSum}
   * would want to initialize the aggregation to be the same as the input event.
   * @param first The first input event recieved which is used to initialize the aggregation.
   * @return An aggregation initialized with the data provided in the first input event.
   */
  public AGGREGATE createDest(EVENT first);
}
