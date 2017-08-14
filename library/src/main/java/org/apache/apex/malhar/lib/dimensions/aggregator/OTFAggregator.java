/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.dimensions.aggregator;

import java.io.Serializable;

import java.util.List;

import org.apache.apex.malhar.lib.appdata.gpo.GPOMutable;
import org.apache.apex.malhar.lib.appdata.schemas.Type;

/**
 * <p>
 * This interface represents an On The Fly Aggregator. On the fly aggregators represent a class
 * of aggregations which use the results of incremental aggregators, which implement the
 * {@link org.apache.apex.malhar.lib.dimensions.aggregator.IncrementalAggregator} interface. An example of an aggregation which
 * needs to be performed on the fly is average. Average needs to be performed on the fly because average cannot be
 * computed with just an existing average and a new data item, an average required the sum of all data items, and the
 * count of all data items. An example implementation of average is {@link AggregatorAverage}. Also note
 * that unlike {@link IncrementalAggregator}s an {@link OTFAggregator} only has one output type. This done
 * because {@link OTFAggregator}s usually represent a very specific computation, with a specific output type.
 * For example, average is a computation that you will almost always want to produce a double. But if you require
 * an average operation that produces an integer, that could be done as a separate {@link OTFAggregator}.
 * </p>
 * <p>
 * The primary usage for {@link OTFAggregator}s are in store operators which respond to queries. Currently,
 * the only places which utilize {@link OTFAggregator}s are subclasses of the DimensionsStoreHDHT operator.
 * </p>
 * <p>
 * This interface extends {@link Serializable} because On The Fly aggregators may be set
 * as properties on some operators and operator properties are required to be java serializable.
 * </p>
 * @since 3.1.0
 */
public interface OTFAggregator extends Serializable
{
  public static final long serialVersionUID = 201505251039L;

  /**
   * This method returns all the incremental aggregators on which this aggregator depends on
   * to compute its result. In the case of {@link AggregatorAverage} it's child aggregators are
   * {@link AggregatorCount} and {@link AggregatorSum}.
   * @return All the incremental aggregators on which this aggregator depends on to compute its
   * result.
   */

  public List<Class<? extends IncrementalAggregator>> getChildAggregators();
  /**
   * This method performs an on the fly aggregation from the given aggregates. The aggregates
   * provided to this aggregator are each the result of one of this aggregators child aggregators.
   * The order in which the aggregates are passed to this method is the same as the order in
   * which the child aggregators are listed in the result of the {@link #getChildAggregators} method.
   * Also note that this aggregator does not aggregate one field at a time. This aggregator recieves
   * a batch of fields from each child aggregator, and the result of the method is also a batch of fields.
   * @param aggregates These are the results of all the child aggregators. The results are in the same
   * order as the child aggregators specified in the result of the {@link #getChildAggregators} method.
   * @return The result of the on the fly aggregation.
   */

  public GPOMutable aggregate(GPOMutable... aggregates);
  /**
   * Returns the output type of the {@link OTFAggregator}. <b>Note<b> that any combination of input types
   * will produce the same output type for {@link OTFAggregator}s.
   * @return The output type of the {@link OTFAggregator}.
   */

  public Type getOutputType();
}
