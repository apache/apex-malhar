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

import org.apache.apex.malhar.lib.appdata.schemas.FieldsDescriptor;
import org.apache.apex.malhar.lib.appdata.schemas.Type;
import org.apache.apex.malhar.lib.dimensions.DimensionsConversionContext;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.Aggregate;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.InputEvent;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregateEvent.Aggregator;

/**
 * <p>
 * {@link IncrementalAggregator}s perform aggregations in place, on a field by field basis. For example if we have a
 * field cost, an incremental aggregator would take a new value of cost and aggregate it to an aggregate value for
 * cost. No fields except the cost field are used in the computation of the cost aggregation in the case of an
 * {@link IncrementalAggregator}.
 * </p>
 * <p>
 * {@link IncrementalAggregator}s are intended to be used with subclasses of
 * {@link org.apache.apex.malhar.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema}. The way in which
 * {@link IncrementalAggregator}s are used in this context is that a batch of fields to be aggregated by the aggregator
 * are provided in the form of an {@link InputEvent}. For example, if there are two fields (cost and revenue), which
 * will be aggregated by a sum aggregator, both of those fields will be included in the {@link InputEvent} passed to
 * the sum aggregator. And the {DimensionsEventregate} event produced by the sum aggregator will contain two fields,
 * one for cost and one for revenue.
 * </p>
 *
 *
 * @since 3.4.0
 */
public interface IncrementalAggregator extends Aggregator<InputEvent, Aggregate>
{
  /**
   * This method defines the type mapping for the {@link IncrementalAggregator}. The type mapping defines the
   * relationship between the type of an input field and the type of its aggregate. For example if the aggregator takes
   * a field of type int and produces an aggregate of type float, then this method would return a type of float when
   * the given input type is an int.
   * @param inputType The type of a field to be aggregate.
   * @return The type of the aggregate corresponding to an input field of the given type.
   */
  public Type getOutputType(Type inputType);

  /**
   * This sets
   */
  public void setDimensionsConversionContext(DimensionsConversionContext context);

  /**
   * Returns a {@link FieldsDescriptor} object which describes the meta data that is stored along with aggregations.
   * This method returns null if this aggregator stores no metadata.
   * @return A {@link FieldsDescriptor} object which describes the meta data that is stored along with aggregations.
   * This method returns null if this aggregator stores no metadata.
   */
  public FieldsDescriptor getMetaDataDescriptor();
}
