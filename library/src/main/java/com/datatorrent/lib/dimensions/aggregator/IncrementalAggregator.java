/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema.DimensionsConversionContext;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.statistics.DimensionsComputation.Aggregator;

/**
 * <p>
 * {@link IncrementalAggregators} perform aggregations in place, on a field by field basis.
 * For example if we have a field cost, an incremental aggregator would take a new value of cost and aggregate it to an aggregate value
 * for cost. No fields except the cost field are used in the computation of the cost aggregation in the case of an {@link IncrementalAggregator}.
 * </p>
 * <p>
 * {@link IncrementalAggregator}s are intended to be used with subclasses of {@link AbstractiDimensionsComputationFlexibleSingleSchema}. The
 * way in which {@link IncrementalAggregator}s are used in this context is that a batch of fields to be aggregated by the aggregator are provided in the
 * form of an {@link InputEvent}. For example, if there are two fields (cost and revenue), which will be aggregated by a sum aggregator, both
 * of those fields will be included in the {@link InputEvent} passed to the sum aggregator. And the {DimensionsEventregate} event produced by the
 * sum aggregator will contain two fields, one for cost and one for revenue.
 * </p>
 * @since 3.1.0
 *
 *
 */
public interface IncrementalAggregator extends Aggregator<InputEvent, Aggregate>
{
  /**
   * This method defines the type mapping for the {@link IncrementalAggregator}. The type mapping defines the relationship
   * between the type of an input field and the type of its aggregate. For example if the aggregator
   * takes a field of type int and produces an aggregate of type float, then this method would return a type of float when
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
