/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.google.common.base.Preconditions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public abstract class GenericAggregator implements DimensionsAggregator<GenericEvent, GenericAggregateEvent>
{
  private static final long serialVersionUID = 201503110151L;

  private AggregatorDescriptor aggregatorDescriptor;

  public GenericAggregator(AggregatorDescriptor aggregatorDescriptor)
  {
    setAggregatorDescriptor(aggregatorDescriptor);
  }

  private void setAggregatorDescriptor(AggregatorDescriptor aggregatorDescriptor)
  {
    Preconditions.checkNotNull(aggregatorDescriptor);
    this.aggregatorDescriptor = aggregatorDescriptor;
  }

  public AggregatorDescriptor getAggregatorDescriptor()
  {
    return aggregatorDescriptor;
  }
}
