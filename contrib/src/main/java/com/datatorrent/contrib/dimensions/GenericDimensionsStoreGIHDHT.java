/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.dimensions;

import com.datatorrent.lib.appdata.dimensions.GenericAggregateEvent;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public abstract class GenericDimensionsStoreGIHDHT extends GenericDimensionsStoreAIHDHT<GenericAggregateEvent>
{
  public GenericDimensionsStoreGIHDHT()
  {
  }

  @Override
  protected void processInputEvent(GenericAggregateEvent gae)
  {
      processGenericEvent(gae);
  }

  @Override
  protected GenericAggregateEvent[] convertInput(GenericAggregateEvent tuple)
  {
    throw new UnsupportedOperationException("Conversion is not supported");
  }
}
