/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.gpo.GPOMutable;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AggregatorCount implements DimensionsAggregator<GenericAggregateEvent>
{
  public AggregatorCount()
  {
  }

  @Override
  public void aggregate(GenericAggregateEvent dest, GenericAggregateEvent src)
  {
    GPOMutable destGPO = dest.getAggregates();
    GPOMutable srcGPO = src.getAggregates();

    for(String field: destGPO.getFieldDescriptor().getFields().getFields()) {
      Long srcObjTemp = (Long) srcGPO.getField(field);
      Long destObjTemp = (Long) destGPO.getField(field);

      Long res = srcObjTemp + destObjTemp;
      destGPO.setField(field, res);
    }
  }
}
