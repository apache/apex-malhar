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
public class AggregatorSum extends GenericAggregator
{
  public AggregatorSum(AggregatorDescriptor aggregatorDescriptor)
  {
    super(aggregatorDescriptor);
  }

  @Override
  public void aggregate(GenericAggregateEvent dest, GenericEvent src)
  {
    aggregate(dest.getAggregates(), src.getAggregates());
  }

  @Override
  public void aggregateAgs(GenericAggregateEvent dest, GenericAggregateEvent src)
  {
    aggregate(dest.getAggregates(), src.getAggregates());
  }

  private void aggregate(GPOMutable dest, GPOMutable src)
  {
    for(String field: dest.getFieldDescriptor().getFields().getFields()) {
      Object destObj = dest.getField(field);
      Object srcObj = src.getField(field);

      if(!srcObj.getClass().equals(destObj)) {
        throw new UnsupportedOperationException("Cannot aggregate different types.");
      }
      else if(srcObj instanceof Byte) {
        Byte srcObjTemp = (Byte) srcObj;
        Byte destObjTemp = (Byte) destObj;

        Byte res = (byte) (srcObjTemp + destObjTemp);
        dest.setField(field, res);
      }
      else if(srcObj instanceof Short) {
        Short srcObjTemp = (Short) srcObj;
        Short destObjTemp = (Short) destObj;

        Short res = (short) (srcObjTemp + destObjTemp);
        dest.setField(field, res);
      }
      else if(srcObj instanceof Integer) {
        Integer srcObjTemp = (Integer) srcObj;
        Integer destObjTemp = (Integer) destObj;

        Integer res = (srcObjTemp + destObjTemp);
        dest.setField(field, res);
      }
      else if(srcObj instanceof Long) {
        Long srcObjTemp = (Long) srcObj;
        Long destObjTemp = (Long) destObj;

        Long res = srcObjTemp + destObjTemp;
        dest.setField(field, res);
      }
      else if(srcObj instanceof Float) {
        Float srcObjTemp = (Float) srcObj;
        Float destObjTemp = (Float) destObj;

        Float res = srcObjTemp + destObjTemp;
        dest.setField(field, res);
      }
      else if(srcObj instanceof Double) {
        Double srcObjTemp = (Double) srcObj;
        Double destObjTemp = (Double) destObj;

        Double res = srcObjTemp + destObjTemp;
        dest.setField(field, res);
      }
    }
  }
}
