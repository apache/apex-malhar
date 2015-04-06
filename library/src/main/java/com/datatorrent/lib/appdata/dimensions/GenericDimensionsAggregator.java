/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;

import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public interface GenericDimensionsAggregator
{
  public Map<Type, Type> getTypeConversionMap();
  public FieldsDescriptor getResultDescriptor(FieldsDescriptor fd);
  public GenericAggregateEvent createDest(GenericAggregateEvent first, FieldsDescriptor fd);
  public void aggregate(GenericAggregateEvent dest, GenericAggregateEvent src);
}
