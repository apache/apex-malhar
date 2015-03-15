/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public interface DimensionsSchema
{
  public FieldsDescriptor getKeyFieldDescriptor();
  public FieldsDescriptor getAggregateFieldDescriptor();
  public FieldsDescriptor getAllFieldsDescriptor();
}
