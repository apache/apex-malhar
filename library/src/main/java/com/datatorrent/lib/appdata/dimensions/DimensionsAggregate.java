/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public interface DimensionsAggregate<AGGREGATE_ID>
{
  public AGGREGATE_ID getAggregateID();
}
