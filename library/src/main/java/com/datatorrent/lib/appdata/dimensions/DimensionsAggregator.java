/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public interface DimensionsAggregator<AGGREGATE>
{
  public void aggregate(AGGREGATE dest, AGGREGATE src);
}
