/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public interface DimensionsAggregator<EVENT, AGGREGATE>
{
  public void aggregate(AGGREGATE dest, EVENT src);
  public void aggregateAgs(AGGREGATE dest, AGGREGATE src);
}
