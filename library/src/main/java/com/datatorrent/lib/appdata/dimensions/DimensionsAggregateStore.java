/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public interface DimensionsAggregateStore<EVENT, AGGREGATE extends DimensionsAggregate<AGGREGATOR_ID>, AGGREGATOR_ID>
{
  public AGGREGATE getAggregate(EVENT event, AGGREGATOR_ID aggregatorID);
  public void put(EVENT event, AGGREGATE aggregate);
}
