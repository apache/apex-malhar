/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.lib.appdata.dimensions.AggregatorCount;
import com.datatorrent.lib.appdata.dimensions.AggregatorMax;
import com.datatorrent.lib.appdata.dimensions.AggregatorMin;
import com.datatorrent.lib.appdata.dimensions.AggregatorSum;
import com.datatorrent.lib.appdata.dimensions.DimensionsAggregator;
import com.datatorrent.lib.appdata.dimensions.GenericAggregateEvent;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public enum AggType
{
  MIN(new AggregatorMin()),
  MAX(new AggregatorMax()),
  SUM(new AggregatorSum()),
  COUNT(new AggregatorCount());

  private DimensionsAggregator<GenericAggregateEvent> aggregator;
  public static final Map<String, Integer> NAME_TO_ORDINAL;

  static {
    Map<String, Integer> nameToOrdinal = Maps.newHashMap();

    for(AggType aggType: AggType.values()) {
      nameToOrdinal.put(aggType.name(), aggType.ordinal());
    }

    NAME_TO_ORDINAL = Collections.unmodifiableMap(nameToOrdinal);
  }

  AggType(DimensionsAggregator<GenericAggregateEvent> aggregator)
  {
    setAggregator(aggregator);
  }

  private void setAggregator(DimensionsAggregator<GenericAggregateEvent> aggregator)
  {
    Preconditions.checkNotNull(aggregator);
    this.aggregator = aggregator;
  }

  public DimensionsAggregator<GenericAggregateEvent> getAggregator()
  {
    return aggregator;
  }
}
