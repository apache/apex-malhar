/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import java.util.Collections;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 3.1.0
 */


public enum AggregatorIncrementalType
{
  SUM(new AggregatorSum()),
  MIN(new AggregatorMin()),
  MAX(new AggregatorMax()),
  COUNT(new AggregatorCount()),
  LAST(new AggregatorLast()),
  FIRST(new AggregatorFirst()),
  CUM_SUM(new AggregatorCumSum());

  public static final Map<String, Integer> NAME_TO_ORDINAL;
  public static final Map<String, IncrementalAggregator> NAME_TO_AGGREGATOR;

  private IncrementalAggregator aggregator;

  static {
    Map<String, Integer> nameToOrdinal = Maps.newHashMap();
    Map<String, IncrementalAggregator> nameToAggregator = Maps.newHashMap();

    for(AggregatorIncrementalType aggType: AggregatorIncrementalType.values()) {
      nameToOrdinal.put(aggType.name(), aggType.ordinal());
      nameToAggregator.put(aggType.name(), aggType.getAggregator());
    }

    NAME_TO_ORDINAL = Collections.unmodifiableMap(nameToOrdinal);
    NAME_TO_AGGREGATOR = Collections.unmodifiableMap(nameToAggregator);
  }

  AggregatorIncrementalType(IncrementalAggregator aggregator)
  {
    setAggregator(aggregator);
  }

  private void setAggregator(IncrementalAggregator aggregator)
  {
    Preconditions.checkNotNull(aggregator);
    this.aggregator = aggregator;
  }

  public IncrementalAggregator getAggregator()
  {
    return aggregator;
  }

  private static final Logger LOG = LoggerFactory.getLogger(AggregatorIncrementalType.class);
}
