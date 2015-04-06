/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public enum AggregatorType
{
  SUM(new AggregatorSum()),
  MIN(new AggregatorMin()),
  MAX(new AggregatorMax()),
  COUNT(new AggregatorCount());

  private static final Logger logger = LoggerFactory.getLogger(AggregatorType.class);

  private GenericDimensionsAggregator aggregator;
  public static final Map<String, Integer> NAME_TO_ORDINAL;

  static {
    Map<String, Integer> nameToOrdinal = Maps.newHashMap();

    for(AggregatorType aggType: AggregatorType.values()) {
      logger.info("Aggs: {} {}", aggType.name(), aggType.ordinal());
      nameToOrdinal.put(aggType.name(), aggType.ordinal());
    }

    NAME_TO_ORDINAL = Collections.unmodifiableMap(nameToOrdinal);
  }

  AggregatorType(GenericDimensionsAggregator aggregator)
  {
    setAggregator(aggregator);
  }

  private void setAggregator(GenericDimensionsAggregator aggregator)
  {
    Preconditions.checkNotNull(aggregator);
    this.aggregator = aggregator;
  }

  public GenericDimensionsAggregator getAggregator()
  {
    return aggregator;
  }
}
