/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.dimensions.AggregatorCount;
import com.datatorrent.lib.appdata.dimensions.AggregatorMax;
import com.datatorrent.lib.appdata.dimensions.AggregatorMin;
import com.datatorrent.lib.appdata.dimensions.AggregatorSum;
import com.datatorrent.lib.appdata.dimensions.DimensionsAggregator;
import com.datatorrent.lib.appdata.dimensions.GenericAggregateEvent;
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
public enum AggType
{
  SUM(new AggregatorSum()),
  MIN(new AggregatorMin()),
  MAX(new AggregatorMax()),
  COUNT(new AggregatorCount());

  private static final Logger logger = LoggerFactory.getLogger(AggType.class);

  private DimensionsAggregator<GenericAggregateEvent> aggregator;
  public static final Map<String, Integer> NAME_TO_ORDINAL;

  static {
    Map<String, Integer> nameToOrdinal = Maps.newHashMap();

    for(AggType aggType: AggType.values()) {
      logger.info("Aggs: {} {}", aggType.name(), aggType.ordinal());
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
