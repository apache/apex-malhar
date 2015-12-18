/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import java.util.Collections;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * This is a convenience enum to store all the information about default {@link OTFAggregator}s
 * in one place.
 *
 * @since 3.1.0
 */
public enum AggregatorOTFType
{
  /**
   * The average {@link OTFAggregator}.
   */
  AVG(AggregatorAverage.INSTANCE);

  /**
   * A map from {@link OTFAggregator} names to {@link OTFAggregator}s.
   */
  public static final Map<String, OTFAggregator> NAME_TO_AGGREGATOR;

  static {
    Map<String, OTFAggregator> nameToAggregator = Maps.newHashMap();

    for (AggregatorOTFType aggType : AggregatorOTFType.values()) {
      nameToAggregator.put(aggType.name(), aggType.getAggregator());
    }

    NAME_TO_AGGREGATOR = Collections.unmodifiableMap(nameToAggregator);
  }

  /**
   * The {@link OTFAggregator} assigned to this enum.
   */
  private OTFAggregator aggregator;

  /**
   * Creates an {@link OTFAggregator} enum with the given aggregator.
   *
   * @param aggregator The {@link OTFAggregator} assigned to this enum.
   */
  AggregatorOTFType(OTFAggregator aggregator)
  {
    setAggregator(aggregator);
  }

  /**
   * Sets the {@link OTFAggregator} assigned to this enum.
   *
   * @param aggregator The {@link OTFAggregator} assigned to this enum.
   */
  private void setAggregator(OTFAggregator aggregator)
  {
    this.aggregator = Preconditions.checkNotNull(aggregator);
  }

  /**
   * Gets the {@link OTFAggregator} assigned to this enum.
   *
   * @return The {@link OTFAggregator} assigned to this enum.
   */
  public OTFAggregator getAggregator()
  {
    return aggregator;
  }
}
