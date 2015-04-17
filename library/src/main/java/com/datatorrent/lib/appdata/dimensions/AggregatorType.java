/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.appdata.dimensions;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public enum AggregatorType
{
  SUM(new AggregatorSum()),
  MIN(new AggregatorMin()),
  MAX(new AggregatorMax()),
  COUNT(new AggregatorCount()),
  LAST(new AggregatorLast()),
  FIRST(new AggregatorFirst());

  private static final Logger logger = LoggerFactory.getLogger(AggregatorType.class);

  public static final Map<Integer, DimensionsAggregator> ORDINAL_TO_AGGREGATOR;
  public static final Map<String, Integer> NAME_TO_ORDINAL;
  public static final Map<String, DimensionsAggregator> NAME_TO_AGGREGATOR;

  private DimensionsAggregator aggregator;

  static {
    Map<Integer, DimensionsAggregator> ordinalToAggregator = Maps.newHashMap();
    Map<String, Integer> nameToOrdinal = Maps.newHashMap();
    Map<String, DimensionsAggregator> nameToAggregator = Maps.newHashMap();

    for(AggregatorType aggType: AggregatorType.values()) {
      ordinalToAggregator.put(aggType.ordinal(), aggType.getAggregator());
      nameToOrdinal.put(aggType.name(), aggType.ordinal());
      nameToAggregator.put(aggType.name(), aggType.getAggregator());
    }

    ORDINAL_TO_AGGREGATOR = Collections.unmodifiableMap(ordinalToAggregator);
    NAME_TO_ORDINAL = Collections.unmodifiableMap(nameToOrdinal);
    NAME_TO_AGGREGATOR = Collections.unmodifiableMap(nameToAggregator);
  }

  AggregatorType(DimensionsAggregator aggregator)
  {
    setAggregator(aggregator);
  }

  private void setAggregator(DimensionsAggregator aggregator)
  {
    Preconditions.checkNotNull(aggregator);
    this.aggregator = aggregator;
  }

  public DimensionsAggregator getAggregator()
  {
    return aggregator;
  }
}
