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
package com.datatorrent.lib.dimensions.aggregator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public enum AggregatorIncrementalType
{
  SUM(new AggregatorSum()),
  MIN(new AggregatorMin()),
  MAX(new AggregatorMax()),
  COUNT(AggregatorCount.INSTANCE),
  LAST(new AggregatorLast()),
  FIRST(AggregatorFirst.INSTANCE);

  private static final Logger logger = LoggerFactory.getLogger(AggregatorIncrementalType.class);

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
}
