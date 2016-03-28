/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.dimensions.aggregator;

import java.util.Collections;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

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

    for (AggregatorIncrementalType aggType : AggregatorIncrementalType.values()) {
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
