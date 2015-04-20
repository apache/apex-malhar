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

public enum AggregatorStaticType
{
  SUM(new AggregatorSum()),
  MIN(new AggregatorMin()),
  MAX(new AggregatorMax()),
  COUNT(new AggregatorCount()),
  LAST(new AggregatorLast()),
  FIRST(new AggregatorFirst());

  private static final Logger logger = LoggerFactory.getLogger(AggregatorStaticType.class);

  public static final Map<Class<? extends DimensionsStaticAggregator>, Integer> CLASS_TO_ORDINAL;
  public static final Map<Class<? extends DimensionsStaticAggregator>, String> CLASS_TO_NAME;
  public static final Map<Integer, DimensionsStaticAggregator> ORDINAL_TO_AGGREGATOR;
  public static final Map<String, Integer> NAME_TO_ORDINAL;
  public static final Map<String, DimensionsStaticAggregator> NAME_TO_AGGREGATOR;

  private DimensionsStaticAggregator aggregator;

  static {
    Map<Class<? extends DimensionsStaticAggregator>, Integer> classToOrdinal = Maps.newHashMap();
    Map<Class<? extends DimensionsStaticAggregator>, String> classToName = Maps.newHashMap();
    Map<Integer, DimensionsStaticAggregator> ordinalToAggregator = Maps.newHashMap();
    Map<String, Integer> nameToOrdinal = Maps.newHashMap();
    Map<String, DimensionsStaticAggregator> nameToAggregator = Maps.newHashMap();

    for(AggregatorStaticType aggType: AggregatorStaticType.values()) {
      classToOrdinal.put(aggType.getAggregator().getClass(), aggType.ordinal());
      classToName.put(aggType.getAggregator().getClass(), aggType.name());
      ordinalToAggregator.put(aggType.ordinal(), aggType.getAggregator());
      nameToOrdinal.put(aggType.name(), aggType.ordinal());
      nameToAggregator.put(aggType.name(), aggType.getAggregator());
    }

    CLASS_TO_ORDINAL = Collections.unmodifiableMap(classToOrdinal);
    CLASS_TO_NAME = Collections.unmodifiableMap(classToName);
    ORDINAL_TO_AGGREGATOR = Collections.unmodifiableMap(ordinalToAggregator);
    NAME_TO_ORDINAL = Collections.unmodifiableMap(nameToOrdinal);
    NAME_TO_AGGREGATOR = Collections.unmodifiableMap(nameToAggregator);
  }

  AggregatorStaticType(DimensionsStaticAggregator aggregator)
  {
    setAggregator(aggregator);
  }

  private void setAggregator(DimensionsStaticAggregator aggregator)
  {
    Preconditions.checkNotNull(aggregator);
    this.aggregator = aggregator;
  }

  public DimensionsStaticAggregator getAggregator()
  {
    return aggregator;
  }
}
