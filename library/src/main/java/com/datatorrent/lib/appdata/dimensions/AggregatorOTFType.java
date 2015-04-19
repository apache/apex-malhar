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

import java.util.Collections;
import java.util.Map;

public enum AggregatorOTFType
{
  AVG(new AggregatorAverage());

  public static final Map<Class<? extends DimensionsStaticAggregator>, String> CLASS_TO_NAME;
  public static final Map<String, DimensionsStaticAggregator> NAME_TO_AGGREGATOR;

  private DimensionsOTFAggregator aggregator;

  static {
    Map<Class<? extends DimensionsStaticAggregator>, String> classToName = Maps.newHashMap();
    Map<String, DimensionsStaticAggregator> nameToAggregator = Maps.newHashMap();

    for(AggregatorStaticType aggType: AggregatorStaticType.values()) {
      classToName.put(aggType.getAggregator().getClass(), aggType.name());
      nameToAggregator.put(aggType.name(), aggType.getAggregator());
    }

    CLASS_TO_NAME = Collections.unmodifiableMap(classToName);
    NAME_TO_AGGREGATOR = Collections.unmodifiableMap(nameToAggregator);
  }

  AggregatorOTFType(DimensionsOTFAggregator aggregator)
  {
    setAggregator(aggregator);
  }

  private void setAggregator(DimensionsOTFAggregator aggregator)
  {
    Preconditions.checkNotNull(aggregator);
    this.aggregator = aggregator;
  }

  public DimensionsOTFAggregator getAggregator()
  {
    return aggregator;
  }
}
