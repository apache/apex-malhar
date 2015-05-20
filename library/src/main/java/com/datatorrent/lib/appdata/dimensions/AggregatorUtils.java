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

import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

public final class AggregatorUtils
{
  private static transient final Map<String, DimensionsIncrementalAggregator> DEFAULT_NAME_TO_INCREMENTAL_AGGREGATOR;
  private static transient final Map<String, DimensionsOTFAggregator> DEFAULT_NAME_TO_OTF_AGGREGATOR;

  static {
    DEFAULT_NAME_TO_INCREMENTAL_AGGREGATOR = Maps.newHashMap(AggregatorStaticType.NAME_TO_AGGREGATOR);
    DEFAULT_NAME_TO_OTF_AGGREGATOR = Maps.newHashMap(AggregatorOTFType.NAME_TO_AGGREGATOR);
  }

  public static final AggregatorInfo DEFAULT_AGGREGATOR_INFO = new AggregatorInfo(DEFAULT_NAME_TO_INCREMENTAL_AGGREGATOR,
                                                                                  DEFAULT_NAME_TO_OTF_AGGREGATOR,
                                                                                  AggregatorStaticType.NAME_TO_ORDINAL);

  public static transient final Map<Type, Type> IDENTITY_TYPE_MAP;
  public static transient final Map<Type, Type> IDENTITY_NUMBER_TYPE_MAP;

  static {
    Map<Type, Type> identityTypeMap = Maps.newHashMap();

    for(Type type: Type.values()) {
      identityTypeMap.put(type, type);
    }

    IDENTITY_TYPE_MAP = Collections.unmodifiableMap(identityTypeMap);

    Map<Type, Type> identityNumberTypeMap = Maps.newHashMap();

    for(Type type: Type.NUMERIC_TYPES) {
      identityNumberTypeMap.put(type, type);
    }

    IDENTITY_NUMBER_TYPE_MAP = Collections.unmodifiableMap(identityNumberTypeMap);
  }

  private AggregatorUtils()
  {
    //Don't instantiate this class.
  }
}
