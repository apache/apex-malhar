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

import java.util.Map;

public final class AggregatorUtils
{
  public static final AggregatorInfo DEFAULT_AGGREGATOR_INFO = new AggregatorInfo(AggregatorStaticType.CLASS_TO_NAME,
                                                                                  AggregatorStaticType.NAME_TO_AGGREGATOR,
                                                                                  AggregatorStaticType.NAME_TO_ORDINAL,
                                                                                  AggregatorOTFType.NAME_TO_AGGREGATOR);

  public static final AggregatorTypeMap IDENTITY_TYPE_MAP;
  public static final AggregatorTypeMap IDENTITY_NUMBER_TYPE_MAP;

  static {
    Map<Type, Type> identityTypeMap = Maps.newHashMap();

    for(Type type: Type.values()) {
      identityTypeMap.put(type, type);
    }

    IDENTITY_TYPE_MAP = new AggregatorTypeMap(identityTypeMap);

    Map<Type, Type> identityNumberTypeMap = Maps.newHashMap();

    for(Type type: Type.NUMERIC_TYPES) {
      identityNumberTypeMap.put(type, type);
    }

    IDENTITY_NUMBER_TYPE_MAP = new AggregatorTypeMap(identityNumberTypeMap);
  }

  private AggregatorUtils()
  {
    //Don't instantiate this class.
  }
}
