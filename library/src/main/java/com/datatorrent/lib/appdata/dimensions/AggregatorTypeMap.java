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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

public class AggregatorTypeMap
{
  private Map<Type, Type> typeMap;

  public AggregatorTypeMap(Map<Type, Type> typeMap)
  {
    setTypeMap(typeMap);
  }

  private void setTypeMap(Map<Type, Type> typeMap)
  {
    Preconditions.checkNotNull(typeMap);

    for(Map.Entry<Type, Type> entry: typeMap.entrySet()) {
      Preconditions.checkNotNull(entry.getKey(), "Given type map cannot conain a null key.");
      Preconditions.checkNotNull(entry.getValue(), "Given type map cannot contain a null value.");
    }

    //Check if mapping is stable

    for(Type targetType: typeMap.values()) {
      Type targetType2 = typeMap.get(targetType);

      if(targetType != targetType2) {
        throw new IllegalArgumentException("Target types must map to themselved.");
      }
    }

    this.typeMap = Maps.newHashMap();
    this.typeMap.putAll(typeMap);
    this.typeMap = Collections.unmodifiableMap(typeMap);
  }

  public Map<Type, Type> getTypeMap()
  {
    return typeMap;
  }
}
