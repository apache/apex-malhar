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

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AggregatorCount implements GenericDimensionsAggregator
{
  public static final Map<Type, Type> TYPE_CONVERSION_MAP;

  static {
    Map<Type, Type> typeConversionMap = Maps.newHashMap();

    for(Type type: Type.values()) {
      typeConversionMap.put(type, Type.LONG);
    }

    TYPE_CONVERSION_MAP = Collections.unmodifiableMap(typeConversionMap);
  }

  public AggregatorCount()
  {
  }

  @Override
  public void aggregate(GenericAggregateEvent dest, GenericAggregateEvent src)
  {
    dest.getAggregates().getFieldsLong()[0]++;
  }

  @Override
  public Map<Type, Type> getTypeConversionMap()
  {
    return TYPE_CONVERSION_MAP;
  }

  @Override
  public FieldsDescriptor getResultDescriptor(FieldsDescriptor fd)
  {
    Set<Type> compressedTypes = Sets.newHashSet();
    compressedTypes.add(Type.LONG);

    Map<String, Type> fieldToType = Maps.newHashMap();

    List<String> fields = fd.getFieldList();

    for(int index = 0;
        index < fields.size();
        index++) {
      String field = fields.get(index);

      fieldToType.put(field, Type.LONG);
    }

    return new FieldsDescriptor(fieldToType,
                                compressedTypes);
  }

  @Override
  public GenericAggregateEvent createDest(GenericAggregateEvent first, FieldsDescriptor fd)
  {
    GPOMutable aggregates = new GPOMutable(fd);
    aggregates.getFieldsLong()[0] = 1;

    return new GenericAggregateEvent(first.getEventKey(), aggregates);
  }
}
