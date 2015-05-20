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
import java.io.Serializable;

import java.util.Collections;
import java.util.Map;

public class AggregatorCount implements DimensionsIncrementalAggregator, Serializable
{
  private static final long serialVersionUID = 20154301645L;
  public static transient final Map<Type, Type> TYPE_CONVERSION_MAP;

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
  public void aggregate(AggregateEvent dest, InputEvent src)
  {
    dest.getAggregates().getFieldsLong()[0]++;
  }

  @Override
  public void aggregateAggs(AggregateEvent destAgg, AggregateEvent srcAgg)
  {
    long[] destLongs = destAgg.getAggregates().getFieldsLong();
    long[] srcLongs = srcAgg.getAggregates().getFieldsLong();

    for(int index = 0;
        index < destLongs.length;
        index++) {
      destLongs[index] += srcLongs[index];
    }
  }

  @Override
  public Type getOutputType(Type inputType)
  {
    return TYPE_CONVERSION_MAP.get(inputType);
  }

/*  @Override
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
*/
  @Override
  public AggregateEvent createDest(AggregateEvent first, FieldsDescriptor fd)
  {
    GPOMutable aggregates = new GPOMutable(fd);
    aggregates.getFieldsLong()[0] = 1;

    return new AggregateEvent(first.getEventKey(), aggregates);
  }
}
