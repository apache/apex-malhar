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
package com.datatorrent.lib.dimensions;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.google.common.collect.Maps;
import java.io.Serializable;

import java.util.Collections;
import java.util.Map;

public class AggregatorCount implements IncrementalAggregator, Serializable
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
  public void aggregate(Aggregate dest, InputEvent src)
  {
    long[] fieldsLong = dest.getAggregates().getFieldsLong();

    for(int index = 0;
        index < fieldsLong.length;
        index++) {
      fieldsLong[index]++;
    }
  }

  @Override
  public void aggregate(Aggregate destAgg, Aggregate srcAgg)
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

  @Override
  public Aggregate createDest(InputEvent first)
  {
    FieldsDescriptor outputFD = AggregatorUtils.getOutputFieldsDescriptor(first.getAggregates().getFieldDescriptor(),
                                                                          this);
    GPOMutable aggregates = new GPOMutable(outputFD);

    long[] aggLongs = aggregates.getFieldsLong();

    for(int index = 0;
        index < aggLongs.length;
        index++) {
      aggLongs[index] = 1L;
    }

    return new Aggregate(first.getEventKey(), aggregates);
  }
}
