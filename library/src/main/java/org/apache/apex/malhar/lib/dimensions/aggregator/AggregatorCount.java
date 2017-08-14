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

import org.apache.apex.malhar.lib.appdata.gpo.GPOMutable;
import org.apache.apex.malhar.lib.appdata.gpo.GPOUtils;
import org.apache.apex.malhar.lib.appdata.schemas.FieldsDescriptor;
import org.apache.apex.malhar.lib.appdata.schemas.Type;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.Aggregate;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.EventKey;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.InputEvent;

import com.google.common.collect.Maps;

import com.datatorrent.api.annotation.Name;

/**
 * This {@link IncrementalAggregator} performs a count of the number of times an input is encountered.
 *
 * @since 3.1.0
 */
@Name("COUNT")
public class AggregatorCount extends AbstractIncrementalAggregator
{
  private static final long serialVersionUID = 20154301645L;

  /**
   * This is a map whose keys represent input types and whose values
   * represent the corresponding output types.
   */
  public static final transient Map<Type, Type> TYPE_CONVERSION_MAP;

  static {
    Map<Type, Type> typeConversionMap = Maps.newHashMap();

    for (Type type : Type.values()) {
      typeConversionMap.put(type, Type.LONG);
    }

    TYPE_CONVERSION_MAP = Collections.unmodifiableMap(typeConversionMap);
  }

  public AggregatorCount()
  {
    //Do nothing
  }

  @Override
  public Aggregate getGroup(InputEvent src, int aggregatorIndex)
  {
    src.used = true;
    GPOMutable aggregates = new GPOMutable(context.aggregateDescriptor);
    GPOMutable keys = new GPOMutable(context.keyDescriptor);
    GPOUtils.indirectCopy(keys, src.getKeys(), context.indexSubsetKeys);

    EventKey eventKey = createEventKey(src,
        context,
        aggregatorIndex);

    long[] longFields = aggregates.getFieldsLong();

    for (int index = 0;
        index < longFields.length;
        index++) {
      longFields[index] = 0;
    }

    return new Aggregate(eventKey,
        aggregates);
  }

  @Override
  public void aggregate(Aggregate dest, InputEvent src)
  {
    long[] fieldsLong = dest.getAggregates().getFieldsLong();

    for (int index = 0;
        index < fieldsLong.length;
        index++) {
      //increment count
      fieldsLong[index]++;
    }
  }

  @Override
  public void aggregate(Aggregate destAgg, Aggregate srcAgg)
  {
    long[] destLongs = destAgg.getAggregates().getFieldsLong();
    long[] srcLongs = srcAgg.getAggregates().getFieldsLong();

    for (int index = 0;
        index < destLongs.length;
        index++) {
      //aggregate count
      destLongs[index] += srcLongs[index];
    }
  }

  @Override
  public Type getOutputType(Type inputType)
  {
    return TYPE_CONVERSION_MAP.get(inputType);
  }

  @Override
  public FieldsDescriptor getMetaDataDescriptor()
  {
    return null;
  }
}
