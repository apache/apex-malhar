/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import com.datatorrent.api.annotation.Name;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.Maps;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;

/**
 * This {@link IncrementalAggregator} performs a count of the number of times an input is encountered.
 */
@Name("COUNT")
public class AggregatorCount extends AbstractIncrementalAggregator
{
  private static final long serialVersionUID = 20154301645L;

  /**
   * This is a map whose keys represent input types and whose values
   * represent the corresponding output types.
   */
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

    for(int index = 0;
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

    for(int index = 0;
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

    for(int index = 0;
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
