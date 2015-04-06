/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
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

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
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
