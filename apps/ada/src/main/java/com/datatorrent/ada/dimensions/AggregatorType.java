/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.dimensions;

import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public enum AggregatorType
{
  MIN("min",
      new AggregatorFactoryMin(),
      null),
  MAX("max",
      new AggregatorFactoryMax(),
      null),
  SUM("sum",
      new AggregatorFactorySum(),
      null),
  COUNT("count",
        new AggregatorFactorySum(),
        Type.LONG);

  public static final Map<String, AggregatorType> NAME_TO_AGGREGATOR_TYPE;

  static
  {
    Map<String, AggregatorType> nameToAggregatorType = Maps.newHashMap();

    for(AggregatorType at: AggregatorType.values()) {
      nameToAggregatorType.put(at.getName(), at);
    }

    NAME_TO_AGGREGATOR_TYPE = Collections.unmodifiableMap(nameToAggregatorType);
  }

  private String name;
  private DimensionsAggregatorFactory factory;
  private Type type;

  AggregatorType(String name,
                 DimensionsAggregatorFactory factory,
                 Type type)
  {
    setName(name);
    setFactory(factory);
    setType(type);
  }

  private void setName(String name)
  {
    Preconditions.checkNotNull(name);
    this.name = name;
  }

  public String getName()
  {
    return name;
  }

  private void setFactory(DimensionsAggregatorFactory factory)
  {
    Preconditions.checkNotNull(factory);
    this.factory = factory;
  }

  public DimensionsAggregatorFactory getFactory()
  {
    return factory;
  }

  private void setType(Type type)
  {
    this.type = type;
  }

  public Type getType()
  {
    return type;
  }
}
