/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.dimensions;

import com.datatorrent.lib.appdata.dimensions.AggregatorCount;
import com.datatorrent.lib.appdata.dimensions.AggregatorMax;
import com.datatorrent.lib.appdata.dimensions.AggregatorMin;
import com.datatorrent.lib.appdata.dimensions.AggregatorSum;
import com.datatorrent.lib.appdata.dimensions.DimensionsAggregator;
import com.datatorrent.lib.appdata.dimensions.GenericAggregateEvent;
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
      new AggregatorMin(),
      null),
  MAX("max",
      new AggregatorMax(),
      null),
  SUM("sum",
      new AggregatorSum(),
      null),
  COUNT("count",
        new AggregatorCount(),
        Type.LONG);

  public static final Map<String, AggregatorType> NAME_TO_AGGREGATOR_TYPE;
  public static final Map<Integer, AggregatorType> ID_TO_AGGREGATOR_TYPE;

  static
  {
    Map<String, AggregatorType> nameToAggregatorType = Maps.newHashMap();
    Map<Integer, AggregatorType> idToAggregatorType = Maps.newHashMap();

    for(AggregatorType at: AggregatorType.values()) {
      nameToAggregatorType.put(at.getName(), at);
      idToAggregatorType.put(at.ordinal(), at);
    }

    NAME_TO_AGGREGATOR_TYPE = Collections.unmodifiableMap(nameToAggregatorType);
    ID_TO_AGGREGATOR_TYPE = Collections.unmodifiableMap(idToAggregatorType);
  }

  private String name;
  private DimensionsAggregator<GenericAggregateEvent> aggregator;
  private Type type;

  AggregatorType(String name,
                 DimensionsAggregator<GenericAggregateEvent> aggregator,
                 Type type)
  {
    setName(name);
    setAggregator(aggregator);
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

  private void setAggregator(DimensionsAggregator<GenericAggregateEvent> aggregator)
  {
    Preconditions.checkNotNull(aggregator);
    this.aggregator = aggregator;
  }

  public DimensionsAggregator<GenericAggregateEvent> getAggregator()
  {
    return aggregator;
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
