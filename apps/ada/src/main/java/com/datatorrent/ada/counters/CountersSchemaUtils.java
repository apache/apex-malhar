/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.counters;

import com.datatorrent.ada.dimensions.AggregatorType;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public final class CountersSchemaUtils
{
  private static final Map<DataGroup, CountersSchema> groupToSchema = Maps.newHashMap();
  private static final Map<DataGroup, Set<AggregatorType>> groupToAggregatorTypes = Maps.newHashMap();

  private CountersSchemaUtils()
  {
  }

  public static void addSchema(CountersSchema schema)
  {
    DataGroup dg = schema.getDataGroup();
    groupToSchema.put(dg, schema);

    Set<AggregatorType> aggregatorTypes = Sets.newHashSet();

    for(String aggregator: schema.getAggregators()) {
      aggregatorTypes.add(AggregatorType.NAME_TO_AGGREGATOR_TYPE.get(aggregator));
    }

    groupToAggregatorTypes.put(dg, aggregatorTypes);
  }

  public static CountersSchema getSchema(CountersUpdateDataLogical cud)
  {
    return groupToSchema.get(cud.getDataGroup());
  }

  public static Set<AggregatorType> getAggregatorTypes(CountersUpdateDataLogical cud)
  {
    return groupToAggregatorTypes.get(cud.getDataGroup());
  }
}
