/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.counters;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public final class CountersSchemaUtils
{
  private static final Map<DataGroup, CountersSchema> groupToSchema = Maps.newHashMap();

  private CountersSchemaUtils()
  {
  }

  public static void addSchema(CountersSchema schema)
  {
    groupToSchema.put(schema.getDataGroup(), schema);
  }

  public static CountersSchema getSchema(CountersUpdateDataLogical cud)
  {
    return groupToSchema.get(cud.getDataGroup());
  }
}
