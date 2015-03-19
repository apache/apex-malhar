/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 *
 * Example schema:
 *
 * {"keys":
 *   [{"name":"keyName1","type":"type1"},
 *    {"name":"keyName2","type":"type2"}]
 *  "values":
 *   [{"name":"valueName1","type":"type1","aggregations":["min","max,"sum","count"]},
 *    {"name":"valueName2","type":"type2","aggregations":["min","max","sum","count"]}]
 *  "aggregations":
 *   [{"descriptor":"keyName1:keyName2",aggregations:["min","max"]},
 *    {"descriptor":"keyName1",aggregations:["sum"]}]
 *
 */
public class GenericEventSchema
{
  public static final String FIELD_KEYS = "keys";
  public static final String FIELD_KEYS_NAME = "name";
  public static final String FIELD_KEYS_TYPE = "type";

  public static final String FIELD_VALUES = "values";
  public static final String FIELD_VALUES_NAME = "name";
  public static final String FIELD_VALUES_TYPE = "type";
  public static final String FIELD_VALUES_AGGREGATIONS = "aggregations";

  public static final String FIELD_AGGREGATIONS = "aggregations";
  public static final String FIELD_AGGREGATIONS_DESCRIPTOR = "descriptor";
  public static final String FIELD_AGGREGATIONS_AGGREGATIONS = "aggregations";

  private FieldsDescriptor keyDescriptor;
  private Map<Integer, FieldsDescriptor> ddIDToKeyDescriptor;
  private Map<Integer, Map<String, FieldsDescriptor>> ddIDToAggregatorToAggregateDescriptor;

  public GenericEventSchema()
  {
  }

  public GenericEventSchema(String json)
  {
    try {
      initialize(json);
    }
    catch(Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  private void initialize(String json) throws Exception
  {
    JSONObject jo = new JSONObject(json);

    JSONArray keysArray = jo.getJSONArray(FIELD_KEYS);
    Map<String, Type> fieldToType = Maps.newHashMap();

    for(int keyIndex = 0;
        keyIndex < keysArray.length();
        keyIndex++) {
      JSONObject keyDescriptor = keysArray.getJSONObject(keyIndex);
      String name = keyDescriptor.getString(FIELD_KEYS_NAME);
      String type = keyDescriptor.getString(FIELD_KEYS_TYPE);

      fieldToType.put(name, Type.NAME_TO_TYPE.get(type));
    }

    //Key descriptor all
    keyDescriptor = new FieldsDescriptor(fieldToType);

    JSONArray valuesArray = jo.getJSONArray(FIELD_VALUES);
    Map<String, Map<String, Type>> aggregatorToFieldToType = Maps.newHashMap();

    for(int valueIndex = 0;
        valueIndex < valuesArray.length();
        valueIndex++) {
      JSONObject value = valuesArray.getJSONObject(valueIndex);
      String name = value.getString(FIELD_VALUES_NAME);
      String type = value.getString(FIELD_VALUES_TYPE);
      Type typeT = Type.NAME_TO_TYPE.get(type);

      JSONArray aggregators = value.getJSONArray(FIELD_VALUES_AGGREGATIONS);

      for(int aggregatorIndex = 0;
          aggregatorIndex < aggregators.length();
          aggregatorIndex++) {
        String aggregatorName = aggregators.getString(aggregatorIndex);

        Map<String, Type> aggregatorFieldToType = aggregatorToFieldToType.get(aggregatorName);

        if(aggregatorFieldToType == null) {
          aggregatorFieldToType = Maps.newHashMap();
          aggregatorToFieldToType.put(aggregatorName, aggregatorFieldToType);
        }

        aggregatorFieldToType.put(name, typeT);
      }
    }

    //Aggregator to fields descriptor
    Map<String, FieldsDescriptor> aggregatorToFieldsDescriptor = Maps.newHashMap();

    for(Map.Entry<String, Map<String, Type>> entry: aggregatorToFieldToType.entrySet()) {
      FieldsDescriptor descriptor = aggregatorToFieldsDescriptor.get(entry.getValue());
      aggregatorToFieldsDescriptor.put(entry.getKey(), descriptor);
    }

    ddIDToKeyDescriptor = Maps.newHashMap();
    ddIDToAggregatorToAggregateDescriptor = Maps.newHashMap();

    JSONArray aggregationsArray = jo.getJSONArray(FIELD_AGGREGATIONS);
    Set<String> dimensionsDescriptors = Sets.newHashSet();

    for(int aggregationsIndex = 0;
        aggregationsIndex < aggregationsArray.length();
        aggregationsIndex++) {
      JSONObject aggregation = aggregationsArray.getJSONObject(aggregationsIndex);

      String descriptor = aggregation.getString(FIELD_AGGREGATIONS_DESCRIPTOR);

      if(dimensionsDescriptors.contains(descriptor)) {
        throw new IllegalArgumentException("Duplicate dimension descriptor: " + descriptor);
      }

      dimensionsDescriptors.add(descriptor);
      DimensionsDescriptor dimensionsDescriptor = new DimensionsDescriptor(descriptor);

      ddIDToKeyDescriptor.put(aggregationsIndex, keyDescriptor.getSubset(dimensionsDescriptor.getFields()));

      JSONArray aggArray = aggregation.getJSONArray(FIELD_AGGREGATIONS_AGGREGATIONS);
      Map<String, FieldsDescriptor> specificAggregatorToFieldsDescriptor = Maps.newHashMap();

      for(int aggIndex = 0;
          aggIndex < aggArray.length();
          aggIndex++) {
        String aggregator = aggArray.getString(aggIndex);
        specificAggregatorToFieldsDescriptor.put(aggregator, aggregatorToFieldsDescriptor.get(aggregator));
      }

      specificAggregatorToFieldsDescriptor = Collections.unmodifiableMap(specificAggregatorToFieldsDescriptor);
      ddIDToAggregatorToAggregateDescriptor.put(aggregationsIndex, specificAggregatorToFieldsDescriptor);
    }

    ddIDToKeyDescriptor = Collections.unmodifiableMap(ddIDToKeyDescriptor);
    ddIDToAggregatorToAggregateDescriptor = Collections.unmodifiableMap(ddIDToAggregatorToAggregateDescriptor);
  }

  public FieldsDescriptor getAllKeysDescriptor()
  {
    return keyDescriptor;
  }

  public Map<Integer, FieldsDescriptor> getDdIDToKeyDescriptor()
  {
    return ddIDToKeyDescriptor;
  }

  public Map<Integer, Map<String, FieldsDescriptor>> getDdIDToAggregatorToAggregateDescriptor()
  {
    return ddIDToAggregatorToAggregateDescriptor;
  }
}
