/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
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
 *   [{"descriptor":"keyName1:keyName2","aggregations":["min","max"]},
 *    {"descriptor":"keyName1","aggregations":["sum"]}]
 * }
 */
public class GenericEventSchema
{
  private static final Logger logger = LoggerFactory.getLogger(GenericEventSchema.class);

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
  private List<FieldsDescriptor> ddIDToKeyDescriptor;
  private List<DimensionsDescriptor> ddIDToDD;
  private List<Map<String, FieldsDescriptor>> ddIDToAggregatorToAggregateDescriptor;
  private Map<String, Set<DimensionsDescriptor>> aggregatorToDimensionsDescriptor;
  private Map<Fields, Integer> fieldsToDimensionDescriptor;

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
    System.out.println("Initializing: " + json);
    JSONObject jo = new JSONObject(json);

    JSONArray keysArray = jo.getJSONArray(FIELD_KEYS);
    Map<String, Type> fieldToType = Maps.newHashMap();

    for(int keyIndex = 0;
        keyIndex < keysArray.length();
        keyIndex++) {
      JSONObject keyDescriptor = keysArray.getJSONObject(keyIndex);
      System.out.println("keyDescriptor: " + keyDescriptor);
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
      FieldsDescriptor descriptor = new FieldsDescriptor(entry.getValue());
      aggregatorToFieldsDescriptor.put(entry.getKey(), descriptor);
    }

    //Aggregator to Dimensions descriptor
    aggregatorToDimensionsDescriptor = Maps.newHashMap();

    ddIDToKeyDescriptor = Lists.newArrayList();
    ddIDToDD = Lists.newArrayList();
    ddIDToAggregatorToAggregateDescriptor = Lists.newArrayList();

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

      ddIDToKeyDescriptor.add(dimensionsDescriptor.createFieldsDescriptor(keyDescriptor));
      getDdIDToDD().add(dimensionsDescriptor);

      JSONArray aggArray = aggregation.getJSONArray(FIELD_AGGREGATIONS_AGGREGATIONS);
      Map<String, FieldsDescriptor> specificAggregatorToFieldsDescriptor = Maps.newHashMap();

      for(int aggIndex = 0;
          aggIndex < aggArray.length();
          aggIndex++) {
        String aggregator = aggArray.getString(aggIndex);
        specificAggregatorToFieldsDescriptor.put(aggregator, aggregatorToFieldsDescriptor.get(aggregator));

        Set<DimensionsDescriptor> dds = aggregatorToDimensionsDescriptor.get(aggregator);

        if(dds == null) {
          dds = Sets.newHashSet();
          aggregatorToDimensionsDescriptor.put(aggregator, dds);
        }

        dds.add(dimensionsDescriptor);
      }

      specificAggregatorToFieldsDescriptor = Collections.unmodifiableMap(specificAggregatorToFieldsDescriptor);
      ddIDToAggregatorToAggregateDescriptor.add(specificAggregatorToFieldsDescriptor);
    }

    ddIDToDD = Collections.unmodifiableList(getDdIDToDD());
    ddIDToKeyDescriptor = Collections.unmodifiableList(ddIDToKeyDescriptor);
    ddIDToAggregatorToAggregateDescriptor = Collections.unmodifiableList(ddIDToAggregatorToAggregateDescriptor);

    //Making aggregator to dimensions descriptor unmodifiable

    for(Map.Entry<String, Set<DimensionsDescriptor>> entry:
        aggregatorToDimensionsDescriptor.entrySet()) {
      aggregatorToDimensionsDescriptor.put(entry.getKey(),
                                           Collections.unmodifiableSet(entry.getValue()));
    }

    aggregatorToDimensionsDescriptor = Collections.unmodifiableMap(aggregatorToDimensionsDescriptor);

    /////////////////////

    fieldsToDimensionDescriptor = Maps.newHashMap();

    for(int index = 0;
        index < ddIDToKeyDescriptor.size();
        index++) {
      FieldsDescriptor fd = ddIDToKeyDescriptor.get(index);
      getFieldsToDimensionDescriptor().put(fd.getFields(), index);
    }
  }

  public FieldsDescriptor getAllKeysDescriptor()
  {
    return keyDescriptor;
  }

  public List<FieldsDescriptor> getDdIDToKeyDescriptor()
  {
    return ddIDToKeyDescriptor;
  }

  public List<Map<String, FieldsDescriptor>> getDdIDToAggregatorToAggregateDescriptor()
  {
    return ddIDToAggregatorToAggregateDescriptor;
  }

  public Map<String, Set<DimensionsDescriptor>> getAggregatorToDimensionsDescriptor()
  {
    return aggregatorToDimensionsDescriptor;
  }

  public List<Map<Integer, FieldsDescriptor>> getDdIDToAggregatorIDToFieldsDescriptor(Map<String, Integer> aggregatorNameToID)
  {
    List<Map<Integer, FieldsDescriptor>> lists = Lists.newArrayList();
    List<Map<String, FieldsDescriptor>> ddIDToAToAD = getDdIDToAggregatorToAggregateDescriptor();

    for(Map<String, FieldsDescriptor> aToAD: ddIDToAToAD) {
      logger.debug("Dd to string aggMap: {}", aToAD);
      Map<Integer, FieldsDescriptor> aggDescriptorsList = Maps.newHashMap();

      for(Map.Entry<String, FieldsDescriptor> entry: aToAD.entrySet()) {
        aggDescriptorsList.put(aggregatorNameToID.get(entry.getKey()), entry.getValue());
      }

      lists.add(aggDescriptorsList);
    }

    return lists;
  }

  /**
   * @return the fieldsToDimensionDescriptor
   */
  public Map<Fields, Integer> getFieldsToDimensionDescriptor()
  {
    return fieldsToDimensionDescriptor;
  }

  /**
   * @return the ddIDToDD
   */
  public List<DimensionsDescriptor> getDdIDToDD()
  {
    return ddIDToDD;
  }
}
