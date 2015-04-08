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
package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
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
import java.util.Iterator;
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
 *   [{"name":"valueName1","type":"type1"},
 *    {"name":"valueName2","type":"type2"}]
 *  "aggregations":
 *   [{"descriptor":"keyName1:keyName2","aggregations":{"valueName1":["min","max"],"valueName2":["sum"]}},
 *    {"descriptor":"keyName1","aggregations":{"valueName1":["sum"]}}]
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

  //Done
  private FieldsDescriptor keyDescriptor;
  //Done
  private List<FieldsDescriptor> ddIDToKeyDescriptor;
  //Done
  private List<DimensionsDescriptor> ddIDToDD;
  //Done
  private List<Map<String, List<String>>> ddIDToValueToAggregator;
  private List<Map<String, FieldsDescriptor>> ddIDToAggregatorToAggregateDescriptor;

  private Map<DimensionsDescriptor, Integer> dimensionsDescriptorToID;

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

    Map<String, Type> aggFieldToType = Maps.newHashMap();
    JSONArray valuesArray = jo.getJSONArray(FIELD_VALUES);

    for(int valueIndex = 0;
        valueIndex < valuesArray.length();
        valueIndex++) {
      JSONObject value = valuesArray.getJSONObject(valueIndex);
      String name = value.getString(FIELD_VALUES_NAME);
      String type = value.getString(FIELD_VALUES_TYPE);
      Type typeT = Type.NAME_TO_TYPE.get(type);

      aggFieldToType.put(name, typeT);
    }

    FieldsDescriptor allValuesFieldDescriptor = new FieldsDescriptor(aggFieldToType);

    ddIDToValueToAggregator = Lists.newArrayList();
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

      JSONObject valuesToAggregationsJSON = aggregation.getJSONObject(FIELD_AGGREGATIONS_AGGREGATIONS);
      Iterator valuesIterator = valuesToAggregationsJSON.keys();

      Map<String, List<String>> valueToAggregators = Maps.newHashMap();

      while(valuesIterator.hasNext()) {
        String value = (String) valuesIterator.next();

        if(!aggFieldToType.containsKey(value)) {
          throw new IllegalArgumentException("The value " + value +
                                             " was not included in the " + FIELD_VALUES +
                                             "section of the json.");
        }

        List<String> aggregators = Lists.newArrayList();
        Set<String> aggregatorSet = Sets.newHashSet();
        JSONArray aggregatorsForValue = valuesToAggregationsJSON.getJSONArray(value);

        for(int aggregatorIndex = 0;
            aggregatorIndex < aggregatorsForValue.length();
            aggregatorIndex++) {
          String aggregatorString = aggregatorsForValue.getString(aggregatorIndex);

          if(!aggregatorSet.add(aggregatorString)) {
            throw new IllegalArgumentException("The aggregator " + aggregatorString + " was listed more than once.");
          }

          aggregators.add(aggregatorString);
        }

        aggregators = Collections.unmodifiableList(aggregators);
        valueToAggregators.put(value, aggregators);
      }

      valueToAggregators = Collections.unmodifiableMap(valueToAggregators);
      getDdIDToValueToAggregator().add(valueToAggregators);
    }

    //DD ID To Aggregator To Aggregate Descriptor

    ddIDToAggregatorToAggregateDescriptor = Lists.newArrayList();

    for(int ddID = 0;
        ddID < getDdIDToValueToAggregator().size();
        ddID++) {
      Map<String, List<String>> valueToAggregator = getDdIDToValueToAggregator().get(ddID);
      Map<String, Set<String>> aggregatorToValues = Maps.newHashMap();

      for(Map.Entry<String, List<String>> entry: valueToAggregator.entrySet()) {
        String value = entry.getKey();
        for(String aggregator: entry.getValue()) {
          Set<String> values = aggregatorToValues.get(aggregator);

          if(values == null) {
            values = Sets.newHashSet();
            aggregatorToValues.put(aggregator, values);
          }

          values.add(value);
        }
      }

      Map<String, FieldsDescriptor> aggregatorToValuesDescriptor = Maps.newHashMap();

      for(Map.Entry<String, Set<String>> entry: aggregatorToValues.entrySet()) {
        for(String value: entry.getValue()) {
          aggFieldToType.get(value);
        }

        aggregatorToValuesDescriptor.put(
        entry.getKey(),
        allValuesFieldDescriptor.getSubset(new Fields(entry.getValue())));
      }

      aggregatorToValuesDescriptor = Collections.unmodifiableMap(aggregatorToValuesDescriptor);
      ddIDToAggregatorToAggregateDescriptor.add(aggregatorToValuesDescriptor);
    }

    ddIDToDD = Collections.unmodifiableList(ddIDToDD);
    ddIDToKeyDescriptor = Collections.unmodifiableList(ddIDToKeyDescriptor);
    ddIDToAggregatorToAggregateDescriptor = Collections.unmodifiableList(ddIDToAggregatorToAggregateDescriptor);

    //
    //Dimensions Descriptor To ID

    dimensionsDescriptorToID = Maps.newHashMap();

    for(int index = 0;
        index < ddIDToDD.size();
        index++) {
      dimensionsDescriptorToID.put(ddIDToDD.get(index), index);
    }

    //
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

  public List<Map<Integer, FieldsDescriptor>> getDdIDToAggregatorIDToFieldsDescriptor(Map<String, Integer> aggregatorNameToID)
  {
    List<Map<Integer, FieldsDescriptor>> lists = Lists.newArrayList();
    List<Map<String, FieldsDescriptor>> ddIDToAToAD = getDdIDToAggregatorToAggregateDescriptor();

    for(Map<String, FieldsDescriptor> aToAD: ddIDToAToAD) {
      logger.info("Dd to string aggMap: {}", aToAD);
      Map<Integer, FieldsDescriptor> aggDescriptorsList = Maps.newHashMap();

      for(Map.Entry<String, FieldsDescriptor> entry: aToAD.entrySet()) {
        aggDescriptorsList.put(aggregatorNameToID.get(entry.getKey()), entry.getValue());
        logger.info("Cost Type {}", entry.getValue().getType("cost"));
      }

      lists.add(aggDescriptorsList);
    }

    logger.info("Cost Type check {}", lists.get(0).get(0).getType("cost"));

    return lists;
  }

  /**
   * @return the fieldsToDimensionDescriptor
   */
  public Map<DimensionsDescriptor, Integer> getDimensionsDescriptorToID()
  {
    return dimensionsDescriptorToID;
  }

  /**
   * @return the ddIDToDD
   */
  public List<DimensionsDescriptor> getDdIDToDD()
  {
    return ddIDToDD;
  }

  /**
   * @return the ddIDToValueToAggregator
   */
  public List<Map<String, List<String>>> getDdIDToValueToAggregator()
  {
    return ddIDToValueToAggregator;
  }
}
