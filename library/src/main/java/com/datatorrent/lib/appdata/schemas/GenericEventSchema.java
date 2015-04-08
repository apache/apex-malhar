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
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * Example schema:
 *
 * {"keys":
 *   [{"name":"keyName1","type":"type1"},
 *    {"name":"keyName2","type":"type2"}],
 *  "timeBuckets":["1m","1h","1d"],
 *  "values":
 *   [{"name":"valueName1","type":"type1","aggregators":["SUM"]},
 *    {"name":"valueName2","type":"type2","aggregators":["SUM"]}]
 *  "dimensions":
 *   [{"combination":["keyName1","keyName2"],"additionalValues":["valueName1:MIN","valueName1:MAX","valueName2:MIN"]},
 *    {"combination":["keyName1"],"additionalValues":["valueName1:MAX"]}]
 * }
 */
public class GenericEventSchema
{
  private static final Logger logger = LoggerFactory.getLogger(GenericEventSchema.class);

  public static final String ADDITIONAL_VALUE_SEPERATOR = ":";
  public static final int ADDITIONAL_VALUE_NUM_COMPONENTS = 2;
  public static final int ADDITIONAL_VALUE_VALUE_INDEX = 0;
  public static final int ADDITIONAL_VALUE_AGGREGATOR_INDEX = 1;

  public static final String FIELD_KEYS = "keys";
  public static final String FIELD_KEYS_NAME = "name";
  public static final String FIELD_KEYS_TYPE = "type";

  public static final String FIELD_TIME_BUCKETS = "timeBuckets";

  public static final String FIELD_VALUES = "values";
  public static final String FIELD_VALUES_NAME = "name";
  public static final String FIELD_VALUES_TYPE = "type";
  public static final String FIELD_VALUES_AGGREGATIONS = "aggregators";

  public static final String FIELD_DIMENSIONS = "dimensions";
  public static final String FIELD_DIMENSIONS_COMBINATIONS = "combinations";
  public static final String FIELD_DIMENSIONS_ADDITIONAL_VALUES = "additionalValues";

  private FieldsDescriptor keyDescriptor;
  private List<FieldsDescriptor> ddIDToKeyDescriptor;
  private List<DimensionsDescriptor> ddIDToDD;
  private List<Map<String, Set<String>>> ddIDToValueToAggregator;
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

    //Keys

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

    //Time Buckets

    Set<TimeBucket> timeBuckets = Sets.newHashSet();

    if(jo.has(FIELD_TIME_BUCKETS)) {
      JSONArray timeBucketsJSON = jo.getJSONArray(FIELD_TIME_BUCKETS);

      for(int timeBucketIndex = 0;
          timeBucketIndex < timeBucketsJSON.length();
          timeBucketIndex++) {
        String timeBucketString = timeBucketsJSON.getString(timeBucketIndex);
        TimeBucket timeBucket = TimeBucket.getBucketEx(timeBucketString);

        if(!timeBuckets.add(timeBucket)) {
          throw new IllegalArgumentException("The bucket " + timeBucket.getText() +
                                             " was defined twice.");
        }
      }
    }

    //No time bucket
    if(timeBuckets.isEmpty()) {
      timeBuckets.add(TimeBucket.NONE);
    }

    //Values

    Map<String, Set<String>> valueToAggregators = Maps.newHashMap();

    Map<String, Type> aggFieldToType = Maps.newHashMap();
    JSONArray valuesArray = jo.getJSONArray(FIELD_VALUES);

    for(int valueIndex = 0;
        valueIndex < valuesArray.length();
        valueIndex++) {
      JSONObject value = valuesArray.getJSONObject(valueIndex);
      String name = value.getString(FIELD_VALUES_NAME);
      String type = value.getString(FIELD_VALUES_TYPE);
      Type typeT = Type.NAME_TO_TYPE.get(type);

      if(aggFieldToType.containsKey(name)) {
        throw new IllegalArgumentException("Cannot define the value " + name +
                                           " twice.");
      }

      aggFieldToType.put(name, typeT);
      Set<String> aggregatorSet = Sets.newHashSet();

      if(value.has(FIELD_VALUES_AGGREGATIONS)) {
        JSONArray aggregators = value.getJSONArray(FIELD_VALUES_AGGREGATIONS);

        for(int aggregatorIndex = 0;
            aggregatorIndex < aggregators.length();
            aggregatorIndex++) {
          String aggregatorName = aggregators.getString(aggregatorIndex);

          if(!aggregatorSet.add(aggregatorName)) {
            throw new IllegalArgumentException("An aggregator " + aggregatorName
                                               + " cannot be specified twice for a value");
          }
        }
      }

      if(!aggregatorSet.isEmpty()) {
        valueToAggregators.put(name, aggregatorSet);
      }
    }

    FieldsDescriptor allValuesFieldDescriptor = new FieldsDescriptor(aggFieldToType);

    // Dimensions

    ddIDToValueToAggregator = Lists.newArrayList();
    ddIDToKeyDescriptor = Lists.newArrayList();
    ddIDToDD = Lists.newArrayList();
    ddIDToAggregatorToAggregateDescriptor = Lists.newArrayList();

    JSONArray dimensionsArray = jo.getJSONArray(FIELD_DIMENSIONS);
    Set<Fields> dimensionsDescriptorFields = Sets.newHashSet();

    //loop through dimension descriptors
    for(int dimensionsIndex = 0;
        dimensionsIndex < dimensionsArray.length();
        dimensionsIndex++) {
      //Get a dimension descriptor
      JSONObject dimension = dimensionsArray.getJSONObject(dimensionsIndex);
      //Get the key fields of a descriptor
      JSONArray combinationFields = dimension.getJSONArray(FIELD_DIMENSIONS_COMBINATIONS);
      Map<String, Set<String>> specificValueToAggregator = Maps.newHashMap();

      for(Map.Entry<String, Set<String>> entry:
          valueToAggregators.entrySet()) {
        Set<String> aggregators = Sets.newHashSet();
        aggregators.addAll(entry.getValue());
        specificValueToAggregator.put(entry.getKey(), aggregators);
      }

      List<String> keyNames = Lists.newArrayList();
      //loop through the key fields of a descriptor
      for(int keyIndex = 0;
          keyIndex < combinationFields.length();
          keyIndex++) {
        String keyName = combinationFields.getString(keyIndex);
        keyNames.add(keyName);
      }

      Fields dimensionDescriptorFields = new Fields(keyNames);

      if(!dimensionsDescriptorFields.add(dimensionDescriptorFields)) {
        throw new IllegalArgumentException("Duplicate dimension descriptor: " +
                                           dimensionDescriptorFields);
      }

      //Loop through time to generate dimension descriptors
      for(TimeBucket timeBucket: timeBuckets) {
        DimensionsDescriptor dimensionsDescriptor =
        new DimensionsDescriptor(timeBucket,
                                 dimensionDescriptorFields);
        ddIDToKeyDescriptor.add(dimensionsDescriptor.createFieldsDescriptor(keyDescriptor));
        ddIDToDD.add(dimensionsDescriptor);
      }

      JSONArray additionalValues = dimension.getJSONArray(FIELD_DIMENSIONS_ADDITIONAL_VALUES);

      //iterate over additional values
      for(int additionalValueIndex = 0;
          additionalValueIndex < additionalValues.length();
          additionalValueIndex++) {
        String additionalValue = additionalValues.getString(additionalValueIndex);
        String[] components = additionalValue.split(ADDITIONAL_VALUE_SEPERATOR);

        if(components.length != ADDITIONAL_VALUE_NUM_COMPONENTS) {
          throw new IllegalArgumentException("The number of component values " +
                                             "in an additional value must be " +
                                             ADDITIONAL_VALUE_NUM_COMPONENTS +
                                             " not " + components.length);
        }

        String valueName = components[ADDITIONAL_VALUE_VALUE_INDEX];
        String aggregatorName = components[ADDITIONAL_VALUE_AGGREGATOR_INDEX];

        Set<String> aggregators = specificValueToAggregator.get(valueName);

        if(aggregators == null) {
          aggregators = Sets.newHashSet();
          specificValueToAggregator.put(valueName, aggregators);
        }

        if(aggregators == null) {
          throw new IllegalArgumentException("The additional value " + additionalValue +
                                             "Does not have a corresponding value " + valueName +
                                             " defined in the " + FIELD_VALUES + " section.");
        }

        if(!aggregators.add(aggregatorName)) {
          throw new IllegalArgumentException("The aggregator " + aggregatorName +
                                             " was already defined in the " + FIELD_VALUES +
                                             " section for the value " + valueName);
        }
      }

      //Map specificValueToAggregator immutable and validate that atleast one aggregator is defined
      //for each value
      for(Map.Entry<String, Set<String>> entry:
          specificValueToAggregator.entrySet()) {
        if(!entry.getValue().isEmpty()) {
          throw new IllegalArgumentException("No aggregations defined " + "for the value " +
                                             entry.getKey());
        }

        specificValueToAggregator.put(entry.getKey(),
                                      Collections.unmodifiableSet(entry.getValue()));
      }

      specificValueToAggregator = Collections.unmodifiableMap(specificValueToAggregator);
      ddIDToValueToAggregator.add(specificValueToAggregator);
    }

    //DD ID To Aggregator To Aggregate Descriptor

    ddIDToAggregatorToAggregateDescriptor = Lists.newArrayList();

    for(int ddID = 0;
        ddID < getDdIDToValueToAggregator().size();
        ddID++) {
      Map<String, Set<String>> valueToAggregator = ddIDToValueToAggregator.get(ddID);
      Map<String, Set<String>> aggregatorToValues = Maps.newHashMap();

      for(Map.Entry<String, Set<String>> entry: valueToAggregator.entrySet()) {
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
  public List<Map<String, Set<String>>> getDdIDToValueToAggregator()
  {
    return ddIDToValueToAggregator;
  }
}
