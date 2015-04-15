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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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
public class DimensionalEventSchema
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionalEventSchema.class);

  public static final String ADDITIONAL_VALUE_SEPERATOR = ":";
  public static final int ADDITIONAL_VALUE_NUM_COMPONENTS = 2;
  public static final int ADDITIONAL_VALUE_VALUE_INDEX = 0;
  public static final int ADDITIONAL_VALUE_AGGREGATOR_INDEX = 1;

  public static final String FIELD_KEYS = "keys";
  public static final String FIELD_KEYS_NAME = "name";
  public static final String FIELD_KEYS_TYPE = "type";
  public static final String FIELD_KEYS_ENUMVALUES = "enumValues";

  public static final List<Fields> VALID_KEY_FIELDS = ImmutableList.of(new Fields(Sets.newHashSet(FIELD_KEYS_NAME,
                                                                                              FIELD_KEYS_TYPE,
                                                                                              FIELD_KEYS_ENUMVALUES)),
                                                                   new Fields(Sets.newHashSet(FIELD_KEYS_NAME,
                                                                                              FIELD_KEYS_TYPE)));

  public static final String FIELD_TIME_BUCKETS = "timeBuckets";

  public static final String FIELD_VALUES = "values";
  public static final String FIELD_VALUES_NAME = "name";
  public static final String FIELD_VALUES_TYPE = "type";
  public static final String FIELD_VALUES_AGGREGATIONS = "aggregators";

  public static final String FIELD_DIMENSIONS = "dimensions";
  public static final String FIELD_DIMENSIONS_COMBINATIONS = "combination";
  public static final String FIELD_DIMENSIONS_ADDITIONAL_VALUES = "additionalValues";

  private FieldsDescriptor keyDescriptor;
  private FieldsDescriptor inputValuesDescriptor;

  private Map<String, List<Object>> keysToValuesList;
  private Map<String, Set<String>> allValueToAggregator;
  private List<FieldsDescriptor> ddIDToKeyDescriptor;
  private List<DimensionsDescriptor> ddIDToDD;
  private List<Map<String, Set<String>>> ddIDToValueToAggregator;
  private List<Map<String, FieldsDescriptor>> ddIDToAggregatorToAggregateDescriptor;
  private Map<DimensionsDescriptor, Integer> dimensionsDescriptorToID;

  private String dimensionsString;
  private String keysString;
  private String bucketsString;

  public DimensionalEventSchema()
  {
  }

  public DimensionalEventSchema(String json)
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

    Iterator keyIterator = jo.keys();

    while(keyIterator.hasNext()) {
      logger.info("The key {}", keyIterator.next());
    }

    //Keys

    keysToValuesList = Maps.newHashMap();
    JSONArray keysArray = jo.getJSONArray(FIELD_KEYS);
    keysString = keysArray.toString();

    Map<String, Type> fieldToType = Maps.newHashMap();

    for(int keyIndex = 0;
        keyIndex < keysArray.length();
        keyIndex++) {
      JSONObject keyDescriptor = keysArray.getJSONObject(keyIndex);

      SchemaUtils.checkValidKeysEx(keyDescriptor, VALID_KEY_FIELDS);

      String keyName = keyDescriptor.getString(FIELD_KEYS_NAME);
      String typeName = keyDescriptor.getString(FIELD_KEYS_TYPE);
      Type type = Type.getTypeEx(typeName);

      fieldToType.put(keyName, type);

      if(keyDescriptor.has(FIELD_KEYS_ENUMVALUES)) {
        Type maxType = null;
        List<Object> valuesList = Lists.newArrayList();
        getKeysToValuesList().put(keyName, valuesList);
        JSONArray valArray = keyDescriptor.getJSONArray(FIELD_KEYS_ENUMVALUES);

        //Validate the provided data types
        for(int valIndex = 0;
            valIndex < valArray.length();
            valIndex++) {
          Object val = valArray.get(valIndex);
          valuesList.add(val);

          Preconditions.checkState(!(val instanceof JSONArray
                                     || val instanceof JSONObject),
                                   "The value must be a primitive.");

          Type currentType = Type.CLASS_TO_TYPE.get(val.getClass());

          if(maxType == null) {
            maxType = currentType;
          }
          else if(maxType != currentType) {
            if(maxType.getHigherTypes().contains(currentType)) {
              maxType = currentType;
            }
            else {
              Preconditions.checkState(currentType.getHigherTypes().contains(maxType),
                                       "Conficting types: " + currentType.getName()
                                       + " cannot be converted to " + maxType.getName());
            }
          }
        }

        //This is not the right thing to do, fix later
        if(!Type.areRelated(maxType, type)) {
          throw new IllegalArgumentException("The type of the values in "
                                             + valArray + " is " + maxType.getName()
                                             + " while the specified type is " + type.getName());
        }
      }
    }

    //Keys to values list immutable


    //Key descriptor all
    keyDescriptor = new FieldsDescriptor(fieldToType);

    //Time Buckets
    Set<TimeBucket> timeBuckets = Sets.newHashSet();

    JSONArray timeBucketsJSON = jo.getJSONArray(FIELD_TIME_BUCKETS);
    bucketsString = timeBucketsJSON.toString();

    if(timeBucketsJSON.length() == 0) {
      throw new IllegalArgumentException("A time bucket must be specified.");
    }

    for(int timeBucketIndex = 0;
        timeBucketIndex < timeBucketsJSON.length();
        timeBucketIndex++) {
      String timeBucketString = timeBucketsJSON.getString(timeBucketIndex);
      TimeBucket timeBucket = TimeBucket.getBucketEx(timeBucketString);

      if(!timeBuckets.add(timeBucket)) {
        throw new IllegalArgumentException("The bucket " + timeBucket.getText()
                                           + " was defined twice.");
      }
    }

    //Values

    allValueToAggregator = Maps.newHashMap();
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
          Set<String> aggregatorNames = allValueToAggregator.get(name);

          if(aggregatorNames == null) {
            aggregatorNames = Sets.newHashSet();
            allValueToAggregator.put(name, aggregatorNames);
          }

          aggregatorNames.add(aggregatorName);

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

    this.inputValuesDescriptor = new FieldsDescriptor(aggFieldToType);

    // Dimensions

    ddIDToValueToAggregator = Lists.newArrayList();
    ddIDToKeyDescriptor = Lists.newArrayList();
    ddIDToDD = Lists.newArrayList();
    ddIDToAggregatorToAggregateDescriptor = Lists.newArrayList();

    JSONArray dimensionsArray = jo.getJSONArray(FIELD_DIMENSIONS);
    dimensionsString = dimensionsArray.toString();
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
        logger.info("Event Schema Dimension Descriptor: {}", dimensionsDescriptor);
        ddIDToKeyDescriptor.add(dimensionsDescriptor.createFieldsDescriptor(keyDescriptor));
        ddIDToDD.add(dimensionsDescriptor);
      }

      if(dimension.has(FIELD_DIMENSIONS_ADDITIONAL_VALUES)) {
        JSONArray additionalValues = dimension.getJSONArray(FIELD_DIMENSIONS_ADDITIONAL_VALUES);
        logger.info("Additional values length {}", additionalValues.length());

        //iterate over additional values
        for(int additionalValueIndex = 0;
            additionalValueIndex < additionalValues.length();
            additionalValueIndex++) {
          String additionalValue = additionalValues.getString(additionalValueIndex);
          String[] components = additionalValue.split(ADDITIONAL_VALUE_SEPERATOR);

          if(components.length != ADDITIONAL_VALUE_NUM_COMPONENTS) {
            throw new IllegalArgumentException("The number of component values "
                                               + "in an additional value must be "
                                               + ADDITIONAL_VALUE_NUM_COMPONENTS
                                               + " not " + components.length);
          }

          String valueName = components[ADDITIONAL_VALUE_VALUE_INDEX];
          String aggregatorName = components[ADDITIONAL_VALUE_AGGREGATOR_INDEX];

          Set<String> aggregatorNames = allValueToAggregator.get(valueName);

          if(aggregatorNames == null) {
            aggregatorNames = Sets.newHashSet();
            allValueToAggregator.put(valueName, aggregatorNames);
          }

          aggregatorNames.add(aggregatorName);
          logger.info("Aggregator name {}", aggregatorName);

          Set<String> aggregators = specificValueToAggregator.get(valueName);

          if(aggregators == null) {
            aggregators = Sets.newHashSet();
            specificValueToAggregator.put(valueName, aggregators);
          }

          if(aggregators == null) {
            throw new IllegalArgumentException("The additional value " + additionalValue
                                               + "Does not have a corresponding value " + valueName
                                               + " defined in the " + FIELD_VALUES + " section.");
          }

          if(!aggregators.add(aggregatorName)) {
            throw new IllegalArgumentException("The aggregator " + aggregatorName
                                               + " was already defined in the " + FIELD_VALUES
                                               + " section for the value " + valueName);
          }
        }
      }

      if(specificValueToAggregator.isEmpty()) {
        throw new IllegalArgumentException("No aggregations defined for the " +
                                           "following field combination " +
                                           combinationFields.toString());
      }

      //Map specificValueToAggregator immutable and validate that atleast one aggregator is defined
      //for each value
      for(Map.Entry<String, Set<String>> entry:
          specificValueToAggregator.entrySet()) {
        specificValueToAggregator.put(entry.getKey(),
                                      Collections.unmodifiableSet(entry.getValue()));
      }

      specificValueToAggregator = Collections.unmodifiableMap(specificValueToAggregator);

      for(TimeBucket timeBucket: timeBuckets) {
        ddIDToValueToAggregator.add(specificValueToAggregator);
      }
    }

    //DD ID To Aggregator To Aggregate Descriptor

    ddIDToAggregatorToAggregateDescriptor = Lists.newArrayList();

    for(int ddID = 0;
        ddID < ddIDToValueToAggregator.size();
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
        inputValuesDescriptor.getSubset(new Fields(entry.getValue())));
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

    //allValueToAggregator unmodifiable
    Map<String, Set<String>> allValueToAggregatorUnmodifiable = Maps.newHashMap();

    for(Map.Entry<String, Set<String>> entry: allValueToAggregator.entrySet()) {
      allValueToAggregatorUnmodifiable.put(entry.getKey(), Collections.unmodifiableSet(entry.getValue()));
    }

    allValueToAggregator = Collections.unmodifiableMap(allValueToAggregatorUnmodifiable);
  }

  public FieldsDescriptor getAllKeysDescriptor()
  {
    return keyDescriptor;
  }

  public FieldsDescriptor getInputValuesDescriptor()
  {
    return inputValuesDescriptor;
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
    List<Map<String, FieldsDescriptor>> ddIDToAToAD = ddIDToAggregatorToAggregateDescriptor;

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

  public String getKeysString()
  {
    return keysString;
  }

  public String getDimensionsString()
  {
    return dimensionsString;
  }

  public Map<String, Set<String>> getAllValueToAggregator()
  {
    return allValueToAggregator;
  }

  /**
   * @return the bucketsString
   */
  public String getBucketsString()
  {
    return bucketsString;
  }

  /**
   * @return the keysToValuesList
   */
  public Map<String, List<Object>> getKeysToValuesList()
  {
    return keysToValuesList;
  }
}
