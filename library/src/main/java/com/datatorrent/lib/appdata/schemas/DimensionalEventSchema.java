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

import com.datatorrent.lib.appdata.dimensions.AggregatorInfo;
import com.datatorrent.lib.appdata.dimensions.AggregatorStaticType;
import com.datatorrent.lib.appdata.dimensions.DimensionsAggregator;
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import javax.validation.constraints.NotNull;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
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
  private List<FieldsDescriptor> ddIDToKeyDescriptor;
  private List<DimensionsDescriptor> ddIDToDD;
  private List<Map<String, Set<String>>> ddIDToValueToAggregator;
  private List<Map<String, Set<String>>> ddIDToValueToOTFAggregator;
  private List<Map<String, FieldsDescriptor>> ddIDToAggregatorToAggregateDescriptor;
  private List<Map<String, FieldsDescriptor>> ddIDToOTFAggregatorToAggregateDescriptor;
  private Map<DimensionsDescriptor, Integer> dimensionsDescriptorToID;

  private List<Int2ObjectMap<FieldsDescriptor>> ddIDToAggIDToInputAggDescriptor;
  private List<Int2ObjectMap<FieldsDescriptor>> ddIDToAggIDToOutputAggDescriptor;
  private List<IntArrayList> ddIDToAggIDs;

  private List<Map<String, Set<String>>> combinationIDToFieldToAggregatorAdditionalValues;
  private List<Fields> combinationIDToKeys;

  private String keysString;
  private String bucketsString;

  private AggregatorInfo aggregatorInfo;

  List<TimeBucket> timeBuckets;

  private Map<String, Map<String, Type>> schemaAllValueToAggregatorToType;

  public DimensionalEventSchema()
  {
  }

  public DimensionalEventSchema(List<Key> keys,
                                List<Value> values,
                                List<TimeBucket> timeBuckets,
                                List<DimensionsCombination> dimensionsCombinations,
                                AggregatorInfo aggregatorInfo)
  {
    setAggregatorInfo(aggregatorInfo);

    initialize(keys,
               values,
               timeBuckets,
               dimensionsCombinations);
  }

  public DimensionalEventSchema(String json,
                                AggregatorInfo aggregatorInfo)
  {
    setAggregatorInfo(aggregatorInfo);

    try {
      initialize(json);
    }
    catch(Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  private void setAggregatorInfo(AggregatorInfo aggregatorInfo)
  {
    this.aggregatorInfo = Preconditions.checkNotNull(aggregatorInfo);
  }

  private List<Map<String, FieldsDescriptor>> computeAggregatorToAggregateDescriptor(List<Map<String, Set<String>>> ddIDToValueToAggregator)
  {
    List<Map<String, FieldsDescriptor>> tempDdIDToAggregatorToAggregateDescriptor = Lists.newArrayList();

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
        aggregatorToValuesDescriptor.put(
        entry.getKey(),
        inputValuesDescriptor.getSubset(new Fields(entry.getValue())));
      }

      aggregatorToValuesDescriptor = Collections.unmodifiableMap(aggregatorToValuesDescriptor);
      tempDdIDToAggregatorToAggregateDescriptor.add(aggregatorToValuesDescriptor);
    }

    return tempDdIDToAggregatorToAggregateDescriptor;
  }

  private void initialize(List<Key> keys,
                          List<Value> values,
                          List<TimeBucket> timeBuckets,
                          List<DimensionsCombination> dimensionsCombinations)
  {
    //time buckets
    this.timeBuckets = timeBuckets;

    //Input aggregate values

    Map<String, Type> valueFieldToType = Maps.newHashMap();

    for(Value value: values) {
      valueFieldToType.put(value.getName(), value.getType());
    }

    inputValuesDescriptor = new FieldsDescriptor(valueFieldToType);

    //Input keys

    Map<String, Type> keyFieldToType = Maps.newHashMap();
    keysToValuesList = Maps.newHashMap();

    for(Key key: keys) {
      keyFieldToType.put(key.getName(), key.getType());
      keysToValuesList.put(key.getName(), key.getEnumValues());
    }

    keyDescriptor = new FieldsDescriptor(keyFieldToType);

    //schemaAllValueToAggregatorToType
    schemaAllValueToAggregatorToType = Maps.newHashMap();

    for(Value value: values) {
      String valueName = value.getName();
      Type inputType = inputValuesDescriptor.getType(valueName);
      Set<String> aggregators = value.getAggregators();
      Map<String, Type> aggregatorToType = Maps.newHashMap();

      for(String aggregatorName: aggregators) {
        DimensionsAggregator da;

        if(aggregatorInfo.isStaticAggregator(aggregatorName)) {
          da = aggregatorInfo.getStaticAggregatorNameToStaticAggregator().get(aggregatorName);
        }
        else {
          da = aggregatorInfo.getNameToOTFAggregators().get(aggregatorName);
        }

        Type outputType = da.getTypeMap().getTypeMap().get(inputType);
        aggregatorToType.put(aggregatorName, outputType);
      }

      schemaAllValueToAggregatorToType.put(valueName, aggregatorToType);
    }

    //ddID

    ddIDToDD = Lists.newArrayList();
    ddIDToKeyDescriptor = Lists.newArrayList();
    dimensionsDescriptorToID = Maps.newHashMap();
    ddIDToValueToAggregator = Lists.newArrayList();
    ddIDToValueToOTFAggregator = Lists.newArrayList();

    int ddID = 0;
    for(DimensionsCombination dimensionsCombination: dimensionsCombinations) {
      for(TimeBucket timeBucket: timeBuckets) {
        DimensionsDescriptor dd = new DimensionsDescriptor(timeBucket, dimensionsCombination.getFields());
        ddIDToDD.add(dd);
        ddIDToKeyDescriptor.add(dd.createFieldsDescriptor(keyDescriptor));
        dimensionsDescriptorToID.put(dd, ddID);

        Map<String, Set<String>> valueToAggregator = Maps.newHashMap();
        Map<String, Set<String>> valueToOTFAggregator = Maps.newHashMap();

        Map<String, Set<String>> tempValueToAggregator = dimensionsCombination.getValueToAggregators();

        for(Map.Entry<String, Set<String>> entry: tempValueToAggregator.entrySet()) {
          String value = entry.getKey();
          Set<String> staticAggregatorNames = Sets.newHashSet();
          Set<String> otfAggregatorNames = Sets.newHashSet();
          Set<String> aggregatorNames = entry.getValue();

          for(String aggregatorName: aggregatorNames) {
            if(!aggregatorInfo.isAggregator(aggregatorName)) {
              throw new UnsupportedOperationException("The aggregator "
                                                      + aggregatorName
                                                      + " is not valid.");
            }

            if(aggregatorInfo.isStaticAggregator(aggregatorName)) {
              staticAggregatorNames.add(aggregatorName);
            }
            else {
              otfAggregatorNames.add(aggregatorName);
            }
          }

          if(!staticAggregatorNames.isEmpty()) {
            valueToAggregator.put(value, staticAggregatorNames);
          }

          if(!otfAggregatorNames.isEmpty()) {
            valueToOTFAggregator.put(value, otfAggregatorNames);
          }
        }

        ddIDToValueToAggregator.add(valueToAggregator);
        ddIDToValueToOTFAggregator.add(valueToOTFAggregator);
        ddID++;
      }
    }

    for(Value value: values) {
      String name = value.getName();
      Set<String> aggregators = value.getAggregators();

      for(String aggregator: aggregators) {
        if(!aggregatorInfo.isAggregator(aggregator)) {
          throw new RuntimeException(aggregator + " is not a valid aggregator.");
        }

        if(aggregatorInfo.isStaticAggregator(aggregator)) {
          for(Map<String, Set<String>> valueToAggregator: ddIDToValueToAggregator) {
            Set<String> valueAggregators = valueToAggregator.get(name);

            if(valueAggregators == null) {
              valueAggregators = Sets.newHashSet();
              valueToAggregator.put(name, valueAggregators);
            }

            valueAggregators.add(aggregator);
          }
        }
        else
        {
          for(Map<String, Set<String>> valueToOTFAggregator: ddIDToValueToOTFAggregator) {
            Set<String> valueAggregators = valueToOTFAggregator.get(name);

            if(valueAggregators == null) {
              valueAggregators = Sets.newHashSet();
              valueToOTFAggregator.put(name, valueAggregators);
            }

            valueAggregators.add(aggregator);
          }
        }
      }
    }

    //ddIDToAggregatorToAggregateDescriptor

    ddIDToAggregatorToAggregateDescriptor = computeAggregatorToAggregateDescriptor(ddIDToValueToAggregator);
    ddIDToAggregatorToAggregateDescriptor = Collections.unmodifiableList(ddIDToAggregatorToAggregateDescriptor);

    ddIDToOTFAggregatorToAggregateDescriptor = computeAggregatorToAggregateDescriptor(ddIDToValueToOTFAggregator);
    ddIDToOTFAggregatorToAggregateDescriptor = Collections.unmodifiableList(ddIDToOTFAggregatorToAggregateDescriptor);

    //combination ID values

    combinationIDToFieldToAggregatorAdditionalValues = Lists.newArrayList();
    combinationIDToKeys = Lists.newArrayList();

    for(DimensionsCombination dimensionsCombination: dimensionsCombinations) {
      combinationIDToFieldToAggregatorAdditionalValues.add(dimensionsCombination.getValueToAggregators());
      combinationIDToKeys.add(dimensionsCombination.getFields());
    }

    //Build keyString

    JSONArray keyArray = new JSONArray();

    for(Key key: keys) {
      JSONObject jo = new JSONObject();

      try {
        jo.put(FIELD_KEYS_NAME, key.getName());
        jo.put(FIELD_KEYS_TYPE, key.getType().getName());

        JSONArray enumArray = new JSONArray();

        for(Object enumVal: key.getEnumValues()) {
          enumArray.put(enumVal);
        }

        jo.put(FIELD_KEYS_ENUMVALUES, enumArray);
      }
      catch(JSONException ex) {
        throw new RuntimeException(ex);
      }

      keyArray.put(jo);
    }

    keysString = keyArray.toString();

    //Build time buckets

    JSONArray timeBucketArray = new JSONArray();

    for(TimeBucket timeBucket: timeBuckets) {
      timeBucketArray.put(timeBucket.getText());
    }

    bucketsString = timeBucketArray.toString();

    //buildDDIDAggID

    buildDDIDAggIDMaps();
  }

  private void initialize(String json) throws Exception
  {
    JSONObject jo = new JSONObject(json);

    //Keys

    keysToValuesList = Maps.newHashMap();
    JSONArray keysArray;

    if(jo.has(FIELD_KEYS)) {
      keysArray = jo.getJSONArray(FIELD_KEYS);
    }
    else {
      keysArray = new JSONArray();
    }

    keysString = keysArray.toString();

    Map<String, Type> fieldToType = Maps.newHashMap();

    for(int keyIndex = 0;
        keyIndex < keysArray.length();
        keyIndex++) {
      JSONObject tempKeyDescriptor = keysArray.getJSONObject(keyIndex);

      SchemaUtils.checkValidKeysEx(tempKeyDescriptor, VALID_KEY_FIELDS);

      String keyName = tempKeyDescriptor.getString(FIELD_KEYS_NAME);
      String typeName = tempKeyDescriptor.getString(FIELD_KEYS_TYPE);
      Type type = Type.getTypeEx(typeName);

      fieldToType.put(keyName, type);

      if(tempKeyDescriptor.has(FIELD_KEYS_ENUMVALUES)) {
        Type maxType = null;
        List<Object> valuesList = Lists.newArrayList();
        getKeysToValuesList().put(keyName, valuesList);
        JSONArray valArray = tempKeyDescriptor.getJSONArray(FIELD_KEYS_ENUMVALUES);

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
    timeBuckets = Lists.newArrayList();

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

    Map<String, Set<String>> allValueToAggregator = Maps.newHashMap();
    Map<String, Set<String>> allValueToOTFAggregator = Maps.newHashMap();
    Map<String, Set<String>> valueToAggregators = Maps.newHashMap();

    Map<String, Type> aggFieldToType = Maps.newHashMap();
    JSONArray valuesArray = jo.getJSONArray(FIELD_VALUES);
    schemaAllValueToAggregatorToType = Maps.newHashMap();

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

      Map<String, Type> aggregatorToType = Maps.newHashMap();
      schemaAllValueToAggregatorToType.put(name, aggregatorToType);

      aggFieldToType.put(name, typeT);
      Set<String> aggregatorSet = Sets.newHashSet();

      if(value.has(FIELD_VALUES_AGGREGATIONS)) {
        JSONArray aggregators = value.getJSONArray(FIELD_VALUES_AGGREGATIONS);

        if(aggregators.length() == 0) {
          throw new IllegalArgumentException("Empty aggregators array for: " + name);
        }

        for(int aggregatorIndex = 0;
            aggregatorIndex < aggregators.length();
            aggregatorIndex++) {
          String aggregatorName = aggregators.getString(aggregatorIndex);
          DimensionsAggregator daggregator = null;

          if(!aggregatorInfo.isAggregator(aggregatorName)) {
            throw new IllegalArgumentException(aggregatorName + " is not a valid aggregator.");
          }

          if(aggregatorInfo.isStaticAggregator(aggregatorName)) {
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

            daggregator = aggregatorInfo.getStaticAggregatorNameToStaticAggregator().get(aggregatorName);
          }
          else {
            //Check for duplicate on the fly aggregators
            Set<String> aggregatorNames = allValueToOTFAggregator.get(name);

            if(aggregatorNames == null) {
              aggregatorNames = Sets.newHashSet();
              allValueToOTFAggregator.put(name, aggregatorNames);
            }

            if(!aggregatorNames.add(aggregatorName)) {
              throw new IllegalArgumentException("An aggregator " + aggregatorName +
                                                 " cannot be specified twice for a value");
            }

            //Add child aggregators
            aggregatorNames = allValueToAggregator.get(name);

            if(aggregatorNames == null) {
              aggregatorNames = Sets.newHashSet();
              allValueToAggregator.put(name, aggregatorNames);
            }

            daggregator = aggregatorInfo.getNameToOTFAggregators().get(aggregatorName);
            aggregatorNames.addAll(aggregatorInfo.getOTFAggregatorToStaticAggregators().get(aggregatorName));
            aggregatorSet.addAll(aggregatorInfo.getOTFAggregatorToStaticAggregators().get(aggregatorName));

            logger.debug("field name {} and adding aggregator names {}:", name, aggregatorNames);
          }

          aggregatorToType.put(aggregatorName, daggregator.getTypeMap().getTypeMap().get(typeT));
        }
      }

      if(!aggregatorSet.isEmpty()) {
        valueToAggregators.put(name, aggregatorSet);
      }
    }

    logger.debug("allValueToAggregator {}", allValueToAggregator);
    logger.debug("valueToAggregators {}", valueToAggregators);

    this.inputValuesDescriptor = new FieldsDescriptor(aggFieldToType);

    // Dimensions

    ddIDToValueToAggregator = Lists.newArrayList();
    ddIDToValueToOTFAggregator = Lists.newArrayList();
    ddIDToKeyDescriptor = Lists.newArrayList();
    ddIDToDD = Lists.newArrayList();
    ddIDToAggregatorToAggregateDescriptor = Lists.newArrayList();

    combinationIDToKeys = Lists.newArrayList();
    combinationIDToFieldToAggregatorAdditionalValues = Lists.newArrayList();

    JSONArray dimensionsArray;

    if(jo.has(FIELD_DIMENSIONS)) {
      dimensionsArray = jo.getJSONArray(FIELD_DIMENSIONS);
    }
    else {
      dimensionsArray = new JSONArray();
      JSONObject combination = new JSONObject();
      combination.put(FIELD_DIMENSIONS_COMBINATIONS, new JSONArray());
      dimensionsArray.put(combination);
    }

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
      Map<String, Set<String>> specificValueToOTFAggregator = Maps.newHashMap();

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

      Map<String, Set<String>> fieldToAggregatorAdditionalValues = Maps.newHashMap();
      combinationIDToKeys.add(dimensionDescriptorFields);
      combinationIDToFieldToAggregatorAdditionalValues.add(fieldToAggregatorAdditionalValues);

      //Loop through time to generate dimension descriptors
      for(TimeBucket timeBucket: timeBuckets) {
        DimensionsDescriptor dimensionsDescriptor =
        new DimensionsDescriptor(timeBucket,
                                 dimensionDescriptorFields);
        ddIDToKeyDescriptor.add(dimensionsDescriptor.createFieldsDescriptor(keyDescriptor));
        ddIDToDD.add(dimensionsDescriptor);
      }

      if(dimension.has(FIELD_DIMENSIONS_ADDITIONAL_VALUES)) {
        JSONArray additionalValues = dimension.getJSONArray(FIELD_DIMENSIONS_ADDITIONAL_VALUES);

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

          {
            Set<String> aggregators = fieldToAggregatorAdditionalValues.get(valueName);

            if(aggregators == null) {
              aggregators = Sets.newHashSet();
              fieldToAggregatorAdditionalValues.put(valueName, aggregators);
            }

            aggregators.add(aggregatorName);
          }

          if(!aggregatorInfo.isAggregator(aggregatorName)) {
            throw new IllegalArgumentException(aggregatorName + " is not a valid aggregator.");
          }

          if(aggregatorInfo.isStaticAggregator(aggregatorName)) {
            Set<String> aggregatorNames = allValueToAggregator.get(valueName);

            if(aggregatorNames == null) {
              aggregatorNames = Sets.newHashSet();
              allValueToAggregator.put(valueName, aggregatorNames);
            }

            aggregatorNames.add(aggregatorName);

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
          else {
            //Check for duplicate on the fly aggregators
            Set<String> aggregatorNames = specificValueToOTFAggregator.get(valueName);

            if(aggregatorNames == null) {
              aggregatorNames = Sets.newHashSet();
              specificValueToOTFAggregator.put(valueName, aggregatorNames);
            }

            if(!aggregatorNames.add(aggregatorName)) {
              throw new IllegalArgumentException("The aggregator " + aggregatorName +
                                                 " cannot be specified twice for the value " + valueName);
            }

            aggregatorNames = allValueToOTFAggregator.get(valueName);

            if(aggregatorNames == null) {
              aggregatorNames = Sets.newHashSet();
              allValueToOTFAggregator.put(valueName, aggregatorNames);
            }

            if(!aggregatorNames.add(aggregatorName)) {
              throw new IllegalArgumentException("The aggregator " + aggregatorName +
                                                 " cannot be specified twice for the value " + valueName);
            }

            //

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

            aggregators.addAll(aggregatorInfo.getOTFAggregatorToStaticAggregators().get(aggregatorName));
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

      for(int timeBucketCounter = 0;
          timeBucketCounter < timeBuckets.size();
          timeBucketCounter++) {
        ddIDToValueToAggregator.add(specificValueToAggregator);
        ddIDToValueToOTFAggregator.add(specificValueToOTFAggregator);
      }
    }

    ddIDToDD = Collections.unmodifiableList(ddIDToDD);
    ddIDToKeyDescriptor = Collections.unmodifiableList(ddIDToKeyDescriptor);

    //DD ID To Aggregator To Aggregate Descriptor

    logger.debug("Correct ddId {}", ddIDToValueToAggregator);

    ddIDToAggregatorToAggregateDescriptor = computeAggregatorToAggregateDescriptor(ddIDToValueToAggregator);
    ddIDToAggregatorToAggregateDescriptor = Collections.unmodifiableList(ddIDToAggregatorToAggregateDescriptor);

    //DD ID To OTF Aggregator To Aggregator Descriptor

    ddIDToOTFAggregatorToAggregateDescriptor = computeAggregatorToAggregateDescriptor(ddIDToValueToOTFAggregator);
    ddIDToOTFAggregatorToAggregateDescriptor = Collections.unmodifiableList(ddIDToOTFAggregatorToAggregateDescriptor);

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

    //Build id maps

    buildDDIDAggIDMaps();
  }

  private void buildDDIDAggIDMaps()
  {
    ddIDToAggIDs = Lists.newArrayList();
    ddIDToAggIDToInputAggDescriptor = Lists.newArrayList();
    ddIDToAggIDToOutputAggDescriptor = Lists.newArrayList();

    for(int index = 0;
        index < ddIDToAggregatorToAggregateDescriptor.size();
        index++) {
      IntArrayList aggIDList = new IntArrayList();
      Int2ObjectMap<FieldsDescriptor> inputMap = new Int2ObjectOpenHashMap<FieldsDescriptor>();
      Int2ObjectMap<FieldsDescriptor> outputMap = new Int2ObjectOpenHashMap<FieldsDescriptor>();

      ddIDToAggIDs.add(aggIDList);
      ddIDToAggIDToInputAggDescriptor.add(inputMap);
      ddIDToAggIDToOutputAggDescriptor.add(outputMap);

      for(Map.Entry<String, FieldsDescriptor> entry:
          ddIDToAggregatorToAggregateDescriptor.get(index).entrySet()) {
        String aggregatorName = entry.getKey();
        FieldsDescriptor inputDescriptor = entry.getValue();
        AggregatorStaticType aggType = AggregatorStaticType.valueOf(aggregatorName);
        aggIDList.add(aggType.ordinal());
        inputMap.put(aggType.ordinal(), inputDescriptor);
        outputMap.put(aggType.ordinal(),
                      aggType.getAggregator().getResultDescriptor(inputDescriptor));
      }
    }
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

  /**
   * @return the ddIDToValueToOTFAggregator
   */
  public List<Map<String, Set<String>>> getDdIDToValueToOTFAggregator()
  {
    return ddIDToValueToOTFAggregator;
  }

  /**
   * @return the ddIDToOTFAggregatorToAggregateDescriptor
   */
  public List<Map<String, FieldsDescriptor>> getDdIDToOTFAggregatorToAggregateDescriptor()
  {
    return ddIDToOTFAggregatorToAggregateDescriptor;
  }

  /**
   * @return the aggregatorInfo
   */
  public AggregatorInfo getAggregatorInfo()
  {
    return aggregatorInfo;
  }

  /**
   * @return the ddIDToAggIDToInputAggDescriptor
   */
  public List<Int2ObjectMap<FieldsDescriptor>> getDdIDToAggIDToInputAggDescriptor()
  {
    return ddIDToAggIDToInputAggDescriptor;
  }

  /**
   * @return the ddIDToAggIDToOutputAggDescriptor
   */
  public List<Int2ObjectMap<FieldsDescriptor>> getDdIDToAggIDToOutputAggDescriptor()
  {
    return ddIDToAggIDToOutputAggDescriptor;
  }

  /**
   * @return the ddIDToAggIDs
   */
  public List<IntArrayList> getDdIDToAggIDs()
  {
    return ddIDToAggIDs;
  }

  public Map<String, Type> getAllFieldToType()
  {
    Map<String, Type> fieldToType = Maps.newHashMap();
    fieldToType.putAll(keyDescriptor.getFieldToType());
    fieldToType.putAll(inputValuesDescriptor.getFieldToType());

    if(!(timeBuckets.size() == 1 && timeBuckets.get(0) == TimeBucket.ALL)) {
      fieldToType.put(DimensionsDescriptor.DIMENSION_TIME, DimensionsDescriptor.DIMENSION_TIME_TYPE);
    }

    return fieldToType;
  }

  /**
   * @return the combinationIDToKeys
   */
  public List<Fields> getCombinationIDToKeys()
  {
    return combinationIDToKeys;
  }

  /**
   * @return the combinationIDToFieldToAggregatorAdditionalValues
   */
  public List<Map<String, Set<String>>> getCombinationIDToFieldToAggregatorAdditionalValues()
  {
    return combinationIDToFieldToAggregatorAdditionalValues;
  }

  /**
   * @return the schemaAllValueToAggregatorToType
   */
  public Map<String, Map<String, Type>> getSchemaAllValueToAggregatorToType()
  {
    return schemaAllValueToAggregatorToType;
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 97 * hash + (this.keyDescriptor != null ? this.keyDescriptor.hashCode() : 0);
    hash = 97 * hash + (this.inputValuesDescriptor != null ? this.inputValuesDescriptor.hashCode() : 0);
    hash = 97 * hash + (this.keysToValuesList != null ? this.keysToValuesList.hashCode() : 0);
    hash = 97 * hash + (this.ddIDToKeyDescriptor != null ? this.ddIDToKeyDescriptor.hashCode() : 0);
    hash = 97 * hash + (this.ddIDToDD != null ? this.ddIDToDD.hashCode() : 0);
    hash = 97 * hash + (this.ddIDToValueToAggregator != null ? this.ddIDToValueToAggregator.hashCode() : 0);
    hash = 97 * hash + (this.ddIDToValueToOTFAggregator != null ? this.ddIDToValueToOTFAggregator.hashCode() : 0);
    hash = 97 * hash + (this.ddIDToAggregatorToAggregateDescriptor != null ? this.ddIDToAggregatorToAggregateDescriptor.hashCode() : 0);
    hash = 97 * hash + (this.ddIDToOTFAggregatorToAggregateDescriptor != null ? this.ddIDToOTFAggregatorToAggregateDescriptor.hashCode() : 0);
    hash = 97 * hash + (this.dimensionsDescriptorToID != null ? this.dimensionsDescriptorToID.hashCode() : 0);
    hash = 97 * hash + (this.ddIDToAggIDToInputAggDescriptor != null ? this.ddIDToAggIDToInputAggDescriptor.hashCode() : 0);
    hash = 97 * hash + (this.ddIDToAggIDToOutputAggDescriptor != null ? this.ddIDToAggIDToOutputAggDescriptor.hashCode() : 0);
    hash = 97 * hash + (this.ddIDToAggIDs != null ? this.ddIDToAggIDs.hashCode() : 0);
    hash = 97 * hash + (this.combinationIDToFieldToAggregatorAdditionalValues != null ? this.combinationIDToFieldToAggregatorAdditionalValues.hashCode() : 0);
    hash = 97 * hash + (this.combinationIDToKeys != null ? this.combinationIDToKeys.hashCode() : 0);
    hash = 97 * hash + (this.keysString != null ? this.keysString.hashCode() : 0);
    hash = 97 * hash + (this.bucketsString != null ? this.bucketsString.hashCode() : 0);
    hash = 97 * hash + (this.aggregatorInfo != null ? this.aggregatorInfo.hashCode() : 0);
    hash = 97 * hash + (this.timeBuckets != null ? this.timeBuckets.hashCode() : 0);
    hash = 97 * hash + (this.schemaAllValueToAggregatorToType != null ? this.schemaAllValueToAggregatorToType.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    int counter = 0;
    counter++;
    logger.debug("here {}", counter);
    if(obj == null) {
      return false;
    }
    if(getClass() != obj.getClass()) {
      return false;
    }
    final DimensionalEventSchema other = (DimensionalEventSchema)obj;
    if(this.keyDescriptor != other.keyDescriptor && (this.keyDescriptor == null || !this.keyDescriptor.equals(other.keyDescriptor))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.inputValuesDescriptor != other.inputValuesDescriptor && (this.inputValuesDescriptor == null || !this.inputValuesDescriptor.equals(other.inputValuesDescriptor))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.keysToValuesList != other.keysToValuesList && (this.keysToValuesList == null || !this.keysToValuesList.equals(other.keysToValuesList))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.ddIDToKeyDescriptor != other.ddIDToKeyDescriptor && (this.ddIDToKeyDescriptor == null || !this.ddIDToKeyDescriptor.equals(other.ddIDToKeyDescriptor))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.ddIDToDD != other.ddIDToDD && (this.ddIDToDD == null || !this.ddIDToDD.equals(other.ddIDToDD))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.ddIDToValueToAggregator != other.ddIDToValueToAggregator && (this.ddIDToValueToAggregator == null || !this.ddIDToValueToAggregator.equals(other.ddIDToValueToAggregator))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.ddIDToValueToOTFAggregator != other.ddIDToValueToOTFAggregator && (this.ddIDToValueToOTFAggregator == null || !this.ddIDToValueToOTFAggregator.equals(other.ddIDToValueToOTFAggregator))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.ddIDToAggregatorToAggregateDescriptor != other.ddIDToAggregatorToAggregateDescriptor && (this.ddIDToAggregatorToAggregateDescriptor == null || !this.ddIDToAggregatorToAggregateDescriptor.equals(other.ddIDToAggregatorToAggregateDescriptor))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.ddIDToOTFAggregatorToAggregateDescriptor != other.ddIDToOTFAggregatorToAggregateDescriptor && (this.ddIDToOTFAggregatorToAggregateDescriptor == null || !this.ddIDToOTFAggregatorToAggregateDescriptor.equals(other.ddIDToOTFAggregatorToAggregateDescriptor))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.dimensionsDescriptorToID != other.dimensionsDescriptorToID && (this.dimensionsDescriptorToID == null || !this.dimensionsDescriptorToID.equals(other.dimensionsDescriptorToID))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.ddIDToAggIDToInputAggDescriptor != other.ddIDToAggIDToInputAggDescriptor && (this.ddIDToAggIDToInputAggDescriptor == null || !this.ddIDToAggIDToInputAggDescriptor.equals(other.ddIDToAggIDToInputAggDescriptor))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.ddIDToAggIDToOutputAggDescriptor != other.ddIDToAggIDToOutputAggDescriptor && (this.ddIDToAggIDToOutputAggDescriptor == null || !this.ddIDToAggIDToOutputAggDescriptor.equals(other.ddIDToAggIDToOutputAggDescriptor))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.ddIDToAggIDs != other.ddIDToAggIDs && (this.ddIDToAggIDs == null || !this.ddIDToAggIDs.equals(other.ddIDToAggIDs))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.combinationIDToFieldToAggregatorAdditionalValues != other.combinationIDToFieldToAggregatorAdditionalValues && (this.combinationIDToFieldToAggregatorAdditionalValues == null || !this.combinationIDToFieldToAggregatorAdditionalValues.equals(other.combinationIDToFieldToAggregatorAdditionalValues))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.combinationIDToKeys != other.combinationIDToKeys && (this.combinationIDToKeys == null || !this.combinationIDToKeys.equals(other.combinationIDToKeys))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if((this.keysString == null) ? (other.keysString != null) : !this.keysString.equals(other.keysString)) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if((this.bucketsString == null) ? (other.bucketsString != null) : !this.bucketsString.equals(other.bucketsString)) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.aggregatorInfo != other.aggregatorInfo && (this.aggregatorInfo == null || !this.aggregatorInfo.equals(other.aggregatorInfo))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.timeBuckets != other.timeBuckets && (this.timeBuckets == null || !this.timeBuckets.equals(other.timeBuckets))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    if(this.schemaAllValueToAggregatorToType != other.schemaAllValueToAggregatorToType && (this.schemaAllValueToAggregatorToType == null || !this.schemaAllValueToAggregatorToType.equals(other.schemaAllValueToAggregatorToType))) {
      return false;
    }
    counter++;
    logger.debug("here {}", counter);
    return true;
  }

  private static <T> Set<T> intersection(Set<T> a, Set<T> b)
  {
    Set<T> result = Sets.newHashSet();

    for(T val: a) {
      if(b.contains(val)) {
        result.add(val);
      }
    }

    return result;
  }

  public static class Value
  {
    private String name;
    private Type type;
    private Set<String> aggregators;

    public Value(String name,
                 Type type,
                 Set<String> aggregators)
    {
      setName(name);
      setType(type);
      setAggregators(aggregators);
    }

    private void setName(@NotNull String name)
    {
      this.name = Preconditions.checkNotNull(name);
    }

    private void setType(@NotNull Type type)
    {
      this.type = Preconditions.checkNotNull(type);
    }

    private void setAggregators(@NotNull Set<String> aggregators)
    {
      Preconditions.checkNotNull(aggregators);

      for(String aggregator: aggregators) {
        Preconditions.checkNotNull(aggregator);
      }

      this.aggregators = Sets.newHashSet(aggregators);
    }

    /**
     * @return the name
     */
    public String getName()
    {
      return name;
    }

    /**
     * @return the type
     */
    public Type getType()
    {
      return type;
    }

    /**
     * @return the aggregators
     */
    public Set<String> getAggregators()
    {
      return aggregators;
    }
  }

  public static class Key
  {
    private String name;
    private Type type;
    private List<Object> enumValues;

    public Key(String name,
               Type type,
               List<Object> enumValues)
    {
      setName(name);
      setType(type);
      setEnumValues(enumValues);
    }

    private void setName(@NotNull String name)
    {
      this.name = Preconditions.checkNotNull(name);
    }

    private void setType(@NotNull Type type)
    {
      this.type = Preconditions.checkNotNull(type);
    }

    private void setEnumValues(@NotNull List<Object> enumValues)
    {
      Preconditions.checkNotNull(enumValues);

      for(Object values: enumValues) {
        Preconditions.checkNotNull(values);
      }

      this.enumValues = enumValues;
    }

    /**
     * @return the name
     */
    public String getName()
    {
      return name;
    }

    /**
     * @return the type
     */
    public Type getType()
    {
      return type;
    }

    /**
     * @return the enumValues
     */
    public List<Object> getEnumValues()
    {
      return enumValues;
    }
  }

  public static class DimensionsCombination
  {
    private Fields fields;
    private Map<String, Set<String>> valueToAggregators;

    public DimensionsCombination(Fields fields,
                                 Map<String, Set<String>> valueToAggregators)
    {
      setFields(fields);
      setValueToAggregators(valueToAggregators);
    }

    private void setFields(@NotNull Fields fields)
    {
      this.fields = Preconditions.checkNotNull(fields);
    }

    public Fields getFields()
    {
      return fields;
    }

    private void setValueToAggregators(@NotNull Map<String, Set<String>> valueToAggregators)
    {
      Preconditions.checkNotNull(valueToAggregators);
      Map<String, Set<String>> newValueToAggregators = Maps.newHashMap();

      for(Map.Entry<String, Set<String>> entry: valueToAggregators.entrySet()) {
        Preconditions.checkNotNull(entry.getKey());
        Preconditions.checkNotNull(entry.getValue());

        newValueToAggregators.put(entry.getKey(), Sets.newHashSet(entry.getValue()));

        for(String aggregator: entry.getValue()) {
          Preconditions.checkNotNull(aggregator);
        }
      }

      this.valueToAggregators = newValueToAggregators;
    }

    public Map<String, Set<String>> getValueToAggregators()
    {
      return valueToAggregators;
    }
  }
}
