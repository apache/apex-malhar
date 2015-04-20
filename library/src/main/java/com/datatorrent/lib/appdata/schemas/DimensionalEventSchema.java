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
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
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
  private List<Map<String, Set<String>>> ddIDToValueToOTFAggregator;
  private List<Map<String, FieldsDescriptor>> ddIDToAggregatorToAggregateDescriptor;
  private List<Map<String, FieldsDescriptor>> ddIDToOTFAggregatorToAggregateDescriptor;
  private Map<DimensionsDescriptor, Integer> dimensionsDescriptorToID;

  private List<Int2ObjectMap<FieldsDescriptor>> ddIDToAggIDToInputAggDescriptor;
  private List<Int2ObjectMap<FieldsDescriptor>> ddIDToAggIDToOutputAggDescriptor;
  private List<IntArrayList> ddIDToAggIDs;

  private String dimensionsString;
  private String keysString;
  private String bucketsString;

  private AggregatorInfo aggregatorInfo;

  public DimensionalEventSchema()
  {
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

  private List<Map<String, FieldsDescriptor>> computeAggregatorToAggregateDescriptor(List<Map<String, Set<String>>> ddIDToValueToAggregator,
                                                                                     Map<String, Type> aggFieldToType)
  {
    List<Map<String, FieldsDescriptor>> ddIDToAggregatorToAggregateDescriptor = Lists.newArrayList();

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

    return ddIDToAggregatorToAggregateDescriptor;
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
    Map<String, Set<String>> allValueToOTFAggregator = Maps.newHashMap();
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

            aggregatorNames.addAll(aggregatorInfo.getOTFAggregatorToStaticAggregators().get(aggregatorName));
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
    ddIDToValueToOTFAggregator = Lists.newArrayList();
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

    ddIDToAggregatorToAggregateDescriptor = computeAggregatorToAggregateDescriptor(ddIDToValueToAggregator,
                                                                                   aggFieldToType);
    ddIDToAggregatorToAggregateDescriptor = Collections.unmodifiableList(ddIDToAggregatorToAggregateDescriptor);

    //DD ID To OTF Aggregator To Aggregator Descriptor

    ddIDToOTFAggregatorToAggregateDescriptor = computeAggregatorToAggregateDescriptor(ddIDToValueToOTFAggregator,
                                                                                      aggFieldToType);
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

    allValueToAggregator = Collections.unmodifiableMap(allValueToAggregatorUnmodifiable);

    //Build id maps

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
   * @param ddIDToAggIDToInputAggDescriptor the ddIDToAggIDToInputAggDescriptor to set
   */
  public void setDdIDToAggIDToInputAggDescriptor(List<Int2ObjectMap<FieldsDescriptor>> ddIDToAggIDToInputAggDescriptor)
  {
    this.ddIDToAggIDToInputAggDescriptor = ddIDToAggIDToInputAggDescriptor;
  }

  /**
   * @return the ddIDToAggIDToOutputAggDescriptor
   */
  public List<Int2ObjectMap<FieldsDescriptor>> getDdIDToAggIDToOutputAggDescriptor()
  {
    return ddIDToAggIDToOutputAggDescriptor;
  }

  /**
   * @param ddIDToAggIDToOutputAggDescriptor the ddIDToAggIDToOutputAggDescriptor to set
   */
  public void setDdIDToAggIDToOutputAggDescriptor(List<Int2ObjectMap<FieldsDescriptor>> ddIDToAggIDToOutputAggDescriptor)
  {
    this.ddIDToAggIDToOutputAggDescriptor = ddIDToAggIDToOutputAggDescriptor;
  }

  /**
   * @return the ddIDToAggIDs
   */
  public List<IntArrayList> getDdIDToAggIDs()
  {
    return ddIDToAggIDs;
  }
}
