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
import com.datatorrent.lib.appdata.dimensions.DimensionsStaticAggregator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
 * Optional
 * Schema stub
 *{
 *  "time": {
 *    "from":1123455556656,
 *    "to":382390859384
 *   }
 *}
 */
public class SchemaDimensional implements Schema
{
  private static final Logger logger = LoggerFactory.getLogger(SchemaDimensional.class);

  public static final String SCHEMA_TYPE = "dimensions";
  public static final String SCHEMA_VERSION = "1.0";

  public static final List<Fields> VALID_KEYS = ImmutableList.of(new Fields(Sets.newHashSet(SchemaWithTime.FIELD_TIME)));
  public static final List<Fields> VALID_TIME_KEYS = ImmutableList.of(new Fields(Sets.newHashSet(SchemaWithTime.FIELD_TIME_FROM,
                                                                                                 SchemaWithTime.FIELD_TIME_TO)));

  private Long from;
  private Long to;

  private boolean changed = false;
  private boolean changedFromTo = false;
  private String schemaJSON;

  private DimensionalEventSchema eventSchema;
  private JSONObject schema;
  private JSONObject time;
  private JSONArray keys;

  private boolean fixedFromTo = false;

  private Map<String, String> schemaKeys;
  private Map<String, List<Object>> updatedEnums;

  public SchemaDimensional(String schemaStub,
                           DimensionalEventSchema eventSchema,
                           Map<String, String> schemaKeys)
  {
    this(eventSchema,
         schemaKeys);

    if(schemaStub != null) {
      fixedFromTo = true;
      try {
        setSchemaStub(schemaStub);
      }
      catch(Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public SchemaDimensional(String schemaStub,
                           DimensionalEventSchema eventSchema)
  {
    this(schemaStub,
         eventSchema,
         null);
  }

  public SchemaDimensional(DimensionalEventSchema eventSchema,
                           Map<String, String> schemaKeys)
  {
    setEventSchema(eventSchema);
    setSchemaKeys(schemaKeys);

    try {
      initialize();
    }
    catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  public SchemaDimensional(DimensionalEventSchema eventSchema)
  {
    this(eventSchema,
         null);
  }

  public AggregatorInfo getAggregatorInfo()
  {
    return eventSchema.getAggregatorInfo();
  }

  @Override
  public final void setSchemaKeys(Map<String, String> schemaKeys)
  {
    if(schemaKeys == null) {
      return;
    }

    for(Map.Entry<String, String> entry: schemaKeys.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.schemaKeys = Collections.unmodifiableMap(Maps.newHashMap(schemaKeys));
  }

  private void setEventSchema(DimensionalEventSchema eventSchema)
  {
    this.eventSchema = Preconditions.checkNotNull(eventSchema, "eventSchema");
  }

  private void setSchemaStub(String schemaStub) throws Exception
  {
    JSONObject jo = new JSONObject(schemaStub);
    SchemaUtils.checkValidKeysEx(jo, VALID_KEYS);

    JSONObject tempTime = jo.getJSONObject(SchemaWithTime.FIELD_TIME);
    SchemaUtils.checkValidKeys(jo, VALID_TIME_KEYS);

    this.from = tempTime.getLong(SchemaWithTime.FIELD_TIME_FROM);
    this.to = tempTime.getLong(SchemaWithTime.FIELD_TIME_TO);
  }

  private void initialize() throws Exception
  {
    schema = new JSONObject();
    schema.put(SchemaTabular.FIELD_SCHEMA_TYPE, SchemaDimensional.SCHEMA_TYPE);
    schema.put(SchemaTabular.FIELD_SCHEMA_VERSION, SchemaDimensional.SCHEMA_VERSION);

    //time
    time = new JSONObject();
    schema.put(SchemaWithTime.FIELD_TIME, time);
    JSONArray bucketsArray = new JSONArray(eventSchema.getBucketsString());
    time.put(SchemaWithTime.FIELD_TIME_BUCKETS, bucketsArray);

    //keys
    keys = new JSONArray(eventSchema.getKeysString());
    schema.put(DimensionalEventSchema.FIELD_KEYS, keys);

    //values;
    JSONArray values = new JSONArray();
    schema.put(SchemaTabular.FIELD_VALUES, values);

    FieldsDescriptor inputValuesDescriptor = eventSchema.getInputValuesDescriptor();
    Map<String, Map<String, Type>> allValueToAggregator = eventSchema.getSchemaAllValueToAggregatorToType();

    for(Map.Entry<String, Map<String, Type>> entry: allValueToAggregator.entrySet()) {
      String valueName = entry.getKey();

      for(Map.Entry<String, Type> entryAggType: entry.getValue().entrySet()) {
        String aggregatorName = entryAggType.getKey();
        Type outputValueType = entryAggType.getValue();

        JSONObject value = new JSONObject();
        String combinedName = valueName +
                              DimensionalEventSchema.ADDITIONAL_VALUE_SEPERATOR +
                              aggregatorName;
        value.put(SchemaTabular.FIELD_VALUES_NAME, combinedName);
        value.put(SchemaTabular.FIELD_VALUES_TYPE, outputValueType.getName());
        values.put(value);
      }
    }

    JSONArray dimensions = new JSONArray();

    for(int combinationID = 0;
        combinationID < eventSchema.getCombinationIDToKeys().size();
        combinationID++) {

      Fields fields = eventSchema.getCombinationIDToKeys().get(combinationID);
      Map<String, Set<String>> fieldToAggregatorAdditionalValues =
      eventSchema.getCombinationIDToFieldToAggregatorAdditionalValues().get(combinationID);

      JSONObject combination = new JSONObject();
      JSONArray combinationArray = new JSONArray();

      for(String field: fields.getFields()) {
        combinationArray.put(field);
      }

      combination.put(DimensionalEventSchema.FIELD_DIMENSIONS_COMBINATIONS, combinationArray);

      if(!fieldToAggregatorAdditionalValues.isEmpty()) {
        JSONArray additionalValueArray = new JSONArray();

        for(Map.Entry<String, Set<String>> entry: fieldToAggregatorAdditionalValues.entrySet()) {
          JSONObject additionalValueObject = new JSONObject();

          String valueName = entry.getKey();
          
          for(String aggregatorName: entry.getValue()) {
            String combinedName = valueName
                                  + DimensionalEventSchema.ADDITIONAL_VALUE_SEPERATOR
                                  + aggregatorName;
            Type inputValueType = inputValuesDescriptor.getType(valueName);

            DimensionsStaticAggregator aggregator
                    = eventSchema.getAggregatorInfo().getStaticAggregatorNameToStaticAggregator().get(aggregatorName);
            Type outputValueType = aggregator.getTypeMap().getTypeMap().get(inputValueType);

            additionalValueObject.put(DimensionalEventSchema.FIELD_VALUES_NAME, combinedName);
            additionalValueObject.put(DimensionalEventSchema.FIELD_VALUES_TYPE, outputValueType.getName());

            additionalValueArray.put(additionalValueObject);
          }
        }

        combination.put(DimensionalEventSchema.FIELD_DIMENSIONS_ADDITIONAL_VALUES, additionalValueArray);
      }

      dimensions.put(combination);
    }

    schema.put(DimensionalEventSchema.FIELD_DIMENSIONS, dimensions);
  }

  public void setFrom(Long from)
  {
    this.from = from;
    changed = true;
    changedFromTo = true;
  }

  public void setTo(Long to)
  {
    this.to = to;
    changed = true;
    changedFromTo = true;
  }

  public void setEnumsSet(Map<String, Set<Object>> enums)
  {
    Preconditions.checkNotNull(enums);

    Map<String, List<Object>> enumsList = Maps.newHashMap();

    //Check that all the given keys are valid
    Preconditions.checkArgument(
            eventSchema.getAllKeysDescriptor().getFields().getFields().containsAll(enums.keySet()),
            "The given map doesn't contain valid keys. Valid keys are %s and the provided keys are %s",
            eventSchema.getAllKeysDescriptor().getFields().getFields(),
            enums.keySet());

    //Todo check the type of the objects, for now just set them on the enum.

    for(Map.Entry<String, Set<Object>> entry: enums.entrySet()) {
      String name = entry.getKey();
      Set<Object> vals = entry.getValue();

      Preconditions.checkNotNull(name);
      Preconditions.checkNotNull(vals);

      for(Object value: entry.getValue()) {
        Preconditions.checkNotNull(value);
      }

      List<Object> valsList = Lists.newArrayList(vals);
      enumsList.put(name, valsList);
    }

    updatedEnums = Maps.newHashMap(enumsList);
  }

  @SuppressWarnings({"rawtypes","unchecked"})
  public void setEnumsSetComparable(Map<String, Set<Comparable>> enums)
  {
    Preconditions.checkNotNull(enums);

    Map<String, List<Object>> enumsList = Maps.newHashMap();

    //Check that all the given keys are valid
    Preconditions.checkArgument(
            eventSchema.getAllKeysDescriptor().getFields().getFields().containsAll(enums.keySet()),
            "The given map doesn't contain valid keys. Valid keys are %s and the provided keys are %s",
            eventSchema.getAllKeysDescriptor().getFields().getFields(),
            enums.keySet());

    //Todo check the type of the objects, for now just set them on the enum.

    for(Map.Entry<String, Set<Comparable>> entry: enums.entrySet()) {
      String name = entry.getKey();
      Set<Comparable> vals = entry.getValue();

      Preconditions.checkNotNull(name);
      Preconditions.checkNotNull(vals);

      for(Object value: entry.getValue()) {
        Preconditions.checkNotNull(value);
      }

      List<Comparable> valsListComparable = Lists.newArrayList(vals);
      Collections.sort(valsListComparable);
      List<Object> valsList = (List) valsListComparable;
      enumsList.put(name, valsList);
    }

    updatedEnums = Maps.newHashMap(enumsList);
  }

  public void setEnumsList(Map<String, List<Object>> enums)
  {
    Preconditions.checkNotNull(enums);

    //Check that all the given keys are valid
    Preconditions.checkArgument(
            eventSchema.getAllKeysDescriptor().getFields().getFields().containsAll(enums.keySet()),
            "The given map doesn't contain valid keys. Valid keys are %s and the provided keys are %s",
            eventSchema.getAllKeysDescriptor().getFields().getFields(),
            enums.keySet());

    //Todo check the type of the objects, for now just set them on the enum.

    for(Map.Entry<String, List<Object>> entry: enums.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    Map<String, List<Object>> tempEnums = Maps.newHashMap();

    for(Map.Entry<String, List<Object>> entry: enums.entrySet()) {
      String key = entry.getKey();
      List<?> enumValues = entry.getValue();
      List<Object> tempEnumValues = Lists.newArrayList();

      for(Object enumValue: enumValues) {
        tempEnumValues.add(enumValue);
      }

      tempEnums.put(key, tempEnumValues);
    }

    updatedEnums = tempEnums;
  }

  @Override
  public String getSchemaJSON()
  {
    if(!changed && schemaJSON != null) {
      return schemaJSON;
    }

    changed = false;

    if(changedFromTo) {
      changedFromTo = false;
      Preconditions.checkState(!(from == null ^ to == null),
                               "Either both from and to should be set or both should be not set.");

      if(from != null) {
        Preconditions.checkState(to > from, "to must be greater than from.");
      }

      if(from == null) {
        time.remove(SchemaWithTime.FIELD_TIME_FROM);
        time.remove(SchemaWithTime.FIELD_TIME_TO);
      }
      else {
        try {
          time.put(SchemaWithTime.FIELD_TIME_FROM, from);
          time.put(SchemaWithTime.FIELD_TIME_TO, to);
        }
        catch(JSONException ex) {
          throw new RuntimeException(ex);
        }
      }
    }

    if(updatedEnums != null) {
      for(int keyIndex = 0;
          keyIndex < keys.length();
          keyIndex++) {
        JSONObject keyData;
        String name;

        try {
          keyData = keys.getJSONObject(keyIndex);
          name = keyData.getString(DimensionalEventSchema.FIELD_KEYS_NAME);
        }
        catch(JSONException ex) {
          throw new RuntimeException(ex);
        }

        List<Object> enumVals = updatedEnums.get(name);

        if(enumVals == null || enumVals.isEmpty()) {
          keyData.remove(DimensionalEventSchema.FIELD_KEYS_ENUMVALUES);
          continue;
        }

        JSONArray newEnumValues = new JSONArray();

        for(Object enumVal: enumVals) {
          newEnumValues.put(enumVal);
        }

        try {
          keyData.put(DimensionalEventSchema.FIELD_KEYS_ENUMVALUES, newEnumValues);
        }
        catch(JSONException ex) {
          throw new RuntimeException(ex);
        }
      }

      updatedEnums = null;
    }

    schemaJSON = schema.toString();
    return schemaJSON;
  }

  public DimensionalEventSchema getGenericEventSchema()
  {
    return eventSchema;
  }

  @Override
  public String getSchemaType()
  {
    return SCHEMA_TYPE;
  }

  @Override
  public String getSchemaVersion()
  {
    return SCHEMA_VERSION;
  }

  @Override
  public Map<String, String> getSchemaKeys()
  {
    return schemaKeys;
  }

  /**
   * @return the fixedFromTo
   */
  public boolean isFixedFromTo()
  {
    return fixedFromTo;
  }
}
