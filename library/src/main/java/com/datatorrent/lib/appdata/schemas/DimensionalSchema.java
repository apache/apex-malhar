/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.schemas;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator;

/**
 * The {@link DimensionalSchema} class represents the App Data dimensions schema. The App Data dimensions
 * schema is built from two sources: a {@link DimensionalConfigurationSchema} and an optional schema stub. The
 * {@link DimensionalConfigurationSchema} is responsible for defining the key, values, dimensions combinations,
 * and the aggregations performed for each dimensions combination. The schema stub defines the from and to
 * times for the App Data dimensions schema. For details on how to define the {@link DimensionalConfiguration}
 * schema please the documentation for the {@link DimensionalConfiguration} class. An example of a valid
 * schema stub which defines the from and to times is below:
 * <br/>
 * <br/>
 * {@code
 * {
 *   "time":
 *   {
 *     "from":1123455556656,
 *     "to":382390859384
 *   }
 * }
 * @since 3.1.0
 */
public class DimensionalSchema implements Schema
{
  /**
   * The type of the schema.
   */
  public static final String SCHEMA_TYPE = "dimensions";
  /**
   * The version of the schema.
   */
  public static final String SCHEMA_VERSION = "1.0";
  /**
   * The JSON key string corresponding to the from field.
   */
  public static final String FIELD_TIME_FROM = "from";
  /**
   * The JSON key string corresponding to the time field.
   */
  public static final String FIELD_TIME = "time";
  /**
   * The JSON key string corresponding to the to field.
   */
  public static final String FIELD_TIME_TO = "to";
  /**
   * The JSON key string corresponding to the buckets field.
   */
  public static final String FIELD_TIME_BUCKETS = "buckets";
  /**
   * The JSON key string corresponding to the slidingAggregateSupported field.
   */
  public static final String FIELD_SLIDING_AGGREGATE_SUPPORTED = "slidingAggregateSupported";
  /**
   * The JSON key string used to identify the tags.
   */
  //TODO To be removed when Malhar Library 3.3 becomes a dependency.
  private static final String FIELD_TAGS = "tags";

  public static final List<Fields> VALID_KEYS = ImmutableList.of(new Fields(Sets.newHashSet(FIELD_TIME)));
  public static final List<Fields> VALID_TIME_KEYS = ImmutableList.of(new Fields(Sets.newHashSet(FIELD_TIME_FROM, FIELD_TIME_TO)));

  /**
   * The from value for the schema. Null if there is no from value.
   */
  private Long from;
  /**
   * The to value for the schema. Null if there is no to value.
   */
  private Long to;
  /**
   * boolean flag indicating if any values in the schema have been changed.
   */
  private boolean changed = false;
  /**
   * boolean flag indicating if the from to fields in the schema have been changed.
   */
  private boolean changedFromTo = false;
  /**
   * boolean flag indicating if the schema keys have been updated for the schema.
   */
  private boolean changedSchemaKeys = false;
  /**
   * boolean flag indicating if the enum vals are updated.
   */
  private boolean areEnumsUpdated = false;
  /**
   * The AppData schema JSON string (which is returned in the schema query).
   */
  private String schemaJSON;
  /**
   * The {@link DimensionalConfigurationSchema} from which this {@link DimensionalSchema} was constructed.
   */
  private DimensionalConfigurationSchema configurationSchema;
  /**
   * The {@link JSONObject} representing the AppData dimensions schema.
   */
  private JSONObject schema;
  /**
   * The {@link JSONObject} representing the time section of the AppData dimensions schema.
   */
  private JSONObject time;
  /**
   * The {@link JSONObject} representing the keys section of the AppData dimensions schema.
   */
  private JSONArray keys;
  /**
   * This flag is true if there was a from and to time defined for this schema initially.
   */
  private boolean predefinedFromTo = false;
  /**
   * The schema keys for this schema.
   */
  private Map<String, String> schemaKeys;
  /**
   * The current enum vals for this schema.
   */
  private Map<String, List<Object>> currentEnumVals;
  /**
   * The schemaID assigned to this schema. This schemaID is only needed for operators
   * which need to host multiple schemas.
   */
  private int schemaID = Schema.DEFAULT_SCHEMA_ID;

  /**
   * Constructor for serialization
   */
  private DimensionalSchema()
  {
    //For kryo
  }

  /**
   * This creates a {@link DimensionalSchema} object from the given schema stub,
   * configuration schema, and schema keys.
   * @param schemaStub The schema stub to use when creating this {@link DimensionalSchema}.
   * @param configurationSchema The configuration schema to use when creating this {@link DimensionalSchema}.
   * @param schemaKeys The schemaKeys to use when creating this {@link DimensionalSchema}.
   */
  public DimensionalSchema(String schemaStub,
                           DimensionalConfigurationSchema configurationSchema,
                           Map<String, String> schemaKeys)
  {
    this(configurationSchema,
         schemaKeys);

    if(schemaStub != null) {
      predefinedFromTo = true;
      try {
        setSchemaStub(schemaStub);
      }
      catch(Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * This creates a {@link DimensionalSchema} object from the given schemaID, schemaStrub,configurationSchema, and schemaKeys.
   * @param schemaID The schemaID assigned to this schema.
   * @param schemaStub The schema stub to use when creating this {@link DimensionalSchema}.
   * @param configurationSchema The configuration schema to use when creating this {@link DimensionalSchema}.
   * @param schemaKeys The schemaKeys to use when creating this {@link DimensionalSchema}.
   */
  public DimensionalSchema(int schemaID,
                           String schemaStub,
                           DimensionalConfigurationSchema configurationSchema,
                           Map<String, String> schemaKeys)
  {
    this(schemaStub,
         configurationSchema,
         schemaKeys);

    this.schemaID = schemaID;
  }

  /**
   * This creates a {@link DimensionalSchema} from the given schemaStub and configuration schema.
   * @param schemaStub The schema stub to use when creating this {@link DimensionalSchema}.
   * @param configurationSchema The configuration schema to use when creating this {@link DimensionalSchema}.
   */
  public DimensionalSchema(String schemaStub,
                           DimensionalConfigurationSchema configurationSchema)
  {
    this(schemaStub,
         configurationSchema,
         null);
  }

  /**
   * This creates a {@link DimensionalSchema} from the given schemaID, schemaStub, and
   * configurationSchema.
   * @param schemaID The schemaID assigned to this schema.
   * @param schemaStub The schema stub to use when creating this {@link DimensionalSchema}.
   * @param configurationSchema The configuration schema to use when creating this {@link DimensionalSchema}.
   */
  public DimensionalSchema(int schemaID,
                           String schemaStub,
                           DimensionalConfigurationSchema configurationSchema)
  {
    this(schemaStub,
         configurationSchema);

    this.schemaID = schemaID;
  }

  /**
   * Creates a {@link DimensionalSchema} from the given configuration schema and schema keys.
   * @param configurationSchema The configuration schema from which to construct this {@link DimensionalEventSchema}.
   * @param schemaKeys The schemaKeys assigned to this schema.
   */
  public DimensionalSchema(DimensionalConfigurationSchema configurationSchema,
                           Map<String, String> schemaKeys)
  {
    setConfigurationSchema(configurationSchema);
    setSchemaKeys(schemaKeys);

    try {
      initialize();
    }
    catch(JSONException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a {@link DimensionalSchema} object from the given schemaID, configurationSchema,
   * and schemaKeys.
   * @param schemaID The schemaID assigned to this schema.
   * @param configurationSchema The configuration schema from which this schema was constructed.
   * @param schemaKeys The schema keys assigned to this schema.
   */
  public DimensionalSchema(int schemaID,
                           DimensionalConfigurationSchema configurationSchema,
                           Map<String, String> schemaKeys)
  {
    this(configurationSchema,
         schemaKeys);

    this.schemaID = schemaID;
  }

  /**
   * Creates a {@link DimensionalSchema} object from the given configuration schema.
   * @param configurationSchema The configuration schema from which to construct this
   * schema.
   */
  public DimensionalSchema(DimensionalConfigurationSchema configurationSchema)
  {
    this(configurationSchema,
         null);
  }

  /**
   * Creates a {@link DimensionalSchema} object with the given schema ID and
   * configuration schema.
   * @param schemaID The schemaID assigned to this schema.
   * @param configurationSchema The configuration schema from which this schema as constructed.
   */
  public DimensionalSchema(int schemaID,
                           DimensionalConfigurationSchema configurationSchema)
  {
    this(configurationSchema);
    this.schemaID = schemaID;
  }

  /**
   * Returns the aggregator registry assigned to this schema object.
   * @return The aggregator registry.
   */
  public AggregatorRegistry getAggregatorRegistry()
  {
    return configurationSchema.getAggregatorRegistry();
  }

  @Override
  public final void setSchemaKeys(Map<String, String> schemaKeys)
  {
    changed = true;
    changedSchemaKeys = true;

    if(schemaKeys == null) {
      this.schemaKeys = null;
      return;
    }

    for(Map.Entry<String, String> entry: schemaKeys.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.schemaKeys = Maps.newHashMap(schemaKeys);
  }

  /**
   * This is a helper method for setting the configuration schema.
   * @param configurationSchema The configuration schema.
   */
  private void setConfigurationSchema(DimensionalConfigurationSchema configurationSchema)
  {
    this.configurationSchema = Preconditions.checkNotNull(configurationSchema, "eventSchema");
  }

  /**
   * This is a helper method extracts and validates the information contained in the schema stub for this schema.
   * @param schemaStub The schema stub to extract information from and validate.
   * @throws JSONException This exception is thrown if there is an error processing the provided JSON schemaStub.
   */
  private void setSchemaStub(String schemaStub) throws JSONException
  {
    JSONObject jo = new JSONObject(schemaStub);
    SchemaUtils.checkValidKeysEx(jo, VALID_KEYS);

    JSONObject tempTime = jo.getJSONObject(FIELD_TIME);
    SchemaUtils.checkValidKeys(jo, VALID_TIME_KEYS);

    this.from = tempTime.getLong(FIELD_TIME_FROM);
    this.to = tempTime.getLong(FIELD_TIME_TO);
  }

  /**
   * Initializes the schema JSON and schema metadata.
   * @throws JSONException This exception is thrown when there is an
   * exception building the schema for the AppData dimensions schema.
   */
  private void initialize() throws JSONException
  {
    schema = new JSONObject();

    if(schemaKeys != null) {
      schema.put(Schema.FIELD_SCHEMA_KEYS,
                      SchemaUtils.createJSONObject(schemaKeys));
    }

    schema.put(SnapshotSchema.FIELD_SCHEMA_TYPE, DimensionalSchema.SCHEMA_TYPE);
    schema.put(SnapshotSchema.FIELD_SCHEMA_VERSION, DimensionalSchema.SCHEMA_VERSION);

    if (!configurationSchema.getTags().isEmpty()) {
      schema.put(FIELD_TAGS, new JSONArray(configurationSchema.getTags()));
    }

    //time
    time = new JSONObject();
    schema.put(FIELD_TIME, time);
    JSONArray bucketsArray = new JSONArray(configurationSchema.getBucketsString());
    time.put(FIELD_TIME_BUCKETS, bucketsArray);
    time.put(FIELD_SLIDING_AGGREGATE_SUPPORTED, true);

    //keys
    keys = new JSONArray(configurationSchema.getKeysString());

    for (int keyIndex = 0; keyIndex < keys.length(); keyIndex++) {
      JSONObject keyJo = keys.getJSONObject(keyIndex);
      String keyName = keyJo.getString(DimensionalConfigurationSchema.FIELD_KEYS_NAME);
      List<String> tags = configurationSchema.getKeyToTags().get(keyName);

      if (!tags.isEmpty()) {
        keyJo.put(FIELD_TAGS, new JSONArray(tags));
      }
    }

    schema.put(DimensionalConfigurationSchema.FIELD_KEYS, keys);

    //values
    JSONArray values = new JSONArray();
    schema.put(SnapshotSchema.FIELD_VALUES, values);

    FieldsDescriptor inputValuesDescriptor = configurationSchema.getInputValuesDescriptor();
    Map<String, Map<String, Type>> allValueToAggregator = configurationSchema.getSchemaAllValueToAggregatorToType();

    for(Map.Entry<String, Map<String, Type>> entry: allValueToAggregator.entrySet()) {
      String valueName = entry.getKey();

      for(Map.Entry<String, Type> entryAggType: entry.getValue().entrySet()) {
        String aggregatorName = entryAggType.getKey();
        Type outputValueType = entryAggType.getValue();

        JSONObject value = new JSONObject();
        String combinedName = valueName +
                              DimensionalConfigurationSchema.ADDITIONAL_VALUE_SEPERATOR +
                              aggregatorName;
        value.put(SnapshotSchema.FIELD_VALUES_NAME, combinedName);
        value.put(SnapshotSchema.FIELD_VALUES_TYPE, outputValueType.getName());

        List<String> tags = configurationSchema.getValueToTags().get(valueName);

        if (!tags.isEmpty()) {
          value.put(FIELD_TAGS, new JSONArray(tags));
        }

        values.put(value);
      }
    }

    JSONArray dimensions = new JSONArray();

    for(int combinationID = 0;
        combinationID < configurationSchema.getDimensionsDescriptorIDToKeys().size();
        combinationID++) {

      Fields fields = configurationSchema.getDimensionsDescriptorIDToKeys().get(combinationID);
      Map<String, Set<String>> fieldToAggregatorAdditionalValues =
      configurationSchema.getDimensionsDescriptorIDToFieldToAggregatorAdditionalValues().get(combinationID);

      JSONObject combination = new JSONObject();
      JSONArray combinationArray = new JSONArray();

      for(String field: fields.getFields()) {
        combinationArray.put(field);
      }

      combination.put(DimensionalConfigurationSchema.FIELD_DIMENSIONS_COMBINATIONS, combinationArray);

      if(!fieldToAggregatorAdditionalValues.isEmpty()) {
        JSONArray additionalValueArray = new JSONArray();

        for(Map.Entry<String, Set<String>> entry: fieldToAggregatorAdditionalValues.entrySet()) {
          String valueName = entry.getKey();

          for(String aggregatorName: entry.getValue()) {
            JSONObject additionalValueObject = new JSONObject();
            String combinedName = valueName
                                  + DimensionalConfigurationSchema.ADDITIONAL_VALUE_SEPERATOR
                                  + aggregatorName;
            Type inputValueType = inputValuesDescriptor.getType(valueName);

            if(!configurationSchema.getAggregatorRegistry().isAggregator(aggregatorName)) {
              if(aggregatorName == null) {
                LOG.error("{} is not a valid aggregator.", aggregatorName);
              }
            }

            Type outputValueType;

            if(configurationSchema.getAggregatorRegistry().isIncrementalAggregator(aggregatorName)) {
              IncrementalAggregator aggregator
                      = configurationSchema.getAggregatorRegistry().getNameToIncrementalAggregator().get(aggregatorName);

              outputValueType = aggregator.getOutputType(inputValueType);
            }
            else {
              outputValueType = configurationSchema.getAggregatorRegistry().getNameToOTFAggregators().get(aggregatorName).getOutputType();
            }

            additionalValueObject.put(DimensionalConfigurationSchema.FIELD_VALUES_NAME, combinedName);
            additionalValueObject.put(DimensionalConfigurationSchema.FIELD_VALUES_TYPE, outputValueType.getName());
            additionalValueArray.put(additionalValueObject);
          }
        }

        combination.put(DimensionalConfigurationSchema.FIELD_DIMENSIONS_ADDITIONAL_VALUES, additionalValueArray);
      }

      dimensions.put(combination);
    }

    schema.put(DimensionalConfigurationSchema.FIELD_DIMENSIONS, dimensions);

    this.schemaJSON = this.schema.toString();
  }

  /**
   * Sets the from time for the schema.
   * @param from The from time for the schema.
   */
  public void setFrom(Long from)
  {
    this.from = from;
    changed = true;
    changedFromTo = true;
  }

  /**
   * Sets the to time for the schema.
   * @param to The to time for the schema.
   */
  public void setTo(Long to)
  {
    this.to = to;
    changed = true;
    changedFromTo = true;
  }

  /**
   * Sets the new enum lists for this schema. The sets in the provided maps are converted into lists.
   * @param enums The new enum sets for this schema.
   */
  public void setEnumsSet(Map<String, Set<Object>> enums)
  {
    Preconditions.checkNotNull(enums);
    areEnumsUpdated = true;

    Map<String, List<Object>> enumsList = Maps.newHashMap();

    //Check that all the given keys are valid
    Preconditions.checkArgument(
            configurationSchema.getKeyDescriptor().getFields().getFields().containsAll(enums.keySet()),
            "The given map doesn't contain valid keys. Valid keys are %s and the provided keys are %s",
            configurationSchema.getKeyDescriptor().getFields().getFields(),
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

    currentEnumVals = Maps.newHashMap(enumsList);
  }

  /**
   * Sets the new enum lists for this schema. The sets in the provided maps are converted into lists, and
   * sorted according to their natural ordering.
   * @param enums The new enum sets for this schema.
   */
  @SuppressWarnings({"rawtypes","unchecked"})
  public void setEnumsSetComparable(Map<String, Set<Comparable>> enums)
  {
    Preconditions.checkNotNull(enums);
    areEnumsUpdated = true;

    Map<String, List<Object>> enumsList = Maps.newHashMap();

    //Check that all the given keys are valid
    Preconditions.checkArgument(
            configurationSchema.getKeyDescriptor().getFields().getFields().containsAll(enums.keySet()),
            "The given map doesn't contain valid keys. Valid keys are %s and the provided keys are %s",
            configurationSchema.getKeyDescriptor().getFields().getFields(),
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

    currentEnumVals = Maps.newHashMap(enumsList);
  }

  /**
   * Sets the new enum lists for this schema.
   * @param enums The new enum lists for this schema.
   */
  public void setEnumsList(Map<String, List<Object>> enums)
  {
    Preconditions.checkNotNull(enums);
    areEnumsUpdated = true;

    //Check that all the given keys are valid
    Preconditions.checkArgument(
            configurationSchema.getKeyDescriptor().getFields().getFields().containsAll(enums.keySet()),
            "The given map doesn't contain valid keys. Valid keys are %s and the provided keys are %s",
            configurationSchema.getKeyDescriptor().getFields().getFields(),
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

    currentEnumVals = tempEnums;
  }

  @Override
  public String getSchemaJSON()
  {
    if(!changed && schemaJSON != null) {
      //If there are no changes, return the pre computed JSON
      return schemaJSON;
    }

    if(changedSchemaKeys) {
      //If the schema keys change, recompute the schema keys portion of the JSON
      changedSchemaKeys = false;

      if(schemaKeys == null) {
        schema.remove(Schema.FIELD_SCHEMA_KEYS);
      }
      else {
        try {
          schema.put(Schema.FIELD_SCHEMA_KEYS,
                          SchemaUtils.createJSONObject(schemaKeys));
        }
        catch(JSONException ex) {
          throw new RuntimeException(ex);
        }
      }
    }

    if(changedFromTo) {
      //If the from to times have changed then recompute the time portion of the schema.
      changedFromTo = false;
      Preconditions.checkState(!(from == null ^ to == null),
                               "Either both from and to should be set or both should be not set.");

      if(from != null) {
        Preconditions.checkState(to >= from, "to {} must be greater than or equal to from {}.", to, from);
      }

      if(from == null) {
        time.remove(FIELD_TIME_FROM);
        time.remove(FIELD_TIME_TO);
      }
      else {
        try {
          time.put(FIELD_TIME_FROM, from);
          time.put(FIELD_TIME_TO, to);
        }
        catch(JSONException ex) {
          throw new RuntimeException(ex);
        }
      }
    }

    if(this.areEnumsUpdated) {
      //If the enums have been updated, recompute the key portion of the schema.
      for(int keyIndex = 0;
          keyIndex < keys.length();
          keyIndex++) {
        JSONObject keyData;
        String name;

        try {
          keyData = keys.getJSONObject(keyIndex);
          name = keyData.getString(DimensionalConfigurationSchema.FIELD_KEYS_NAME);
        }
        catch(JSONException ex) {
          throw new RuntimeException(ex);
        }

        List<Object> enumVals = currentEnumVals.get(name);

        if(enumVals == null || enumVals.isEmpty()) {
          keyData.remove(DimensionalConfigurationSchema.FIELD_KEYS_ENUMVALUES);
          continue;
        }

        JSONArray newEnumValues = new JSONArray();

        for(Object enumVal: enumVals) {
          newEnumValues.put(enumVal);
        }

        try {
          keyData.put(DimensionalConfigurationSchema.FIELD_KEYS_ENUMVALUES, newEnumValues);
        }
        catch(JSONException ex) {
          throw new RuntimeException(ex);
        }
      }

      this.areEnumsUpdated = false;
    }

    //Rebuild the schema JSON string.
    schemaJSON = schema.toString();
    return schemaJSON;
  }

  /**
   * Gets the {@link DimensionalConfigurationSchema} from which this {@link DimensionalSchema}.
   * @return The {@link DimensionalConfigurationSchema} from which this {@link DimensionalSchema} was
   * constructed.
   */
  public DimensionalConfigurationSchema getDimensionalConfigurationSchema()
  {
    return configurationSchema;
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
   *
   * @return the predefinedFromTo
   */
  public boolean isPredefinedFromTo()
  {
    return predefinedFromTo;
  }

  /**
   * Returns the schema ID for this schema. This is only relevant for operators which
   * host multiple schemas.
   * @return The schema ID for this schema.
   */
  @Override
  public int getSchemaID()
  {
    return schemaID;
  }

  /**
   * Returns the current enum vals for the schema. The current enum vals for the
   * schema are expressed in a map whose keys are the names of the keys in the schema, and whose
   * values are a list of possible values for the key.
   * @return the currentEnumVals The current enum vals for the schema.
   */
  public Map<String, List<Object>> getCurrentEnumVals()
  {
    return currentEnumVals;
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionalSchema.class);
}
