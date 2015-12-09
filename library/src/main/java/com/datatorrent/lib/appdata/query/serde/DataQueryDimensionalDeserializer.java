/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.query.serde;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability.Unstable;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.lib.appdata.schemas.CustomTimeBucket;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.DimensionalSchema;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsAggregatable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Message;
import com.datatorrent.lib.appdata.schemas.QRBase;
import com.datatorrent.lib.appdata.schemas.Query;
import com.datatorrent.lib.appdata.schemas.SchemaRegistry;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;

/**
 * This class is a deserializer for {@link DataQueryDimensional} objects.
 *
 * @since 3.1.0
 */
public class DataQueryDimensionalDeserializer implements CustomMessageDeserializer
{
  /**
   * Constructor used to instantiate deserializer in {@link MessageDeserializerFactory}.
   */
  public DataQueryDimensionalDeserializer()
  {
    //Do nothing
  }

  @Override
  public Message deserialize(String json, Class<? extends Message> clazz, Object context) throws IOException
  {
    try {
      return deserializeHelper(json, context);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * This is a helper message to deserialize queries.
   *
   * @param json    The json to deserialize.
   * @param context The context to use when deserializing the json.
   * @return The deserialized query.
   * @throws Exception
   */
  private Message deserializeHelper(String json, Object context) throws Exception
  {
    JSONObject jo;

    jo = new JSONObject(json);

    //// Message
    String id = jo.getString(QRBase.FIELD_ID);
    String type = jo.getString(Message.FIELD_TYPE);

    boolean oneTime = !jo.has(QRBase.FIELD_COUNTDOWN);
    long countdown = 1;

    if (!oneTime) {
      countdown = jo.getLong(QRBase.FIELD_COUNTDOWN);
    }

    boolean incompleteResultOK = true;

    if (jo.has(DataQueryDimensional.FIELD_INCOMPLETE_RESULT_OK)) {
      incompleteResultOK = jo.getBoolean(DataQueryDimensional.FIELD_INCOMPLETE_RESULT_OK);
    }

    //// Message
    JSONObject data = jo.getJSONObject(DataQueryDimensional.FIELD_DATA);

    ////Schema keys
    Map<String, String> schemaKeys = null;

    if (data.has(Query.FIELD_SCHEMA_KEYS)) {
      schemaKeys = SchemaUtils.extractMap(data.getJSONObject(Query.FIELD_SCHEMA_KEYS));
    }

    SchemaRegistry schemaRegistry = ((SchemaRegistry)context);
    DimensionalSchema gsd = (DimensionalSchema)schemaRegistry.getSchema(schemaKeys);

    boolean hasFromTo = false;
    int latestNumBuckets = -1;
    long from = 0;
    long to = 0;
    CustomTimeBucket bucket = null;

    boolean hasTime = data.has(DataQueryDimensional.FIELD_TIME);

    int slidingAggregateSize = 1;

    if (hasTime) {
      //// Time
      JSONObject time = data.getJSONObject(DataQueryDimensional.FIELD_TIME);

      if (time.has(DataQueryDimensional.FIELD_SLIDING_AGGREGATE_SIZE)) {
        slidingAggregateSize = time.getInt(DataQueryDimensional.FIELD_SLIDING_AGGREGATE_SIZE);
      }

      if (time.has(DataQueryDimensional.FIELD_FROM)
          ^ time.has(DataQueryDimensional.FIELD_TO)) {
        LOG.error("Both from and to must be specified, or netiher");
        return null;
      }

      hasFromTo = time.has(DataQueryDimensional.FIELD_FROM);

      if (hasFromTo) {
        from = time.getLong(DataQueryDimensional.FIELD_FROM);
        to = time.getLong(DataQueryDimensional.FIELD_TO);
      } else {
        latestNumBuckets = time.getInt(DataQueryDimensional.FIELD_LATEST_NUM_BUCKETS);
      }

      if (time.has(DataQueryDimensional.FIELD_BUCKET)) {
        String timeBucketString = time.getString(DataQueryDimensional.FIELD_BUCKET);
        bucket = new CustomTimeBucket(timeBucketString);
      } else {
        bucket = gsd.getDimensionalConfigurationSchema().getCustomTimeBuckets().get(0);
      }
    } else {
      bucket = new CustomTimeBucket(TimeBucket.ALL);
    }

    //// Keys
    JSONObject keys = data.getJSONObject(DataQueryDimensional.FIELD_KEYS);

    @SuppressWarnings("unchecked")
    Iterator<String> keyIterator = (Iterator<String>)keys.keys();
    Set<String> keySet = Sets.newHashSet();

    while (keyIterator.hasNext()) {
      String key = keyIterator.next();
      if (!keySet.add(key)) {
        LOG.error("Duplicate key: {}", key);
        return null;
      }
    }

    DimensionsDescriptor dimensionDescriptor = new DimensionsDescriptor(bucket,
        new Fields(keySet));
    Integer ddID = gsd.getDimensionalConfigurationSchema().getDimensionsDescriptorToID().get(dimensionDescriptor);

    if (ddID == null) {
      LOG.error("The given dimensionDescriptor is not valid: {}", dimensionDescriptor);
      return null;
    }

    Map<String, Set<String>> valueToAggregator =
        gsd.getDimensionalConfigurationSchema().getDimensionsDescriptorIDToValueToAggregator().get(ddID);

    ////Fields

    Set<String> nonAggregatedFields = Sets.newHashSet();
    Map<String, Set<String>> fieldToAggregator;

    if (data.has(DataQueryDimensional.FIELD_FIELDS)) {
      fieldToAggregator = Maps.newHashMap();

      JSONArray fields = data.getJSONArray(DataQueryDimensional.FIELD_FIELDS);

      for (int fieldIndex = 0;
          fieldIndex < fields.length();
          fieldIndex++) {
        String field = fields.getString(fieldIndex);

        if (DimensionsDescriptor.TIME_FIELDS.getFields().contains(field)) {
          nonAggregatedFields.add(field);
        }

        String[] components = field.split(DimensionalConfigurationSchema.ADDITIONAL_VALUE_SEPERATOR);

        if (components.length == 1) {
          Set<String> aggregators = valueToAggregator.get(field);

          if (aggregators == null) {
            nonAggregatedFields.add(field);
          } else {
            fieldToAggregator.put(field, aggregators);
          }
        } else if (components.length == 2) {
          String value = components[DimensionalConfigurationSchema.ADDITIONAL_VALUE_VALUE_INDEX];
          String aggregator = components[DimensionalConfigurationSchema.ADDITIONAL_VALUE_AGGREGATOR_INDEX];

          if (!gsd.getDimensionalConfigurationSchema().getAggregatorRegistry().isAggregator(aggregator)) {
            LOG.error("{} is not a valid aggregator", aggregator);
            return null;
          }

          Set<String> aggregators = fieldToAggregator.get(value);

          if (aggregators == null) {
            aggregators = Sets.newHashSet();
            fieldToAggregator.put(value, aggregators);
          }

          aggregators.add(aggregator);
        } else {
          LOG.error("A field selector can have at most one {}.",
              DimensionalConfigurationSchema.ADDITIONAL_VALUE_SEPERATOR);
        }
      }
    } else {
      if (hasTime) {
        nonAggregatedFields.add(DimensionsDescriptor.DIMENSION_TIME);
      }

      fieldToAggregator = valueToAggregator;
    }

    FieldsAggregatable queryFields = new FieldsAggregatable(nonAggregatedFields,
        fieldToAggregator);
    FieldsDescriptor keyFieldsDescriptor = gsd.getDimensionalConfigurationSchema().getKeyDescriptor().getSubset(
        new Fields(keySet));
    Map<String, Set<Object>> map = deserializeToMap(keyFieldsDescriptor, keys);
    DataQueryDimensional resultQuery;

    if (!hasTime) {
      resultQuery = new DataQueryDimensional(id,
          type,
          keyFieldsDescriptor,
          map,
          queryFields,
          incompleteResultOK,
          schemaKeys);
    } else {
      if (oneTime) {
        if (hasFromTo) {
          resultQuery = new DataQueryDimensional(id,
              type,
              from,
              to,
              bucket,
              keyFieldsDescriptor,
              map,
              queryFields,
              incompleteResultOK,
              schemaKeys);
        } else {
          resultQuery = new DataQueryDimensional(id,
              type,
              latestNumBuckets,
              bucket,
              keyFieldsDescriptor,
              map,
              queryFields,
              incompleteResultOK,
              schemaKeys);
        }
      } else {
        if (hasFromTo) {
          resultQuery = new DataQueryDimensional(id,
              type,
              from,
              to,
              bucket,
              keyFieldsDescriptor,
              map,
              queryFields,
              countdown,
              incompleteResultOK,
              schemaKeys);
        } else {
          resultQuery = new DataQueryDimensional(id,
              type,
              latestNumBuckets,
              bucket,
              keyFieldsDescriptor,
              map,
              queryFields,
              countdown,
              incompleteResultOK,
              schemaKeys);
        }
      }
    }

    resultQuery.setSlidingAggregateSize(slidingAggregateSize);

    return resultQuery;
  }

  //TODO this is duplicate code remove once malhar dependency is upgraded to 3.3
  @Unstable
  private static Map<String, Set<Object>> deserializeToMap(FieldsDescriptor fieldsDescriptor,
      JSONObject dpou)
  {
    Map<String, Set<Object>> keyToValues = Maps.newHashMap();

    for (String key : fieldsDescriptor.getFields().getFields()) {
      if (!dpou.has(key)) {
        throw new IllegalArgumentException("The given key " + key + " is not contained in the given JSON");
      }

      Set<Object> keyValues;
      Object keyValue;

      try {
        keyValue = dpou.get(key);
      } catch (JSONException ex) {
        throw new IllegalStateException("This should never happen", ex);
      }

      if (keyValue instanceof JSONArray) {

        JSONArray ja = (JSONArray)keyValue;
        keyValues = Sets.newHashSetWithExpectedSize(ja.length());

        Type type = fieldsDescriptor.getType(key);

        for (int index = 0; index < ja.length(); index++) {
          keyValues.add(getFieldFromJSON(type, ja, index));
        }

      } else if (keyValue instanceof JSONObject) {
        throw new UnsupportedOperationException("Cannot extract objects from JSONObjects");
      } else {
        keyValues = Sets.newHashSetWithExpectedSize(1);
        keyValues.add(getFieldFromJSON(fieldsDescriptor, key, dpou));
      }

      keyToValues.put(key, keyValues);
    }

    return keyToValues;
  }

  //TODO this is duplicate code remove once malhar dependency is upgraded to 3.3
  @Unstable
  private static Object getFieldFromJSON(Type type, JSONArray ja, int index)
  {
    int intVal = 0;

    if (numericTypeIntOrSmaller(type)) {
      try {
        intVal = ja.getInt(index);
      } catch (JSONException ex) {
        throw new IllegalArgumentException("The index "
            + index
            + " does not have a valid "
            + type
            + " value.", ex);
      }

      if (type != Type.INTEGER && !insideRange(type, intVal)) {
        throw new IllegalArgumentException("The index "
            + index
            + " has a value "
            + intVal
            + " which is out of range for a "
            + type
            + ".");
      }
    }

    if (type == Type.BOOLEAN) {
      try {
        return ja.getBoolean(index);
      } catch (JSONException ex) {
        throw new IllegalArgumentException("The index " + index + " does not have a valid bool value.", ex);
      }
    } else if (type == Type.BYTE) {
      return ((byte)intVal);
    } else if (type == Type.SHORT) {
      return ((short)intVal);
    } else if (type == Type.INTEGER) {
      return intVal;
    } else if (type == Type.LONG) {
      try {
        return ja.getLong(index);
      } catch (JSONException ex) {
        throw new IllegalArgumentException("The index "
            + index
            + " does not have a valid long value.",
            ex);
      }
    } else if (type == Type.CHAR) {
      String val;

      try {
        val = ja.getString(index);
      } catch (JSONException ex) {
        throw new IllegalArgumentException("The index "
            + index
            + " does not have a valid character value.",
            ex);
      }

      if (val.length() != 1) {
        throw new IllegalArgumentException("The index "
            + index
            + " has a value "
            + val
            + " that is not one character long.");
      }

      return val.charAt(0);
    } else if (type == Type.STRING) {
      try {
        return ja.getString(index);
      } catch (JSONException ex) {
        throw new IllegalArgumentException("The index "
            + index
            + " does not have a valid string value.",
            ex);
      }
    } else if (type == Type.DOUBLE) {
      try {
        return ja.getDouble(index);
      } catch (JSONException ex) {
        throw new IllegalArgumentException("The index "
            + index
            + " does not have a valid double value.",
            ex);
      }
    } else if (type == Type.FLOAT) {
      try {
        return (float)ja.getDouble(index);
      } catch (JSONException ex) {
        throw new IllegalArgumentException("The index "
            + index
            + " does not have a valid double value.",
            ex);
      }
    } else {
      throw new UnsupportedOperationException("The type " + type + " is not supported.");
    }
  }

  //TODO this is duplicate code remove once malhar dependency is upgraded to 3.3
  @Unstable
  private static Object getFieldFromJSON(FieldsDescriptor fd, String field, JSONObject jo)
  {
    Type type = fd.getType(field);
    int intVal = 0;

    if (numericTypeIntOrSmaller(type)) {
      try {
        intVal = jo.getInt(field);
      } catch (JSONException ex) {
        throw new IllegalArgumentException("The key "
            + field
            + " does not have a valid "
            + type
            + " value.", ex);
      }

      if (type != Type.INTEGER && !insideRange(type, intVal)) {
        throw new IllegalArgumentException("The key "
            + field
            + " has a value "
            + intVal
            + " which is out of range for a "
            + type
            + ".");
      }
    }

    if (type == Type.BOOLEAN) {
      try {
        return jo.getBoolean(field);
      } catch (JSONException ex) {
        throw new IllegalArgumentException("The key " + field + " does not have a valid bool value.", ex);
      }
    } else if (type == Type.BYTE) {
      return ((byte)intVal);
    } else if (type == Type.SHORT) {
      return ((short)intVal);
    } else if (type == Type.INTEGER) {
      return intVal;
    } else if (type == Type.LONG) {
      try {
        return jo.getLong(field);
      } catch (JSONException ex) {
        throw new IllegalArgumentException("The key "
            + field
            + " does not have a valid long value.",
            ex);
      }
    } else if (type == Type.CHAR) {
      String val;

      try {
        val = jo.getString(field);
      } catch (JSONException ex) {
        throw new IllegalArgumentException("The key "
            + field
            + " does not have a valid character value.",
            ex);
      }

      if (val.length() != 1) {
        throw new IllegalArgumentException("The key "
            + field
            + " has a value "
            + val
            + " that is not one character long.");
      }

      return val.charAt(0);
    } else if (type == Type.STRING) {
      try {
        return jo.getString(field);
      } catch (JSONException ex) {
        throw new IllegalArgumentException("The key "
            + field
            + " does not have a valid string value.",
            ex);
      }
    } else if (type == Type.DOUBLE) {
      try {
        return jo.getDouble(field);
      } catch (JSONException ex) {
        throw new IllegalArgumentException("The key "
            + field
            + " does not have a valid double value.",
            ex);
      }
    } else if (type == Type.FLOAT) {
      try {
        return (float)jo.getDouble(field);
      } catch (JSONException ex) {
        throw new IllegalArgumentException("The key "
            + field
            + " does not have a valid double value.",
            ex);
      }
    } else {
      throw new UnsupportedOperationException("The type " + type + " is not supported.");
    }
  }

  //TODO this is duplicate code remove once malhar dependency is upgraded to 3.3
  @Unstable
  private static boolean insideRange(Type type, int val)
  {
    switch (type) {
      case BYTE: {
        return !(val < (int)Byte.MIN_VALUE || val > (int)Byte.MAX_VALUE);
      }
      case SHORT: {
        return !(val < (int)Short.MIN_VALUE || val > (int)Short.MAX_VALUE);
      }
      default:
        throw new UnsupportedOperationException("This operation is not supported for the type " + type);
    }
  }

  //TODO this is duplicate code remove once malhar dependency is upgraded to 3.3
  @Unstable
  private static boolean numericTypeIntOrSmaller(Type type)
  {
    return type == Type.BYTE || type == Type.SHORT || type == Type.INTEGER;
  }

  private static final Logger LOG = LoggerFactory.getLogger(DataQueryDimensionalDeserializer.class);
}
