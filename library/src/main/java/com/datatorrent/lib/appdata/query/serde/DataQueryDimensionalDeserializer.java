/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.query.serde;

import java.io.IOException;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
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
import com.datatorrent.lib.dimensions.DimensionsDescriptor;

/**
 * This class is a deserializer for {@link DataQueryDimensional} objects.
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
    }
    catch(Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * This is a helper message to deserialize queries.
   * @param json The json to deserialize.
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

    if(!oneTime) {
      countdown = jo.getLong(QRBase.FIELD_COUNTDOWN);
    }


    boolean incompleteResultOK = true;

    if(jo.has(DataQueryDimensional.FIELD_INCOMPLETE_RESULT_OK)) {
      incompleteResultOK = jo.getBoolean(DataQueryDimensional.FIELD_INCOMPLETE_RESULT_OK);
    }

    //// Message
    JSONObject data = jo.getJSONObject(DataQueryDimensional.FIELD_DATA);

    ////Schema keys
    Map<String, String> schemaKeys = null;

    if(data.has(Query.FIELD_SCHEMA_KEYS)) {
      schemaKeys = SchemaUtils.extractMap(data.getJSONObject(Query.FIELD_SCHEMA_KEYS));
    }

    SchemaRegistry schemaRegistry = ((SchemaRegistry) context);
    DimensionalSchema gsd = (DimensionalSchema) schemaRegistry.getSchema(schemaKeys);

    boolean hasFromTo = false;
    int latestNumBuckets = -1;
    long from = 0;
    long to = 0;
    TimeBucket bucket = null;

    boolean hasTime = data.has(DataQueryDimensional.FIELD_TIME);

    int slidingAggregateSize = 1;

    if(hasTime) {
      //// Time
      JSONObject time = data.getJSONObject(DataQueryDimensional.FIELD_TIME);

      if (time.has(DataQueryDimensional.FIELD_SLIDING_AGGREGATE_SIZE)) {
        slidingAggregateSize = time.getInt(DataQueryDimensional.FIELD_SLIDING_AGGREGATE_SIZE);
      }

      if(time.has(DataQueryDimensional.FIELD_FROM)
         ^ time.has(DataQueryDimensional.FIELD_TO)) {
        LOG.error("Both from and to must be specified, or netiher");
        return null;
      }

      hasFromTo = time.has(DataQueryDimensional.FIELD_FROM);

      if(hasFromTo) {
        from = time.getLong(DataQueryDimensional.FIELD_FROM);
        to = time.getLong(DataQueryDimensional.FIELD_TO);
      }
      else {
        latestNumBuckets = time.getInt(DataQueryDimensional.FIELD_LATEST_NUM_BUCKETS);
      }

      if(time.has(DataQueryDimensional.FIELD_BUCKET)) {
        String timeBucketString = time.getString(DataQueryDimensional.FIELD_BUCKET);
        bucket = TimeBucket.BUCKET_TO_TYPE.get(timeBucketString);

        if(bucket == null) {
          LOG.error("{} is not a valid time bucket", timeBucketString);
          bucket = gsd.getDimensionalConfigurationSchema().getTimeBuckets().get(0);
        }
      }
      else {
        bucket = gsd.getDimensionalConfigurationSchema().getTimeBuckets().get(0);
      }
    }

    //// Keys
    JSONObject keys = data.getJSONObject(DataQueryDimensional.FIELD_KEYS);

    @SuppressWarnings("unchecked")
    Iterator<String> keyIterator = (Iterator<String>) keys.keys();
    Set<String> keySet = Sets.newHashSet();

    while(keyIterator.hasNext()) {
      String key = keyIterator.next();
      if(!keySet.add(key)) {
        LOG.error("Duplicate key: {}", key);
        return null;
      }
    }

    DimensionsDescriptor dimensionDescriptor = new DimensionsDescriptor(bucket,
                                                                        new Fields(keySet));
    Integer ddID = gsd.getDimensionalConfigurationSchema().getDimensionsDescriptorToID().get(dimensionDescriptor);

    if(ddID == null) {
      LOG.error("The given dimensionDescriptor is not valid: {}", dimensionDescriptor);
      return null;
    }

    Map<String, Set<String>> valueToAggregator = gsd.getDimensionalConfigurationSchema().getDimensionsDescriptorIDToValueToAggregator().get(ddID);

    ////Fields

    Set<String> nonAggregatedFields = Sets.newHashSet();
    Map<String, Set<String>> fieldToAggregator;

    if(data.has(DataQueryDimensional.FIELD_FIELDS)) {
      fieldToAggregator = Maps.newHashMap();

      JSONArray fields = data.getJSONArray(DataQueryDimensional.FIELD_FIELDS);

      for(int fieldIndex = 0;
          fieldIndex < fields.length();
          fieldIndex++) {
        String field = fields.getString(fieldIndex);

        if(DimensionsDescriptor.TIME_FIELDS.getFields().contains(field)) {
          nonAggregatedFields.add(field);
        }

        String[] components = field.split(DimensionalConfigurationSchema.ADDITIONAL_VALUE_SEPERATOR);

        if(components.length == 1) {
          Set<String> aggregators = valueToAggregator.get(field);

          if(aggregators == null) {
            nonAggregatedFields.add(field);
          }
          else {
            fieldToAggregator.put(field, aggregators);
          }
        }
        else if(components.length == 2) {
          String value = components[DimensionalConfigurationSchema.ADDITIONAL_VALUE_VALUE_INDEX];
          String aggregator = components[DimensionalConfigurationSchema.ADDITIONAL_VALUE_AGGREGATOR_INDEX];

          if(!gsd.getDimensionalConfigurationSchema().getAggregatorRegistry().isAggregator(aggregator)) {
            LOG.error("{} is not a valid aggregator", aggregator);
            return null;
          }

          Set<String> aggregators = fieldToAggregator.get(value);

          if(aggregators == null) {
            aggregators = Sets.newHashSet();
            fieldToAggregator.put(value, aggregators);
          }

          aggregators.add(aggregator);
        }
        else {
          LOG.error("A field selector can have at most one {}.", DimensionalConfigurationSchema.ADDITIONAL_VALUE_SEPERATOR);
        }
      }
    }
    else {
      if(hasTime) {
        nonAggregatedFields.add(DimensionsDescriptor.DIMENSION_TIME);
      }

      fieldToAggregator = valueToAggregator;
    }

    FieldsAggregatable queryFields = new FieldsAggregatable(nonAggregatedFields,
                                                            fieldToAggregator);
    FieldsDescriptor keyFieldsDescriptor = gsd.getDimensionalConfigurationSchema().getKeyDescriptor().getSubset(new Fields(keySet));
    GPOMutable gpoIm = new GPOMutable(GPOUtils.deserialize(keyFieldsDescriptor, keys));
    DataQueryDimensional resultQuery;

    if (!hasTime) {
      resultQuery = new DataQueryDimensional(id,
                                             type,
                                             gpoIm,
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
                                                 gpoIm,
                                                 queryFields,
                                                 incompleteResultOK,
                                                 schemaKeys);
        } else {
          resultQuery = new DataQueryDimensional(id,
                                                 type,
                                                 latestNumBuckets,
                                                 bucket,
                                                 gpoIm,
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
                                                 gpoIm,
                                                 queryFields,
                                                 countdown,
                                                 incompleteResultOK,
                                                 schemaKeys);
        } else {
          resultQuery = new DataQueryDimensional(id,
                                                 type,
                                                 latestNumBuckets,
                                                 bucket,
                                                 gpoIm,
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

  private static final Logger LOG = LoggerFactory.getLogger(DataQueryDimensionalDeserializer.class);
}
