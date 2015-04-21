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
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.qr.CustomDataDeserializer;
import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.qr.Query;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class DataQueryDimensionalDeserializer extends CustomDataDeserializer
{
  private static final Logger logger = LoggerFactory.getLogger(DataQueryDimensionalDeserializer.class);

  public DataQueryDimensionalDeserializer()
  {
  }

  @Override
  public Query deserialize(String json, Object context)
  {
    try {
      return deserializeHelper(json, context);
    }
    catch(Exception e) {
      logger.error("Exception while deserializing.", e);
      return null;
    }
  }

  private Query deserializeHelper(String json, Object context) throws Exception
  {
    SchemaDimensional gsd = (SchemaDimensional)context;
    JSONObject jo;

    jo = new JSONObject(json);

      //// Message
    String id = jo.getString(Query.FIELD_ID);
    String type = jo.getString(Data.FIELD_TYPE);

    boolean oneTime = !jo.has(DataQueryTabular.FIELD_COUNTDOWN);
    long countdown = 1;

    if(!oneTime) {
      countdown = jo.getLong(DataQueryDimensional.FIELD_COUNTDOWN);
    }

    boolean incompleteResultOK = jo.getBoolean(DataQueryDimensional.FIELD_INCOMPLETE_RESULT_OK);

      //// Data
    JSONObject data = jo.getJSONObject(DataQueryDimensional.FIELD_DATA);

    boolean hasFromTo = false;
    int latestNumBuckets = -1;
    String from = null;
    String to = null;
    TimeBucket bucket = null;

    boolean hasTime = data.has(DataQueryDimensional.FIELD_TIME);

    if(hasTime) {
      //// Time
      JSONObject time = data.getJSONObject(DataQueryDimensional.FIELD_TIME);

      if(time.has(DataQueryDimensional.FIELD_FROM)
         ^ time.has(DataQueryDimensional.FIELD_TO)) {
        logger.error("Both from and to must be specified, or netiher");
        return null;
      }

      hasFromTo = time.has(DataQueryDimensional.FIELD_FROM);

      if(hasFromTo) {
        from = time.getString(DataQueryDimensional.FIELD_FROM);
        to = time.getString(DataQueryDimensional.FIELD_TO);
      }
      else {
        latestNumBuckets = time.getInt(DataQueryDimensional.FIELD_LATEST_NUM_BUCKETS);
      }

      bucket = TimeBucket.BUCKET_TO_TYPE.get(time.getString(DataQueryDimensional.FIELD_BUCKET));
    }

    //// Keys
    JSONObject keys = data.getJSONObject(DataQueryDimensional.FIELD_KEYS);

    Iterator keyIterator = keys.keys();
    Set<String> keySet = Sets.newHashSet();

    while(keyIterator.hasNext()) {
      String key = (String)keyIterator.next();
      if(!keySet.add(key)) {
        logger.error("Duplicate key: {}", key);
        return null;
      }
    }

    DimensionsDescriptor dimensionDescriptor = new DimensionsDescriptor(bucket,
                                                                        new Fields(keySet));
    logger.info("GSD {}", gsd);
    logger.info("Dimension Descriptor to ID {}", gsd.getGenericEventSchema().getDimensionsDescriptorToID());
    Integer ddID = gsd.getGenericEventSchema().getDimensionsDescriptorToID().get(dimensionDescriptor);

    if(ddID == null) {
      logger.error("The given dimensionDescriptor is not valid: {}", dimensionDescriptor);
      return null;
    }

    Map<String, Set<String>> valueToAggregator = gsd.getGenericEventSchema().getDdIDToValueToAggregator().get(ddID);

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

        String[] components = field.split(DimensionalEventSchema.ADDITIONAL_VALUE_SEPERATOR);

        if(components.length == 1) {
          Set<String> aggregators = valueToAggregator.get(field);

          if(aggregators == null) {
            logger.error("The provided field {} is not a valid field.", field);
            nonAggregatedFields.add(field);
          }
          else {
            fieldToAggregator.put(field, aggregators);
          }
        }
        else if(components.length == 2) {
          String value = components[DimensionalEventSchema.ADDITIONAL_VALUE_VALUE_INDEX];
          String aggregator = components[DimensionalEventSchema.ADDITIONAL_VALUE_AGGREGATOR_INDEX];

          if(!gsd.getGenericEventSchema().getAggregatorInfo().isAggregator(aggregator)) {
            logger.error("{} is not a valid aggregator", aggregator);
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
          logger.error("A field selector can have at most one {}.", DimensionalEventSchema.ADDITIONAL_VALUE_SEPERATOR);
        }
      }
    }
    else {
      logger.info("Has time: {}", hasTime);
      if(hasTime) {
        nonAggregatedFields.add(DimensionsDescriptor.DIMENSION_TIME);
      }

      fieldToAggregator = valueToAggregator;
    }

    FieldsAggregatable queryFields = new FieldsAggregatable(nonAggregatedFields,
                                                                   fieldToAggregator);
    FieldsDescriptor keyFieldsDescriptor = gsd.getGenericEventSchema().getAllKeysDescriptor().getSubset(new Fields(keySet));
    GPOMutable gpoIm = new GPOMutable(GPOUtils.deserialize(keyFieldsDescriptor, keys));

    if(!hasTime) {
      return new DataQueryDimensional(id,
                                  type,
                                  gpoIm,
                                  queryFields,
                                  incompleteResultOK);
    }
    else {
      if(oneTime) {
        if(hasFromTo) {
          return new DataQueryDimensional(id,
                                      type,
                                      from,
                                      to,
                                      bucket,
                                      gpoIm,
                                      queryFields,
                                      incompleteResultOK);
        }
        else {
          return new DataQueryDimensional(id,
                                      type,
                                      latestNumBuckets,
                                      bucket,
                                      gpoIm,
                                      queryFields,
                                      incompleteResultOK);
        }
      }
      else {
        if(hasFromTo) {
          return new DataQueryDimensional(id,
                                      type,
                                      from,
                                      to,
                                      bucket,
                                      gpoIm,
                                      queryFields,
                                      countdown,
                                      incompleteResultOK);
        }
        else {
          return new DataQueryDimensional(id,
                                      type,
                                      latestNumBuckets,
                                      bucket,
                                      gpoIm,
                                      queryFields,
                                      countdown,
                                      incompleteResultOK);
        }
      }
    }
  }
}
