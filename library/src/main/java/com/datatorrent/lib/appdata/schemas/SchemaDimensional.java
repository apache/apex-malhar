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


import com.datatorrent.lib.appdata.dimensions.DimensionsAggregator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public static final String SCHEMA_TYPE = "dimensional";
  public static final String SCHEMA_VERSION = "1.0";

  public static final List<Fields> VALID_KEYS = ImmutableList.of(new Fields(Sets.newHashSet(SchemaWithTime.FIELD_TIME)));
  public static final List<Fields> VALID_TIME_KEYS = ImmutableList.of(new Fields(Sets.newHashSet(SchemaWithTime.FIELD_TIME_FROM,
                                                                                                 SchemaWithTime.FIELD_TIME_TO)));

  private Long from;
  private Long to;

  private boolean changed = false;
  private String schemaJSON;

  private EventSchema eventSchema;
  private Map<String, DimensionsAggregator> nameToAggregator;
  private JSONObject schema;
  private JSONObject time;

  public SchemaDimensional(String schemaStub,
                                  EventSchema eventSchema,
                                  Map<String, DimensionsAggregator> nameToAggregator)
  {
    this(eventSchema,
         nameToAggregator);

    if(schemaStub != null) {
      try {
        setSchemaStub(schemaStub);
      }
      catch(Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public SchemaDimensional(String schemaStub,
                           String eventSchemaJSON,
                           Map<String, DimensionsAggregator> nameToAggregator)
  {
    this(schemaStub,
         new EventSchema(eventSchemaJSON),
         nameToAggregator);
  }

  public SchemaDimensional(String eventSchemaJSON,
                           Map<String, DimensionsAggregator> nameToAggregator)
  {
    this(null,
         eventSchemaJSON,
         nameToAggregator);
  }

  public SchemaDimensional(EventSchema eventSchema,
                                  Map<String, DimensionsAggregator> nameToAggregator)
  {
    setEventSchema(eventSchema);
    setNameToAggregator(nameToAggregator);

    try {
      initialize();
    }
    catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void setEventSchema(EventSchema eventSchema)
  {
    this.eventSchema = Preconditions.checkNotNull(eventSchema, "eventSchema");
  }

  private void setNameToAggregator(Map<String, DimensionsAggregator> nameToAggregator)
  {
    this.nameToAggregator = Preconditions.checkNotNull(nameToAggregator, "nameToAggregator");

    for(Map.Entry<String, DimensionsAggregator> entry: nameToAggregator.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }
  }

  private void setSchemaStub(String schemaStub) throws Exception
  {
    JSONObject jo = new JSONObject(schemaStub);
    SchemaUtils.checkValidKeysEx(jo, VALID_KEYS);

    JSONObject time = jo.getJSONObject(SchemaWithTime.FIELD_TIME);
    SchemaUtils.checkValidKeys(jo, VALID_TIME_KEYS);

    this.from = time.getLong(SchemaWithTime.FIELD_TIME_FROM);
    this.to = time.getLong(SchemaWithTime.FIELD_TIME_TO);
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
    JSONArray keys = new JSONArray(eventSchema.getKeysString());
    schema.put(EventSchema.FIELD_KEYS, keys);

    //values;
    JSONArray values = new JSONArray();
    schema.put(SchemaTabular.FIELD_VALUES, values);

    FieldsDescriptor inputValuesDescriptor = eventSchema.getInputValuesDescriptor();
    Map<String, Set<String>> allValueToAggregator = eventSchema.getAllValueToAggregator();

    for(Map.Entry<String, Set<String>> entry: allValueToAggregator.entrySet()) {
      String valueName = entry.getKey();
      Type inputValueType = inputValuesDescriptor.getType(valueName);

      for(String aggregatorName: entry.getValue()) {
        DimensionsAggregator aggregator = nameToAggregator.get(aggregatorName);
        Type outputValueType = aggregator.getTypeConversionMap().get(inputValueType);

        JSONObject value = new JSONObject();
        String combinedName = valueName +
                              EventSchema.ADDITIONAL_VALUE_SEPERATOR +
                              aggregatorName;
        value.put(SchemaTabular.FIELD_VALUES_NAME, combinedName);
        value.put(SchemaTabular.FIELD_VALUES_TYPE, outputValueType.getName());
        values.put(value);
      }
    }

    //dimensions
    JSONArray dimensions = new JSONArray(eventSchema.getDimensionsString());
    schema.put(EventSchema.FIELD_DIMENSIONS, dimensions);

    getSchemaJSON();
  }

  public void setFrom(Long from)
  {
    this.from = from;
    changed = true;
  }

  public void setTo(Long to)
  {
    this.to = to;
    changed = true;
  }

  public String getSchemaJSON()
  {
    if(!changed) {
      return schemaJSON;
    }

    changed = false;
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

    schemaJSON = schema.toString();
    return schemaJSON;
  }

  public EventSchema getGenericEventSchema()
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
}
