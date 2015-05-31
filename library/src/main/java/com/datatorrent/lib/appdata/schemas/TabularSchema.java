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

import com.datatorrent.common.util.DTThrowable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Collections;
import java.util.Map;

/**
 * This class represents an AppData point schema. This class allows you to specify your
 * point schema in json and to load it. An example of a point schema specification is the following:
 *
 * {@code
 * }
 */
public class TabularSchema implements Schema
{
  public static final String SCHEMA_TYPE = "point";
  public static final String SCHEMA_VERSION = "1.0";

  public static final String FIELD_SCHEMA_TYPE = "schemaType";
  public static final String FIELD_SCHEMA_VERSION = "schemaVersion";

  public static final String FIELD_VALUES = "values";
  public static final String FIELD_VALUES_NAME = "name";
  public static final String FIELD_VALUES_TYPE = "type";

  public static final int NUM_KEYS_FIRST_LEVEL = 1;
  public static final int NUM_KEYS_VALUES = 2;

  private String schemaJSON;
  private String schemaType;

  private Map<String, Type> valueToType;
  private FieldsDescriptor valuesDescriptor;

  private Map<String, String> schemaKeys;
  private int schemaID = Schema.DEFAULT_SCHEMA_ID;

  private JSONObject schema;
  private boolean changed = false;

  public TabularSchema(String schemaJSON,
                       Map<String, String> schemaKeys)
  {
    this(schemaJSON,
         true,
         schemaKeys);
  }


  public TabularSchema(int schemaID,
                       String schemaJSON,
                       Map<String, String> schemaKeys)
  {
    this(schemaJSON,
         schemaKeys);
  }

  public TabularSchema(String schemaJSON)
  {
    this(schemaJSON,
         true,
         null);
  }

  public TabularSchema(int schemaID,
                       String schemaJSON)
  {
    this(schemaJSON);

    this.schemaID = schemaID;
  }

  //This would be needed for more rigorous validation of schemas
  public TabularSchema(String schemaJSON,
                       boolean validate,
                       Map<String, String> schemaKeys)
  {
    setSchema(schemaJSON);
    setSchemaKeys(schemaKeys);

    try {
      initialize(validate);
    }
    catch(Exception ex) {
      DTThrowable.rethrow(ex);
    }
  }

  public TabularSchema(int schemaID,
                       String schemaJSON,
                       boolean validate,
                       Map<String, String> schemaKeys)
  {
    this(schemaJSON,
         validate,
         schemaKeys);

    this.schemaID = schemaID;
  }

  public TabularSchema(String schemaJSON,
                       boolean validate)
  {
    this(schemaJSON,
         validate,
         null);
  }

  public TabularSchema(int schemaID,
                       String schemaJSON,
                       boolean validate)
  {
    this(schemaJSON,
         validate);

    this.schemaID = schemaID;
  }

  @Override
  public final void setSchemaKeys(Map<String, String> schemaKeys)
  {
    changed = true;

    if(schemaKeys == null) {
      schemaKeys = null;
      return;
    }

    for(Map.Entry<String, String> entry: schemaKeys.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.schemaKeys = Collections.unmodifiableMap(Maps.newHashMap(schemaKeys));
  }

  private void initialize(boolean validate) throws Exception
  {
    schema = new JSONObject(schemaJSON);

    if(validate) {
      Preconditions.checkState(schema.length() == NUM_KEYS_FIRST_LEVEL,
                               "Expected "
                               + NUM_KEYS_FIRST_LEVEL
                               + " keys in the first level but found "
                               + schema.length());
    }

    if(schemaKeys != null) {
      schema.put(Schema.FIELD_SCHEMA_KEYS,
                 SchemaUtils.createJSONObject(schemaKeys));
    }

    valueToType = Maps.newHashMap();

    JSONArray values = schema.getJSONArray(FIELD_VALUES);

    Preconditions.checkState(values.length() > 0,
                             "The schema does not specify any values.");

    for(int index = 0;
        index < values.length();
        index++)
    {
      JSONObject value = values.getJSONObject(index);
      String name = value.getString(FIELD_VALUES_NAME);
      String typeName = value.getString(FIELD_VALUES_TYPE);

      Type type = Type.NAME_TO_TYPE.get(typeName);
      valueToType.put(name, type);

      Preconditions.checkArgument(type != null,
                                  typeName
                                  + " is not a valid type.");
    }

    valueToType = Collections.unmodifiableMap(valueToType);
    valuesDescriptor = new FieldsDescriptor(valueToType);

    try {
      schema.put(FIELD_SCHEMA_TYPE, SCHEMA_TYPE);
      schema.put(FIELD_SCHEMA_VERSION, SCHEMA_VERSION);
    }
    catch(JSONException e) {
      throw new RuntimeException(e);
    }

    schemaJSON = this.schema.toString();
  }

  protected final void setSchema(String schemaJSON)
  {
    this.schemaJSON = Preconditions.checkNotNull(schemaJSON);
  }

  @Override
  public String getSchemaJSON()
  {
    if(!changed && schemaJSON != null) {
      return schemaJSON;
    }

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

    schemaJSON = schema.toString();
    return schemaJSON;
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

  public Map<String, Type> getFieldToType()
  {
    return valueToType;
  }

  /**
   * @return the valuesDescriptor
   */
  public FieldsDescriptor getValuesDescriptor()
  {
    return valuesDescriptor;
  }

  @Override
  public Map<String, String> getSchemaKeys()
  {
    return schemaKeys;
  }

  /**
   * @return the schemaID
   */
  @Override
  public int getSchemaID()
  {
    return schemaID;
  }
}
