/*
 * Copyright (c) 2015 DataTorrent, Inc.
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

import java.util.Collections;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * This class represents an AppData snapshot schema. This class allows you to specify your
 * snapshot schema in json and to load it. An example of a snapshot schema specification is the following:
 * <br/>
 * <br/>
 * {@code
 *  {
 *    "values": [{"name": "hashtag", "type": "string"},
 *               {"name": "count", "type": "integer"}]
 *  }
 * }
 * <br/>
 * <br/>
 * As can be seen above, the snapshot schema defines the name of each field which is served as well as
 * its type.
 */
public class SnapshotSchema implements Schema
{
  /**
   * The type of the schema.
   */
  public static final String SCHEMA_TYPE = "snapshot";
  /**
   * The version of the schema.
   */
  public static final String SCHEMA_VERSION = "1.0";
  /**
   * The type of the schema.
   */
  public static final String FIELD_SCHEMA_TYPE = "schemaType";
  /**
   * The version of the schema.
   */
  public static final String FIELD_SCHEMA_VERSION = "schemaVersion";
  /**
   * The JSON key string for the values section of the schema.
   */
  public static final String FIELD_VALUES = "values";
  /**
   * The JSON key string for field names.
   */
  public static final String FIELD_VALUES_NAME = "name";
  /**
   * The JSON key string for field types.
   */
  public static final String FIELD_VALUES_TYPE = "type";

  public static final int NUM_KEYS_FIRST_LEVEL = 1;
  public static final int NUM_KEYS_VALUES = 2;

  /**
   * The JSON for the schema.
   */
  private String schemaJSON;
  /**
   * A map from the field name to its type.
   */
  private Map<String, Type> valueToType;
  /**
   * The fieds descriptor object for the values.
   */
  private FieldsDescriptor valuesDescriptor;
  /**
   * The schema keys for this schema. In the case where this is the only schema served by an operator,
   * these could be null.
   */
  private Map<String, String> schemaKeys;
  /**
   * The schemaID assigned to this schema. In the case where this is the only schema being served by an operator,
   * then this is not important.
   */
  private int schemaID = Schema.DEFAULT_SCHEMA_ID;
  /**
   * The JSONObject representing the schema.
   */
  private JSONObject schema;
  /**
   * Flag indicating whether any items in the schema have been changed.
   */
  private boolean changed = false;

  /**
   * This creates a snapshot schema from the specified json with the specified schema keys.
   * @param schemaJSON The JSON defining this schema.
   * @param schemaKeys The schema keys tied to this schema.
   */
  public SnapshotSchema(String schemaJSON,
                       Map<String, String> schemaKeys)
  {
    this(schemaJSON);

    setSchemaKeys(schemaKeys);
  }

  /**
   * This creates a snapshot schema from the specified json with the specified schema keys, and
   * schemaID.
   * @param schemaID The ID associated with this schema.
   * @param schemaJSON The schemaJSON this schema is built from.
   * @param schemaKeys The schemaKeys associated with this schema.
   */
  public SnapshotSchema(int schemaID,
                       String schemaJSON,
                       Map<String, String> schemaKeys)
  {
    this(schemaJSON,
         schemaKeys);

    this.schemaID = schemaID;
  }

  /**
   * This creates a snapshot schema from the specified json.
   * @param schemaJSON The JSON specifying the snapshot schema.
   */
  public SnapshotSchema(String schemaJSON)
  {
    setSchema(schemaJSON);

    try {
      initialize();
    }
    catch(Exception ex) {
      DTThrowable.rethrow(ex);
    }
  }

  /**
   * This creates a snapshot schema from the specified json, and associates the given
   * schemaID with this schema.
   * @param schemaID The schemaID associated with this schema.
   * @param schemaJSON The JSON that this schema is constructed from.
   */
  public SnapshotSchema(int schemaID,
                       String schemaJSON)
  {
    this(schemaJSON);
    this.schemaID = schemaID;
  }

  @Override
  public final void setSchemaKeys(Map<String, String> schemaKeys)
  {
    changed = true;

    if(schemaKeys == null) {
      this.schemaKeys = null;
      return;
    }

    for(Map.Entry<String, String> entry: schemaKeys.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.schemaKeys = Collections.unmodifiableMap(Maps.newHashMap(schemaKeys));
  }

  /**
   * This is a helper method to initialize the schema.
   * @throws JSONException This exception is thrown if there is an error parsing the JSON
   * which specified this schema.
   */
  private void initialize() throws JSONException
  {
    schema = new JSONObject(schemaJSON);

    Preconditions.checkState(schema.length() == NUM_KEYS_FIRST_LEVEL,
                             "Expected "
                             + NUM_KEYS_FIRST_LEVEL
                             + " keys in the first level but found "
                             + schema.length());

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

  /**
   * This is a helper method which sets the JSON that represents this schema.
   * @param schemaJSON The JSON that represents this schema.
   */
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

  /**
   * Returns the {@link FieldsDescriptor} object which represents the values in this schema.
   * @return The {@link FieldsDescriptor} object representing the values in this schema.
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
   * Gets the schemaID assigned to this schema. This is only relevant
   * when an operator is hosting multiple schemas.
   * @return The schemaID assigned to this schema.
   */
  @Override
  public int getSchemaID()
  {
    return schemaID;
  }
}
