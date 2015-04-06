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
import org.codehaus.jettison.json.JSONObject;

import java.util.Collections;
import java.util.Map;

public class GenericSchemaTabular implements GenericSchema
{
  public static final String FIELD_SCHEMA_TYPE = "schemaType";
  public static final String FIELD_SCHEMA_VERSION = "schemaVersion";

  public static final String FIELD_VALUES = "values";
  public static final String FIELD_VALUES_NAME = "name";
  public static final String FIELD_VALUES_TYPE = "type";

  public static final int NUM_KEYS_FIRST_LEVEL = 3;
  public static final int NUM_KEYS_VALUES = 2;

  private String schemaJSON;
  private String schemaType;
  private String schemaVersion;

  private Map<String, Type> valueToType;
  private FieldsDescriptor valuesDescriptor;

  public GenericSchemaTabular(String schemaJSON)
  {
    this(schemaJSON, true);
  }

  //This would be needed for more rigorous validation of schemas
  GenericSchemaTabular(String schemaJSON,
                       boolean validate)
  {
    setSchema(schemaJSON);

    try {
      initialize(validate);
    }
    catch(Exception ex) {
      DTThrowable.rethrow(ex);
    }
  }

  private void initialize(boolean validate) throws Exception
  {
    JSONObject schema = new JSONObject(schemaJSON);

    if(validate) {
      Preconditions.checkState(schema.length() == NUM_KEYS_FIRST_LEVEL,
                               "Expected "
                               + NUM_KEYS_FIRST_LEVEL
                               + " keys in the first level but found "
                               + schema.length());
    }

    schemaType = schema.getString(FIELD_SCHEMA_TYPE);
    schemaVersion = schema.getString(FIELD_SCHEMA_VERSION);
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
  }

  protected void setSchema(String schemaJSON)
  {
    Preconditions.checkNotNull(schemaJSON);
    this.schemaJSON = schemaJSON;
  }

  @Override
  public String getSchemaJSON()
  {
    return schemaJSON;
  }

  @Override
  public String getSchemaType()
  {
    return schemaType;
  }

  @Override
  public String getSchemaVersion()
  {
    return schemaVersion;
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
}
