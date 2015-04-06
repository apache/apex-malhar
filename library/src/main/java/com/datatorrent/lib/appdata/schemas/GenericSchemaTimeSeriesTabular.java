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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.InputStream;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import java.util.Collections;
import java.util.Map;

public class GenericSchemaTimeSeriesTabular extends GenericSchemaWithTime
{
  public static final int NUM_KEYS_DATA = 5;
  public static final int NUM_KEYS_KEY = 2;
  public static final int NUM_KEYS_VALUE = 2;

  public static final String FIELD_KEYS = "keys";
  public static final String FIELD_KEY_NAME = "name";
  public static final String FIELD_KEY_TYPE = "type";

  private Map<String, Type> keyToType = Maps.newHashMap();

  public GenericSchemaTimeSeriesTabular(InputStream inputStream)
  {
    this(SchemaUtils.inputStreamToString(inputStream));
  }

  public GenericSchemaTimeSeriesTabular(String schemaJSON)
  {
    this(schemaJSON, true);
  }

  GenericSchemaTimeSeriesTabular(String schemaJSON, boolean validate)
  {
    super(schemaJSON);

    try {
      initialize(validate);
    }
    catch(Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void initialize(boolean validate) throws Exception
  {
    JSONObject schema = new JSONObject(getSchemaJSON());

    Preconditions.checkState(!(validate
                             && schema.length() != NUM_KEYS_FIRST_LEVEL),
                             "Expected "
                             + NUM_KEYS_FIRST_LEVEL
                             + " keys in the first level, but found "
                             + schema.length());

    JSONArray keys = schema.getJSONArray(FIELD_KEYS);

    Preconditions.checkState(keys.length() > 0,
                             "The schema must specify keys.");

    for(int index = 0;
        index < keys.length();
        index++)
    {
      JSONObject keyVal = keys.getJSONObject(index);

      Preconditions.checkState(keyVal.length() != NUM_KEYS_KEY,
                               "Expected " + NUM_KEYS_KEY +
                               " in the key definition, but found " + keyVal.length());

      String keyName = keyVal.getString(FIELD_KEY_NAME);
      String keyType = keyVal.getString(FIELD_KEY_TYPE);

      keyToType.put(keyName, Type.getTypeEx(keyType));
    }

    keyToType = Collections.unmodifiableMap(keyToType);
  }

  public Map<String, Type> getKeyToType()
  {
    return keyToType;
  }

  public Type getType(String key)
  {
    return keyToType.get(key);
  }
}
