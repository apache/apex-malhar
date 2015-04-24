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

package com.datatorrent.lib.appdata.converter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

public class MapGPOConverterSchema
{
  private static final Logger logger = LoggerFactory.getLogger(MapGPOConverterSchema.class);

  private String jsonString;
  private final Map<String, String> gpoFieldToMapField = Maps.newHashMap();

  public MapGPOConverterSchema(String jsonString)
  {
    this.jsonString = Preconditions.checkNotNull(jsonString);
    JSONObject jo = null;

    try {
      jo = new JSONObject(jsonString);
    }
    catch(JSONException ex) {
      throw new RuntimeException(ex);
    }

    Iterator keyIterator = jo.keys();

    logger.debug("Has Next {}", keyIterator.hasNext());

    while(keyIterator.hasNext()) {
      String key = (String) keyIterator.next();
      String value = null;

      try {
        value = jo.getString(key);
      }
      catch(JSONException ex) {
        throw new IllegalArgumentException(ex);
      }

      gpoFieldToMapField.put(key, value);
    }
  }

  public String getMapField(String gpoField)
  {
    String mapField = gpoFieldToMapField.get(gpoField);

    if(mapField == null) {
      return gpoField;
    }

    return mapField;
  }

  /**
   * @return the jsonString
   */
  public String getJsonString()
  {
    return jsonString;
  }

  /**
   * @param jsonString the jsonString to set
   */
  public void setJsonString(String jsonString)
  {
    this.jsonString = jsonString;
  }
}
