/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.datamodel.converter;

import java.util.Iterator;
import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.collect.Maps;

/**
 * A {@link Converter} implementation that converts {@link JSONObject} to a {@link Map}
 */
public class JsonToFlatMapConverter implements Converter<JSONObject, Map<String, Object>>
{
  private char concatChar = '_';

  @Override
  public Map<String, Object> convert(JSONObject jsonObject)
  {
    Map<String, Object> output = Maps.newHashMap();
    try {
      getFlatMap(jsonObject, output, null);
    }
    catch (JSONException e) {
      throw new RuntimeException("converting to map", e);
    }
    return output;
  }

  @SuppressWarnings("unchecked")
  private void getFlatMap(JSONObject jSONObject, Map<String, Object> map, String keyPrefix) throws JSONException
  {
    Iterator<String> iterator = jSONObject.keys();
    while (iterator.hasNext()) {
      String key = iterator.next();
      String insertKey = (keyPrefix == null) ? key : keyPrefix + concatChar + key;

      JSONObject value = jSONObject.optJSONObject(key);
      if (value == null) {
        map.put(insertKey, jSONObject.get(key));
      }
      else {
        getFlatMap(value, map, insertKey);
      }
    }
  }

  /**
   * Sets the concatenation character.
   *
   * @param concatChar
   */
  public void setConcatChar(char concatChar)
  {
    this.concatChar = concatChar;
  }
}