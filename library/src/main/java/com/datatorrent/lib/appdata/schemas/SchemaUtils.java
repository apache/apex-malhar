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
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemaUtils
{
  private static final Logger logger = LoggerFactory.getLogger(SchemaUtils.class);
  public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

  private SchemaUtils()
  {
  }

  public static String getDateString(long time)
  {
    return sdf.format(new Date(time));
  }

  public static long getLong(String date)
  {
    try {
      return sdf.parse(date).getTime();
    }
    catch(ParseException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static String jarResourceFileToString(String resource)
  {
    StringWriter stringWriter = new StringWriter();
    try {
      InputStream is = SchemaUtils.class.getClassLoader().getResourceAsStream(resource);
      Preconditions.checkArgument(is != null, resource + " could not be found in the resources.");

      IOUtils.copy(is,
                   stringWriter);
    }
    catch(IOException ex) {
      throw new RuntimeException(ex);
    }
    return stringWriter.toString();
  }

  public static String inputStreamToString(InputStream inputStream)
  {
    StringWriter stringWriter = new StringWriter();

    try {
      IOUtils.copy(inputStream, stringWriter);
    }
    catch(IOException ex) {
      throw new RuntimeException(ex);
    }

    return stringWriter.toString();
  }

  public static void checkDateEx(String date)
  {
    try {
      sdf.parse(date);
    }
    catch(ParseException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static boolean checkDate(String date)
  {
    try {
      checkDateEx(date);
    }
    catch(Exception e) {
      return false;
    }

    return true;
  }

  public static boolean checkValidKeys(JSONObject jo,
                                       Fields fields)
  {
    @SuppressWarnings("unchecked")
    Iterator<String> keyIterator = jo.keys();
    Set<String> fieldSet = fields.getFields();

    while(keyIterator.hasNext()) {
      String key = keyIterator.next();

      if(!fieldSet.contains(key)) {
        return false;
      }
    }

    return true;
  }

  public static void checkValidKeysEx(JSONObject jo,
                                      Fields fields)
  {
    @SuppressWarnings("unchecked")
    Iterator<String> keyIterator = jo.keys();
    Set<String> fieldSet = fields.getFields();

    while(keyIterator.hasNext()) {
      String key = keyIterator.next();

      if(!fieldSet.contains(key)) {
        throw new IllegalArgumentException("The key " + key +
                                           " is not valid.");
      }
    }
  }

  public static boolean checkValidKeys(JSONObject jo,
                                       List<Fields> fieldsList)
  {
    for(Fields fields: fieldsList) {
      if(checkValidKeys(jo, fields)) {
        return true;
      }
    }

    return false;
  }

  public static boolean checkValidKeysEx(JSONObject jo,
                                         List<Fields> fieldsList)
  {
    for(Fields fields: fieldsList) {
      if(checkValidKeys(jo, fields)) {
        return true;
      }
    }

    Set<String> keys = Sets.newHashSet();
    @SuppressWarnings("unchecked")
    Iterator<String> keyIterator = jo.keys();

    while(keyIterator.hasNext()) {
      String key = keyIterator.next();
      keys.add(key);
    }

    throw new IllegalArgumentException("The given json object has an invalid set of keys: " +
                                       keys +
                                       "\nOne of the following key combinations was expected:\n" +
                                       fieldsList);
  }

  public static Map<String, String> convertFieldToType(Map<String, Type> fieldToType)
  {
    Map<String, String> fieldToTypeString = Maps.newHashMap();

    for(Map.Entry<String, Type> entry: fieldToType.entrySet()) {
      String field = entry.getKey();
      String typeString = entry.getValue().name();

      fieldToTypeString.put(field, typeString);
    }

    return fieldToTypeString;
  }

  public static JSONArray findFirstKeyJSONArray(JSONObject jo, String key)
  {
    if(jo.has(key)) {
      try {
        JSONArray jsonArray = jo.getJSONArray(key);
        return jsonArray;
      }
      catch(JSONException ex) {
        throw new RuntimeException(ex);
      }
    }

    @SuppressWarnings("unchecked")
    Iterator<String> keyIterator = jo.keys();

    while(keyIterator.hasNext()) {
      String childKey = keyIterator.next();

      JSONArray childJa = null;

      try {
        childJa = jo.getJSONArray(childKey);
      }
      catch(JSONException ex) {
        //Do nothing
      }

      if(childJa != null) {
        JSONArray result = findFirstKeyJSONArray(childJa, key);

        if(result != null) {
          return result;
        }

        continue;
      }

      JSONObject childJo = null;

      try {
        childJo = jo.getJSONObject(childKey);
      }
      catch(JSONException ex) {
        //Do nothing
      }

      if(childJo != null) {
        JSONArray result = findFirstKeyJSONArray(childJo, key);

        if(result != null) {
          return result;
        }
      }
    }

    return null;
  }

  public static JSONArray findFirstKeyJSONArray(JSONArray ja, String key)
  {
    for(int index = 0;
        index < ja.length();
        index++) {
      JSONArray childJa = null;

      try {
        childJa = ja.getJSONArray(index);
      }
      catch(JSONException ex) {
        //Do nothing
      }

      if(childJa != null) {
        JSONArray result = findFirstKeyJSONArray(childJa, key);

        if(result != null) {
          return result;
        }

        continue;
      }

      JSONObject childJo = null;

      try {
        childJo = ja.getJSONObject(index);
      }
      catch(JSONException ex) {
        //Do nothing
      }

      if(childJo != null) {
        JSONArray result = findFirstKeyJSONArray(childJo, key);

        if(result != null) {
          return result;
        }
      }
    }

    return null;
  }

  public static JSONObject findFirstKeyJSONObject(JSONObject jo, String key)
  {
    if(jo.has(key)) {
      try {
        JSONObject jsonObject = jo.getJSONObject(key);
        return jsonObject;
      }
      catch(JSONException ex) {
        throw new RuntimeException(ex);
      }
    }

    @SuppressWarnings("unchecked")
    Iterator<String> keyIterator = jo.keys();

    while(keyIterator.hasNext()) {
      String childKey = keyIterator.next();

      JSONArray childJa = null;

      try {
        childJa = jo.getJSONArray(childKey);
      }
      catch(JSONException ex) {
        //Do nothing
      }

      if(childJa != null) {
        JSONObject result = findFirstKeyJSONObject(childJa, key);

        if(result != null) {
          return result;
        }

        continue;
      }

      JSONObject childJo = null;

      try {
        childJo = jo.getJSONObject(childKey);
      }
      catch(JSONException ex) {
        //Do nothing
      }

      if(childJo != null) {
        JSONObject result = findFirstKeyJSONObject(childJo, key);

        if(result != null) {
          return result;
        }
      }
    }

    return null;
  }

  public static JSONObject findFirstKeyJSONObject(JSONArray ja, String key)
  {
    for(int index = 0;
        index < ja.length();
        index++) {
      JSONArray childJa = null;

      try {
        childJa = ja.getJSONArray(index);
      }
      catch(JSONException ex) {
        //Do nothing
      }

      if(childJa != null) {
        JSONObject result = findFirstKeyJSONObject(childJa, key);

        if(result != null) {
          return result;
        }

        continue;
      }

      JSONObject childJo = null;

      try {
        childJo = ja.getJSONObject(index);
      }
      catch(JSONException ex) {
        //Do nothing
      }

      if(childJo != null) {
        JSONObject result = findFirstKeyJSONObject(childJo, key);

        if(result != null) {
          return result;
        }
      }
    }

    return null;
  }

  public static Map<String, String> extractMap(JSONObject jo)
  {
    Map<String, String> resultMap = Maps.newHashMap();
    @SuppressWarnings("unchecked")
    Iterator<String> keyIterator = jo.keys();

    while(keyIterator.hasNext()) {
      String key = keyIterator.next();
      String value;

      try {
        value = jo.getString(key);
      }
      catch(JSONException ex) {
        throw new RuntimeException(ex);
      }

      resultMap.put(key, value);
    }

    return resultMap;
  }
}
