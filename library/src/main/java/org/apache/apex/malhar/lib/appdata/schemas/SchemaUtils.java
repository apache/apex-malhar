/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.appdata.schemas;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * This class holds utility methods for processing JSON.
 * @since 3.0.0
 */
public class SchemaUtils
{
  public static final String FIELD_TAGS = "tags";

  /**
   * This constructor should not be used.
   */
  private SchemaUtils()
  {
    //Do nothing
  }

  /**
   * This is a utility method which loads the contents of a resource file into a string.
   * @param resource The resource file whose contents need to be loaded.
   * @return The contents of the specified resource file.
   */
  public static String jarResourceFileToString(String resource)
  {
    StringWriter stringWriter = new StringWriter();
    try {
      InputStream is = SchemaUtils.class.getClassLoader().getResourceAsStream(resource);
      Preconditions.checkArgument(is != null, resource + " could not be found in the resources.");

      IOUtils.copy(is, stringWriter);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    return stringWriter.toString();
  }

  /**
   * This is a utility method that loads the contents of the given input stream into a string.
   * @param inputStream The input stream to read from.
   * @return The contents of the given input stream as a String.
   */
  public static String inputStreamToString(InputStream inputStream)
  {
    StringWriter stringWriter = new StringWriter();

    try {
      IOUtils.copy(inputStream, stringWriter);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    return stringWriter.toString();
  }

  /**
   * This is a utility method to check that the given JSONObject has the given keys.
   * @param jo The {@link JSONObject} to check.
   * @param fields The keys in the {@link JSONObject} to check.
   * @return True if the given {@link JSONObject} contains all the given keys. False otherwise.
   */
  public static boolean checkValidKeys(JSONObject jo, Fields fields)
  {
    @SuppressWarnings("unchecked")
    Set<String> fieldSet = fields.getFields();
    Set<String> jsonKeys = getSetOfJSONKeys(jo);

    return jsonKeys.containsAll(fieldSet);
  }

  /**
   * This is a utility method to check that the given JSONObject has the given keys.
   * It throws an {@link IllegalArgumentException} if it doesn't contain all the given keys.
   * @param jo The {@link JSONObject} to check.
   * @param fields The keys in the {@link JSONObject} to check.
   */
  public static void checkValidKeysEx(JSONObject jo, Fields fields)
  {
    @SuppressWarnings("unchecked")
    Set<String> fieldSet = fields.getFields();
    Set<String> jsonKeys = getSetOfJSONKeys(jo);

    if (!jsonKeys.containsAll(fieldSet)) {

      throw new IllegalArgumentException("The given set of keys " + fieldSet
          + " doesn't equal the set of keys in the json " + jsonKeys);
    }
  }

  /**
   * This is a utility method to check that the given JSONObject has the given keys.
   * @param jo The {@link JSONObject} to check.
   * @param fieldsCollection The keys in the {@link JSONObject} to check.
   * @return True if the given {@link JSONObject} contains all the given keys. False otherwise.
   */
  public static boolean checkValidKeys(JSONObject jo, Collection<Fields> fieldsCollection)
  {
    return checkValidKeysHelper(jo, fieldsCollection);
  }

  private static boolean checkValidKeysHelper(JSONObject jo, Collection<Fields> fieldsCollection)
  {
    for (Fields fields: fieldsCollection) {
      LOG.debug("Checking keys: {}", fields);
      if (checkValidKeys(jo, fields)) {
        return true;
      }
    }

    LOG.error("The first level of keys in the provided JSON {} do not match any of the " + "valid keysets {}",
        getSetOfJSONKeys(jo), fieldsCollection);
    return false;
  }

  public static boolean checkValidKeys(JSONObject jo, List<Fields> fieldsCollection)
  {
    return checkValidKeysHelper(jo, fieldsCollection);
  }

  /**
   * This is a utility method to check that the given JSONObject has the given keys.
   * It throws an {@link IllegalArgumentException} if it doesn't contain all the given keys.
   * @param jo The {@link JSONObject} to check.
   * @param fieldsCollection The keys in the {@link JSONObject} to check.
   * @return True if the given {@link JSONObject} contains all the given keys. False otherwise.
   */
  public static boolean checkValidKeysEx(JSONObject jo, Collection<Fields> fieldsCollection)
  {
    return checkValidKeysExHelper(jo, fieldsCollection);
  }

  public static boolean checkValidKeysExHelper(JSONObject jo, Collection<Fields> fieldsCollection)
  {
    for (Fields fields: fieldsCollection) {
      if (checkValidKeys(jo, fields)) {
        return true;
      }
    }

    Set<String> keys = getSetOfJSONKeys(jo);

    throw new IllegalArgumentException("The given json object has an invalid set of keys: " + keys
        + "\nOne of the following key combinations was expected:\n" + fieldsCollection);
  }

  public static boolean checkValidKeysEx(JSONObject jo, List<Fields> fieldsCollection)
  {
    return checkValidKeysExHelper(jo,
                                  fieldsCollection);
  }

  public static Set<String> getSetOfJSONKeys(JSONObject jo)
  {
    @SuppressWarnings("unchecked")
    Iterator<String> keyIterator = jo.keys();
    Set<String> keySet = Sets.newHashSet();

    while (keyIterator.hasNext()) {
      keySet.add(keyIterator.next());
    }

    return keySet;
  }

  public static Map<String, String> convertFieldToType(Map<String, Type> fieldToType)
  {
    Map<String, String> fieldToTypeString = Maps.newHashMap();

    for (Map.Entry<String, Type> entry : fieldToType.entrySet()) {
      String field = entry.getKey();
      String typeString = entry.getValue().name();

      fieldToTypeString.put(field, typeString);
    }

    return fieldToTypeString;
  }

  public static JSONArray findFirstKeyJSONArray(JSONObject jo, String key)
  {
    if (jo.has(key)) {
      try {
        JSONArray jsonArray = jo.getJSONArray(key);
        return jsonArray;
      } catch (JSONException ex) {
        throw new RuntimeException(ex);
      }
    }

    @SuppressWarnings("unchecked")
    Iterator<String> keyIterator = jo.keys();

    while (keyIterator.hasNext()) {
      String childKey = keyIterator.next();

      JSONArray childJa = null;

      try {
        childJa = jo.getJSONArray(childKey);
      } catch (JSONException ex) {
        //Do nothing
      }

      if (childJa != null) {
        JSONArray result = findFirstKeyJSONArray(childJa, key);

        if (result != null) {
          return result;
        }

        continue;
      }

      JSONObject childJo = null;

      try {
        childJo = jo.getJSONObject(childKey);
      } catch (JSONException ex) {
        //Do nothing
      }

      if (childJo != null) {
        JSONArray result = findFirstKeyJSONArray(childJo, key);

        if (result != null) {
          return result;
        }
      }
    }

    return null;
  }

  public static JSONArray findFirstKeyJSONArray(JSONArray ja, String key)
  {
    for (int index = 0; index < ja.length(); index++) {
      JSONArray childJa = null;

      try {
        childJa = ja.getJSONArray(index);
      } catch (JSONException ex) {
        //Do nothing
      }

      if (childJa != null) {
        JSONArray result = findFirstKeyJSONArray(childJa, key);

        if (result != null) {
          return result;
        }

        continue;
      }

      JSONObject childJo = null;

      try {
        childJo = ja.getJSONObject(index);
      } catch (JSONException ex) {
        //Do nothing
      }

      if (childJo != null) {
        JSONArray result = findFirstKeyJSONArray(childJo, key);

        if (result != null) {
          return result;
        }
      }
    }

    return null;
  }

  public static JSONObject findFirstKeyJSONObject(JSONObject jo, String key)
  {
    if (jo.has(key)) {
      try {
        JSONObject jsonObject = jo.getJSONObject(key);
        return jsonObject;
      } catch (JSONException ex) {
        throw new RuntimeException(ex);
      }
    }

    @SuppressWarnings("unchecked")
    Iterator<String> keyIterator = jo.keys();

    while (keyIterator.hasNext()) {
      String childKey = keyIterator.next();

      JSONArray childJa = null;

      try {
        childJa = jo.getJSONArray(childKey);
      } catch (JSONException ex) {
        //Do nothing
      }

      if (childJa != null) {
        JSONObject result = findFirstKeyJSONObject(childJa, key);

        if (result != null) {
          return result;
        }

        continue;
      }

      JSONObject childJo = null;

      try {
        childJo = jo.getJSONObject(childKey);
      } catch (JSONException ex) {
        //Do nothing
      }

      if (childJo != null) {
        JSONObject result = findFirstKeyJSONObject(childJo, key);

        if (result != null) {
          return result;
        }
      }
    }

    return null;
  }

  public static JSONObject findFirstKeyJSONObject(JSONArray ja, String key)
  {
    for (int index = 0; index < ja.length(); index++) {
      JSONArray childJa = null;

      try {
        childJa = ja.getJSONArray(index);
      } catch (JSONException ex) {
        //Do nothing
      }

      if (childJa != null) {
        JSONObject result = findFirstKeyJSONObject(childJa, key);

        if (result != null) {
          return result;
        }

        continue;
      }

      JSONObject childJo = null;

      try {
        childJo = ja.getJSONObject(index);
      } catch (JSONException ex) {
        //Do nothing
      }

      if (childJo != null) {
        JSONObject result = findFirstKeyJSONObject(childJo, key);

        if (result != null) {
          return result;
        }
      }
    }

    return null;
  }

  /**
   * Converts the given JSONObject into a {@link Map}.
   * @param jo The {@link JSONObject} to convert.
   * @return The converted {@link JSONObject}.
   */
  public static Map<String, String> extractMap(JSONObject jo)
  {
    Map<String, String> resultMap = Maps.newHashMap();
    @SuppressWarnings("unchecked")
    Iterator<String> keyIterator = jo.keys();

    while (keyIterator.hasNext()) {
      String key = keyIterator.next();
      String value;

      try {
        value = jo.getString(key);
      } catch (JSONException ex) {
        throw new RuntimeException(ex);
      }

      resultMap.put(key, value);
    }

    return resultMap;
  }

  /**
   * This is a utility method which creates a {@link JSONObject} out of the given map.
   * @param map The map to convert into a {@link JSONObject}.
   * @return The converted map.
   */
  public static JSONObject createJSONObject(Map<String, String> map)
  {
    JSONObject jo = new JSONObject();

    for (Map.Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();

      try {
        jo.put(key, value);
      } catch (JSONException ex) {
        throw new RuntimeException(ex);
      }
    }

    return jo;
  }

  /**
   * This is a helper method which converts the given {@link JSONArray} to a {@link List} of Strings.
   *
   * @param jsonStringArray The {@link JSONArray} to convert.
   * @return The converted {@link List} of Strings.
   */
  public static List<String> getStringsFromJSONArray(JSONArray jsonStringArray) throws JSONException
  {
    List<String> stringArray = Lists.newArrayListWithCapacity(jsonStringArray.length());

    for (int stringIndex = 0; stringIndex < jsonStringArray.length(); stringIndex++) {
      stringArray.add(jsonStringArray.getString(stringIndex));
    }

    return stringArray;
  }

  /**
   * This is a helper method which retrieves the schema tags from the {@link JSONObject} if they are present.
   *
   * @param jo The {@link JSONObject} to retrieve schema tags from.
   * @return A list containing the retrieved schema tags. The list is empty if there are no schema tags present.
   */
  public static List<String> getTags(JSONObject jo) throws JSONException
  {
    if (jo.has(FIELD_TAGS)) {
      return getStringsFromJSONArray(jo.getJSONArray(FIELD_TAGS));
    } else {
      return Collections.emptyList();
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);
}
