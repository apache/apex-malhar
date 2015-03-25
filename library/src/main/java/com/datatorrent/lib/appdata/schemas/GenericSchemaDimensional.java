/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericSchemaDimensional extends GenericSchemaWithTime
{
  public static final int NUM_KEYS_DATA = 5;
  public static final int NUM_KEYS_KEY = 2;
  public static final int NUM_KEYS_VALUE = 2;

  public static final String FIELD_KEYS = "keys";
  public static final String FIELD_KEY_NAME = "name";
  public static final String FIELD_KEY_VALS = "enumValues";
  public static final String FIELD_KEY_TYPE = "type";

  public static final String EXTRA_FIELD_NAME_TIME = "time";
  public static final Type EXTRA_FIELD_TYPE_TIME = Type.STRING;

  private Map<String, Type> keyToType = Maps.newHashMap();
  private Map<String, Set<Object>> keyToValues = Maps.newHashMap();
  private FieldsDescriptor keyFieldsDescriptor;

  public GenericSchemaDimensional(String schemaJSON)
  {
    this(schemaJSON, false);
  }

  public GenericSchemaDimensional(String schemaJSON, boolean validate)
  {
    super(schemaJSON);

    try {
      initialize();
    }
    catch(Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void initialize() throws Exception
  {
    JSONObject schema = new JSONObject(getSchemaJSON());
    JSONArray keys = schema.getJSONArray(FIELD_KEYS);

    Preconditions.checkState(keys.length() > 0,
                             "The schema must specify keys.");

    for(int index = 0;
        index < keys.length();
        index++) {
      JSONObject keyVal = keys.getJSONObject(index);
      System.out.println("KeyVal: " + keyVal.toString());
      Preconditions.checkState(keyVal.length() == NUM_KEYS_KEY,
                               "Expected " + NUM_KEYS_KEY +
                               " in the key definition, but found " + keyVal.length());

      String keyName = keyVal.getString(FIELD_KEY_NAME);
      keyToType.put(keyName, Type.STRING);
      JSONArray valArray = keyVal.getJSONArray(FIELD_KEY_VALS);

      Type maxType = null;

      //Validate the provided data types
      for(int valIndex = 0;
          valIndex < valArray.length();
          valIndex++) {
        Object val = valArray.get(index);

        Preconditions.checkState(!(val instanceof JSONArray
                                   || val instanceof JSONObject),
                                 "The value must be a primitive.");

        Type currentType = Type.CLASS_TO_TYPE.get(val.getClass());

        if(maxType == null) {
          maxType = currentType;
        }
        else if (maxType != currentType) {
          if(maxType.getHigherTypes().contains(currentType)) {
            maxType = currentType;
          }
          else {
            Preconditions.checkState(currentType.getHigherTypes().contains(maxType),
                                     "Conficting types: " + currentType.getName() +
                                     " cannot be converted to " + maxType.getName());
          }
        }
      }

      //Load the data into a set
      Set<Object> vals = Sets.newHashSet();

      for(int valIndex = 0;
          valIndex < valArray.length();
          valIndex++) {
        Object val = valArray.get(valIndex);
        Type valType = Type.CLASS_TO_TYPE.get(val.getClass());
        Object promotedVal = Type.promote(valType, maxType, val);

        Preconditions.checkState(vals.add(promotedVal),
                                 "Duplicate value: " + promotedVal);
      }

      vals = Collections.unmodifiableSet(vals);

      getKeyToType().put(keyName, maxType);
      getKeyToValues().put(keyName, vals);
    }

    keyToType.put(EXTRA_FIELD_NAME_TIME, Type.STRING);

    keyToType = Collections.unmodifiableMap(getKeyToType());
    keyToValues = Collections.unmodifiableMap(getKeyToValues());
    keyFieldsDescriptor = new FieldsDescriptor(keyToType);
  }

  /**
   * @return the keyToType
   */
  public Map<String, Type> getKeyToType()
  {
    return keyToType;
  }

  /**
   * @return the keyToValues
   */
  public Map<String, Set<Object>> getKeyToValues()
  {
    return keyToValues;
  }

  /**
   * @return the keyFieldsDescriptor
   */
  public FieldsDescriptor getKeyFieldsDescriptor()
  {
    return keyFieldsDescriptor;
  }
}
