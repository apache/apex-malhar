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
package com.datatorrent.lib.appdata.gpo;

import java.io.Serializable;
import java.lang.reflect.Array;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.ResultFormatter;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterByte;
import com.datatorrent.lib.util.PojoUtils.GetterChar;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.GetterShort;

/**
 * This class holds utility methods for serializing and deserializing {@link GPOMutable} objects to/from bytes and JSON.
 * There are also utility methods for converting POJOs into GPOMutable objects.
 */
public class GPOUtils
{
  /**
   * This class should not be instantiated
   */
  private GPOUtils()
  {
    //Do nothing
  }

  /**
   * This utility method converts a field to type map specified in JSON into
   * a java map from field to type. An example of a JSON field to type map
   * is the following:
   * <br/>
   * <br/>
   * <pre>
   * {@code
   * {
   *  "fieldName1":"integer",
   *  "fieldName2":"string",
   *  "fieldName3":"byte",
   *  ...
   * }
   * }
   * </pre>
   * @param jo The {@link JSONObject} containing the JSON to convert.
   * @return A java Map from field name to the corresponding field type.
   * @throws JSONException
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Type> buildTypeMap(JSONObject jo) throws JSONException
  {
    Map<String, Type> fieldToType = Maps.newHashMap();
    for(Iterator<String> keys = (Iterator<String>) jo.keys();
        keys.hasNext();) {
      String key = keys.next();
      String val = jo.getString(key);
      Type type = Type.getTypeEx(val);
      fieldToType.put(key, type);
    }

    return fieldToType;
  }

  /**
   * Converts the provided JSON into a GPOMutable object with the provided {@link FieldsDescriptor}
   * @param fieldsDescriptor The {@link FieldsDescriptor} to initialize the {@link GPOMutable} object with.
   * @param dpou The JSONObject to deserialize from.
   * @return The deserialized GPOMutable object.
   */
  public static GPOMutable deserialize(FieldsDescriptor fieldsDescriptor,
                                       JSONObject dpou)
  {
    GPOMutable gpo = new GPOMutable(fieldsDescriptor);
    @SuppressWarnings("unchecked")
    Iterator<String> itr = (Iterator<String>) dpou.keys();

    while(itr.hasNext()) {
      String field = itr.next();
      setFieldFromJSON(gpo, field, dpou);
    }

    return gpo;
  }

  /**
   * This is a helper method for deserialization of a GPOMutable from JSON. It allows you to select a field from
   * a JSONObject in a json array and set it on the provided GPOMutable object. The format of the JSONArray should
   * be the following:
   * <br/>
   * <br/>
   * <pre>
   * {@code
   * [
   *  {
   *    "fieldName1":"val1",
   *    "fieldName2":"val2",
   *    ...
   *  },
   *  {
   *    "fieldName1":"valx",
   *    ...
   *  }
   *  ...
   * ]
   * }
   * </pre>
   * <br/>
   * <br/>
   * @param gpo The {@link GPOMutable} to set fields on.
   * @param type The type of the field that will be set.
   * @param field The name of the field that will be set.
   * The name of the field must be the same in both the {@link GPOMutable}'s {@link FieldsDescriptor} object and in the JSONObject.
   * @param jo The JSONOArray holding JSONObjects to deserialize from.
   * @param index The index of the JSONArray
   */
  public static void setFieldFromJSON(GPOMutable gpo, Type type, String field, JSONArray jo, int index)
  {
    GPOType gpoType = GPOType.GPO_TYPE_ARRAY[type.ordinal()];
    gpoType.setFieldFromJSON(gpo, field, jo, index);
  }

  /**
   * This is a utility method to deserialize data from the given JSONObject into a {@link GPOMutable} object.
   * The format of the JSON to deserialize from should look like this.
   * <pre>
   *  {@code
   *  {
   *    "fieldName1":"val1",
   *    "fieldName2":"val2",
   *    ...
   *  }
   *  }
   * </pre>
   * @param gpo The {@link GPOMutable} object to deserialize into.
   * @param field The name of the field to take from the JSON and place into the {@link GPOMutable} object.
   * The name of the field must be the same in the {@link FieldsDescriptor} and the {@link GPOMutable} object.
   * @param jo The JSONObject to deserialize from.
   */
  public static void setFieldFromJSON(GPOMutable gpo, String field, JSONObject jo)
  {
    Type type = gpo.getFieldDescriptor().getType(field);

    if(type == Type.BOOLEAN) {
      Boolean val;

      try {
        val = jo.getBoolean(field);
      }
      catch(JSONException ex) {
        throw new IllegalArgumentException("The key " + field + " does not have a valid bool value.", ex);
      }

      gpo.setFieldGeneric(field, val);
    }
    else if(type == Type.BYTE) {
      int val;

      try {
        val = jo.getInt(field);
      }
      catch(JSONException ex) {
        throw new IllegalArgumentException("The key "
                                           + field
                                           + " does not have a valid byte value.", ex);
      }

      if(val < (int)Byte.MIN_VALUE) {
        throw new IllegalArgumentException("The key "
                                           + field
                                           + " has a value "
                                           + val
                                           + " which is too small to fit into a byte.");
      }

      if(val > (int)Byte.MAX_VALUE) {
        throw new IllegalArgumentException("The key "
                                           + field
                                           + " has a value "
                                           + val
                                           + " which is too larg to fit into a byte.");
      }

      gpo.setField(field, (byte)val);
    }
    else if(type == Type.SHORT) {
      int val;

      try {
        val = jo.getInt(field);
      }
      catch(JSONException ex) {
        throw new IllegalArgumentException("The key "
                                           + field
                                           + " does not have a valid short value.",
                                           ex);
      }

      if(val < (int)Short.MIN_VALUE) {
        throw new IllegalArgumentException("The key "
                                           + field
                                           + " has a value "
                                           + val
                                           + " which is too small to fit into a short.");
      }

      if(val > (int)Short.MAX_VALUE) {
        throw new IllegalArgumentException("The key "
                                           + field
                                           + " has a value "
                                           + val
                                           + " which is too large to fit into a short.");
      }

      gpo.setField(field, (short)val);
    }
    else if(type == Type.INTEGER) {
      int val;

      try {
        val = jo.getInt(field);
      }
      catch(JSONException ex) {
        throw new IllegalArgumentException("The key "
                                           + field
                                           + " does not have a valid int value.",
                                           ex);
      }

      gpo.setField(field, val);
    }
    else if(type == Type.LONG) {
      long val;

      try {
        val = jo.getLong(field);
      }
      catch(JSONException ex) {
        throw new IllegalArgumentException("The key "
                                           + field
                                           + " does not have a valid long value.",
                                           ex);
      }

      gpo.setField(field, val);
    }
    else if(type == Type.CHAR) {
      String val;

      try {
        val = jo.getString(field);
      }
      catch(JSONException ex) {
        throw new IllegalArgumentException("The key "
                                           + field
                                           + " does not have a valid character value.",
                                           ex);
      }

      if(val.length() != 1) {
        throw new IllegalArgumentException("The key "
                                           + field
                                           + " has a value "
                                           + val
                                           + " that is not one character long.");
      }

      gpo.setField(field, val.charAt(0));
    }
    else if(type == Type.STRING) {
      String val;

      try {
        val = jo.getString(field);
      }
      catch(JSONException ex) {
        throw new IllegalArgumentException("The key "
                                           + field
                                           + " does not have a valid string value.",
                                           ex);
      }

      gpo.setField(field, val);
    }
    else if(type == Type.DOUBLE) {
      Double val;

      try {
        val = jo.getDouble(field);
      }
      catch(JSONException ex) {
        throw new IllegalArgumentException("The key "
                                           + field
                                           + " does not have a valid double value.",
                                           ex);
      }

      gpo.setFieldGeneric(field, val);
    }
    else if(type == Type.FLOAT) {
      Float val;

      try {
        val = (float)jo.getDouble(field);
      }
      catch(JSONException ex) {
        throw new IllegalArgumentException("The key "
                                           + field
                                           + " does not have a valid double value.",
                                           ex);
      }

      gpo.setFieldGeneric(field, val);
    }
  }

  /**
   * This utility method serializes the given fields from the given {@link GPOMutable} object into JSON using the
   * given resultFormatter.
   * @param gpo The {@link GPOMutable} to serialize.
   * @param fields The fields from the given {@link GPOMutable} object to serialize.
   * @param resultFormatter The result formatter to use when formatting the output data during serialization.
   * @return The serialized {@link GPOMutable} object.
   * @throws JSONException
   */
  public static JSONObject serializeJSONObject(GPOMutable gpo, Fields fields, ResultFormatter resultFormatter) throws JSONException
  {
    JSONObject jo = new JSONObject();
    FieldsDescriptor fd = gpo.getFieldDescriptor();

    for(String field: fields.getFields()) {
      Type fieldType = fd.getType(field);
      GPOType gpoType = GPOType.GPO_TYPE_ARRAY[fieldType.ordinal()];
      gpoType.serializeJSONObject(jo, gpo, field, resultFormatter);
    }

    return jo;
  }

  /**
   * Serializes the given {@link GPOMutable} object with the given resultFormatter.
   * @param gpo The {@link GPOMutable} to serialize.
   * @param resultFormatter The result formatter to use when serializing data.
   * @return The serialized {@link GPOMutable} object.
   * @throws JSONException
   */
  public static JSONObject serializeJSONObject(GPOMutable gpo, ResultFormatter resultFormatter) throws JSONException
  {
    return serializeJSONObject(gpo, gpo.getFieldDescriptor().getFields(), resultFormatter);
  }

  /**
   * Computes the number of bytes required to serialize the given {@link GPOMutable} object. Excluding the
   * object fields in the {@link GPOMutable}.
   * @param gpo The {@link GPOMutable} object to compute a serialized size for.
   * @return The serialized size for the given {@link GPOMutable} object.
   */
  public static int serializedLength(GPOMutable gpo)
  {
    int arrayLength = 0;
    FieldsDescriptor fd = gpo.getFieldDescriptor();

    List<Type> types = fd.getTypesList();

    for(int typeIndex = 0;
        typeIndex < types.size();
        typeIndex++) {
      Type type = types.get(typeIndex);

      switch(type) {
        case STRING: {
          for(String val: gpo.getFieldsString()) {
            arrayLength += Type.INTEGER.getByteSize();
            arrayLength += val.getBytes().length;
          }
          break;
        }
        case OBJECT: {
          //Don't include objects.
          break;
        }
        default: {
          arrayLength += fd.getTypeToFields().get(type).size() *
                         type.getByteSize();
        }
      }
    }

    return arrayLength;
  }

  /**
   * Serializes the given {@link GPOMutable} object to an array of bytes.
   * @param gpo The {@link GPOMutable} object to serialize.
   * @param byteArrayList A byte array list to pack serialized data into. Note that
   * it is assumed that the byteArrayList is empty when passed to this method.
   * @return The serialized {@link GPOMutable} object.
   */
  public static byte[] serialize(GPOMutable gpo, GPOByteArrayList byteArrayList)
  {
    int slength = serializedLength(gpo);
    byte[] sbytes = new byte[slength];
    MutableInt offset = new MutableInt(0);

    boolean[] fieldsBoolean = gpo.getFieldsBoolean();
    if(fieldsBoolean != null) {
      for(int index = 0;
          index < fieldsBoolean.length;
          index++) {
        serializeBoolean(fieldsBoolean[index],
                         sbytes,
                         offset);
      }
    }

    char[] fieldsCharacter = gpo.getFieldsCharacter();
    if(fieldsCharacter != null) {
      for(int index = 0;
          index < fieldsCharacter.length;
          index++) {
        serializeChar(fieldsCharacter[index],
                      sbytes,
                      offset);
      }
    }

    byte[] fieldsByte = gpo.getFieldsByte();
    if(fieldsByte != null) {
      for(int index = 0;
          index < fieldsByte.length;
          index++) {
        serializeByte(fieldsByte[index],
                      sbytes,
                      offset);
      }
    }

    short[] fieldsShort = gpo.getFieldsShort();
    if(fieldsShort != null) {
      for(int index = 0;
          index < fieldsShort.length;
          index++) {
        serializeShort(fieldsShort[index],
                      sbytes,
                      offset);
      }
    }

    int[] fieldsInteger = gpo.getFieldsInteger();
    if(fieldsInteger != null) {
      for(int index = 0;
          index < fieldsInteger.length;
          index++) {
        serializeInt(fieldsInteger[index],
                      sbytes,
                      offset);
      }
    }

    long[] fieldsLong = gpo.getFieldsLong();
    if(fieldsLong != null) {
      for(int index = 0;
          index < fieldsLong.length;
          index++) {
        serializeLong(fieldsLong[index],
                      sbytes,
                      offset);
      }
    }

    float[] fieldsFloat = gpo.getFieldsFloat();
    if(fieldsFloat != null) {
      for(int index = 0;
          index < fieldsFloat.length;
          index++) {
        serializeFloat(fieldsFloat[index],
                      sbytes,
                      offset);
      }
    }

    double[] fieldsDouble = gpo.getFieldsDouble();
    if(fieldsDouble != null) {
      for(int index = 0;
          index < fieldsDouble.length;
          index++) {
        serializeDouble(fieldsDouble[index],
                      sbytes,
                      offset);
      }
    }

    String[] fieldsString = gpo.getFieldsString();
    if(fieldsString != null) {
      for(int index = 0;
          index < fieldsString.length;
          index++) {
        serializeString(fieldsString[index],
                      sbytes,
                      offset);
      }
    }

    if(sbytes.length > 0) {
      byteArrayList.add(sbytes);
    }

    Object[] fieldsObject = gpo.getFieldsObject();
    Serde[] serdes = gpo.getFieldDescriptor().getSerdes();
    if(fieldsObject != null) {
      for(int index = 0;
          index < fieldsObject.length;
          index++) {
        byteArrayList.add(serdes[index].serializeObject(fieldsObject[index]));
      }
    }

    byte[] bytes = byteArrayList.toByteArray();
    byteArrayList.clear();
    return bytes;
  }

  /**
   * Serializes the given {@link GPOMutable} object while excluding the provided fields from the serialization.
   * @param gpo The {@link GPOMutable} to serialize.
   * @param excludedFields The fields from the {@link GPOMutable} object to exclude.
   * @return A byte array containing the serialized {@link GPOMutable}.
   */
  public static byte[] serialize(GPOMutable gpo, Fields excludedFields)
  {
    int slength = serializedLength(gpo);
    byte[] sbytes = new byte[slength];
    MutableInt offset = new MutableInt(0);

    Set<String> fields = gpo.getFieldDescriptor().getFields().getFields();
    Set<String> exFieldsSet = excludedFields.getFields();

    for(String field: fields) {
      if(exFieldsSet.contains(field)) {
        continue;
      }

      Type type = gpo.getFieldDescriptor().getType(field);
      GPOType gpoType = GPOType.GPO_TYPE_ARRAY[type.ordinal()];
      gpoType.serialize(gpo, field, sbytes, offset);
    }

    return sbytes;
  }

  /**
   * Deserializes a {@link GPOMutable} object from the given byte array at the given offset with the given
   * {@link FieldsDescriptor} object.
   * @param fd The {@link FieldsDescriptor} object which describes the schema of the {@link GPOMutable} object
   * to deserialize.
   * @param serializedGPO A byte array containing the serialized {@link GPOMutable} object.
   * @param offset An offset in the byte array to start deserializing from.
   * @return The deserialized GPOMutable.
   */
  public static GPOMutable deserialize(FieldsDescriptor fd,
                                       byte[] serializedGPO,
                                       MutableInt offset)
  {
    GPOMutable gpo = new GPOMutable(fd);

    boolean[] fieldsBoolean = gpo.getFieldsBoolean();
    if(fieldsBoolean != null) {
      for(int index = 0;
          index < fieldsBoolean.length;
          index++) {
        fieldsBoolean[index] = deserializeBoolean(serializedGPO, offset);
      }
    }

    char[] fieldsCharacter = gpo.getFieldsCharacter();
    if(fieldsCharacter != null) {
      for(int index = 0;
          index < fieldsCharacter.length;
          index++) {
        fieldsCharacter[index] = deserializeChar(serializedGPO, offset);
      }
    }

    byte[] fieldsByte = gpo.getFieldsByte();
    if(fieldsByte != null) {
      for(int index = 0;
          index < fieldsByte.length;
          index++) {
        fieldsByte[index] = deserializeByte(serializedGPO, offset);
      }
    }

    short[] fieldsShort = gpo.getFieldsShort();
    if(fieldsShort != null) {
      for(int index = 0;
          index < fieldsShort.length;
          index++) {
        fieldsShort[index] = deserializeShort(serializedGPO, offset);
      }
    }

    int[] fieldsInteger = gpo.getFieldsInteger();
    if(fieldsInteger != null) {
      for(int index = 0;
          index < fieldsInteger.length;
          index++) {
        fieldsInteger[index] = deserializeInt(serializedGPO, offset);
      }
    }

    long[] fieldsLong = gpo.getFieldsLong();
    if(fieldsLong != null) {
      for(int index = 0;
          index < fieldsLong.length;
          index++) {
        fieldsLong[index] = deserializeLong(serializedGPO, offset);
      }
    }

    float[] fieldsFloat = gpo.getFieldsFloat();
    if(fieldsFloat != null) {
      for(int index = 0;
          index < fieldsFloat.length;
          index++) {
        fieldsFloat[index] = deserializeFloat(serializedGPO, offset);
      }
    }

    double[] fieldsDouble = gpo.getFieldsDouble();
    if(fieldsDouble != null) {
      for(int index = 0;
          index < fieldsDouble.length;
          index++) {
        fieldsDouble[index] = deserializeDouble(serializedGPO, offset);
      }
    }

    String[] fieldsString = gpo.getFieldsString();
    if(fieldsString != null) {
      for(int index = 0;
          index < fieldsString.length;
          index++) {
        fieldsString[index] = deserializeString(serializedGPO, offset);
      }
    }

    Object[] fieldsObject = gpo.getFieldsObject();
    Serde[] serdes = fd.getSerdes();
    if(fieldsObject != null) {
      for(int index = 0;
          index < fieldsObject.length;
          index++) {
        fieldsObject[index] = serdes[index].deserializeObject(serializedGPO, offset);
      }
    }

    return gpo;
  }

  /**
   * Deserializes a {@link GPOMutable} object from the given byte array at the given offset, which was
   * serialized with the given {@link FieldsDescriptor} with the given fields excluded.
   * @param fieldsDescriptor The {@link FieldsDescriptor} object corresponding to the {@link GPOMutable}.
   * @param excludedFields The fields to exclude from serializing the {@link GPOMutable}.
   * @param serializedGPO The array containing the serialized {@link GPOMutable}.
   * @param offset The offset in the provided array to start deserializing from.
   * @return The deserialized {@link GPOMutable}.
   */
  public static GPOMutable deserialize(FieldsDescriptor fieldsDescriptor,
                                       Fields excludedFields,
                                       byte[] serializedGPO,
                                       int offset)
  {
    GPOMutable gpo = new GPOMutable(fieldsDescriptor);
    MutableInt offsetM = new MutableInt(offset);

    Set<String> exFieldsSet = excludedFields.getFields();

    for(String field: fieldsDescriptor.getFields().getFields()) {
      if(exFieldsSet.contains(field)) {
        continue;
      }

      Type type = fieldsDescriptor.getType(field);
      GPOType gpoType = GPOType.GPO_TYPE_ARRAY[type.ordinal()];
      gpoType.deserialize(gpo, field, serializedGPO, offsetM);
    }

    return gpo;
  }

  /**
   * This method deserializes a string from the given byte array from the given offset,
   * and increments the offset appropriately.
   * @param buffer The byte buffer to deserialize from.
   * @param offset The offset to deserialize from.
   * @return The deserialized string.
   */
  public static String deserializeString(byte[] buffer,
                                         MutableInt offset)
  {
    int length = deserializeInt(buffer,
                                offset);

    String val = new String(buffer, offset.intValue(), length);
    offset.add(length);
    return val;
  }

  /**
   * This method serializes the given string to the given byte buffer to the given offset,
   * the method also increments the offset appropriately.
   * @param val The value to serialize.
   * @param buffer The byte buffer to serialize to.
   * @param offset The offset in the buffer to serialize to and also to increment appropriately.
   */
  public static void serializeString(String val,
                                     byte[] buffer,
                                     MutableInt offset)
  {
    byte[] stringBytes = val.getBytes();
    int length = stringBytes.length;

    serializeInt(length,
                 buffer,
                 offset);

    for(int index = 0;
        index < length;
        index++) {
      buffer[offset.intValue() + index] = stringBytes[index];
    }

    offset.add(length);
  }

  public static byte[] serializeString(String val)
  {
    byte[] stringBytes = val.getBytes();
    byte[] length = GPOUtils.serializeInt(stringBytes.length);

    byte[] serialized = new byte[stringBytes.length + length.length];

    System.arraycopy(length, 0, serialized, 0, length.length);
    System.arraycopy(stringBytes, 0, serialized, length.length, stringBytes.length);

    return serialized;
  }

  /**
   * This method deserializes a long from the given byte array from the given offset,
   * and increments the offset appropriately.
   * @param buffer The byte buffer to deserialize from.
   * @param offset The offset to deserialize from.
   * @return The deserialized long.
   */
  public static long deserializeLong(byte[] buffer,
                                     MutableInt offset)
  {
    int offsetInt = offset.intValue();
    long val = ((((long) buffer[0 + offsetInt]) & 0xFFL) << 56) |
           ((((long) buffer[1 + offsetInt]) & 0xFFL) << 48) |
           ((((long) buffer[2 + offsetInt]) & 0xFFL) << 40) |
           ((((long) buffer[3 + offsetInt]) & 0xFFL) << 32) |
           ((((long) buffer[4 + offsetInt]) & 0xFFL) << 24) |
           ((((long) buffer[5 + offsetInt]) & 0xFFL) << 16) |
           ((((long) buffer[6 + offsetInt]) & 0xFFL) << 8)  |
           (((long) buffer[7 + offsetInt]) & 0xFFL);

    offset.add(Type.LONG.getByteSize());
    return val;
  }

  /**
   * This method serializes the given long to the given byte buffer to the given offset,
   * the method also increments the offset appropriately.
   * @param val The value to serialize.
   * @param buffer The byte buffer to serialize to.
   * @param offset The offset in the buffer to serialize to and also to increment appropriately.
   */
  public static void serializeLong(long val,
                                   byte[] buffer,
                                   MutableInt offset)
  {
    int offsetInt = offset.intValue();
    buffer[0 + offsetInt] = (byte) ((val >> 56) & 0xFFL);
    buffer[1 + offsetInt] = (byte) ((val >> 48) & 0xFFL);
    buffer[2 + offsetInt] = (byte) ((val >> 40) & 0xFFL);
    buffer[3 + offsetInt] = (byte) ((val >> 32) & 0xFFL);
    buffer[4 + offsetInt] = (byte) ((val >> 24) & 0xFFL);
    buffer[5 + offsetInt] = (byte) ((val >> 16) & 0xFFL);
    buffer[6 + offsetInt] = (byte) ((val >> 8) & 0xFFL);
    buffer[7 + offsetInt] = (byte) (val & 0xFFL);

    offset.add(Type.LONG.getByteSize());
  }

  /**
   * Serializes the given long value to an array of bytes.
   * @param val The long value to serialize.
   * @return The serialized long value.
   */
  public static byte[] serializeLong(long val)
  {
    byte[] buffer = new byte[Type.LONG.getByteSize()];

    buffer[0] = (byte) ((val >> 56) & 0xFFL);
    buffer[1] = (byte) ((val >> 48) & 0xFFL);
    buffer[2] = (byte) ((val >> 40) & 0xFFL);
    buffer[3] = (byte) ((val >> 32) & 0xFFL);
    buffer[4] = (byte) ((val >> 24) & 0xFFL);
    buffer[5] = (byte) ((val >> 16) & 0xFFL);
    buffer[6] = (byte) ((val >> 8) & 0xFFL);
    buffer[7] = (byte) (val & 0xFFL);

    return buffer;
  }

  /**
   * Deserializes the given long value.
   * @param buffer A serialized long value.
   * @return The deserialized long value.
   */
  public static long deserializeLong(byte[] buffer)
  {
    Preconditions.checkArgument(buffer.length == Type.LONG.getByteSize());
    return deserializeLong(buffer, new MutableInt(0));
  }

  /**
   * This method deserializes a double from the given byte array from the given offset,
   * and increments the offset appropriately.
   * @param buffer The byte buffer to deserialize from.
   * @param offset The offset to deserialize from.
   * @return The deserialized double.
   */
  public static double deserializeDouble(byte[] buffer,
                                       MutableInt offset)
  {
    int offsetInt = offset.intValue();
    long val = (((long) buffer[0 + offsetInt]) & 0xFFL) << 56 |
           ((((long) buffer[1 + offsetInt]) & 0xFFL) << 48) |
           ((((long) buffer[2 + offsetInt]) & 0xFFL) << 40) |
           ((((long) buffer[3 + offsetInt]) & 0xFFL) << 32) |
           ((((long) buffer[4 + offsetInt]) & 0xFFL) << 24) |
           ((((long) buffer[5 + offsetInt]) & 0xFFL) << 16) |
           ((((long) buffer[6 + offsetInt]) & 0xFFL) << 8)  |
           (((long) buffer[7 + offsetInt]) & 0xFFL);

    offset.add(Type.DOUBLE.getByteSize());
    return Double.longBitsToDouble(val);
  }

  /**
   * This method serializes the given double to the given byte buffer to the given offset,
   * the method also increments the offset appropriately.
   * @param valD The value to serialize.
   * @param buffer The byte buffer to serialize to.
   * @param offset The offset in the buffer to serialize to and also to increment appropriately.
   */
  public static void serializeDouble(double valD,
                                   byte[] buffer,
                                   MutableInt offset)
  {
    long val = Double.doubleToLongBits(valD);

    int offsetInt = offset.intValue();
    buffer[0 + offsetInt] = (byte) ((val >> 56) & 0xFFL);
    buffer[1 + offsetInt] = (byte) ((val >> 48) & 0xFFL);
    buffer[2 + offsetInt] = (byte) ((val >> 40) & 0xFFL);
    buffer[3 + offsetInt] = (byte) ((val >> 32) & 0xFFL);
    buffer[4 + offsetInt] = (byte) ((val >> 24) & 0xFFL);
    buffer[5 + offsetInt] = (byte) ((val >> 16) & 0xFFL);
    buffer[6 + offsetInt] = (byte) ((val >> 8) & 0xFFL);
    buffer[7 + offsetInt] = (byte) (val & 0xFFL);

    offset.add(Type.DOUBLE.getByteSize());
  }

  public static byte[] serializeDouble(double valD)
  {
    byte[] buffer = new byte[Type.DOUBLE.getByteSize()];
    long val = Double.doubleToLongBits(valD);

    buffer[0] = (byte) ((val >> 56) & 0xFFL);
    buffer[1] = (byte) ((val >> 48) & 0xFFL);
    buffer[2] = (byte) ((val >> 40) & 0xFFL);
    buffer[3] = (byte) ((val >> 32) & 0xFFL);
    buffer[4] = (byte) ((val >> 24) & 0xFFL);
    buffer[5] = (byte) ((val >> 16) & 0xFFL);
    buffer[6] = (byte) ((val >> 8) & 0xFFL);
    buffer[7] = (byte) (val & 0xFFL);

    return buffer;
  }

  /**
   * This method deserializes an integer from the given byte array from the given offset,
   * and increments the offset appropriately.
   * @param buffer The byte buffer to deserialize from.
   * @param offset The offset to deserialize from.
   * @return The deserialized integer.
   */
  public static int deserializeInt(byte[] buffer,
                                   MutableInt offset)
  {
    int offsetInt = offset.intValue();
    int val = ((((int) buffer[0 + offsetInt]) & 0xFF) << 24) |
           ((((int) buffer[1 + offsetInt]) & 0xFF) << 16) |
           ((((int) buffer[2 + offsetInt]) & 0xFF) << 8)  |
           (((int) buffer[3 + offsetInt]) & 0xFF);

    offset.add(Type.INTEGER.getByteSize());
    return val;
  }

  /**
   * Deserializes the given serialized integer.
   * @param buffer The integer value to deserialized.
   * @return The deserialized integer value.
   */
  public static int deserializeInt(byte[] buffer)
  {
    Preconditions.checkArgument(buffer.length == Type.INTEGER.getByteSize());
    return deserializeInt(buffer, new MutableInt(0));
  }

  /**
   * This method serializes the given integer to the given byte buffer to the given offset,
   * the method also increments the offset appropriately.
   * @param val The value to serialize.
   * @param buffer The byte buffer to serialize to.
   * @param offset The offset in the buffer to serialize to and also to increment appropriately.
   */
  public static void serializeInt(int val,
                                  byte[] buffer,
                                  MutableInt offset)
  {
    int offsetInt = offset.intValue();
    buffer[0 + offsetInt] = (byte) ((val >> 24) & 0xFF);
    buffer[1 + offsetInt] = (byte) ((val >> 16) & 0xFF);
    buffer[2 + offsetInt] = (byte) ((val >> 8) & 0xFF);
    buffer[3 + offsetInt] = (byte) (val & 0xFF);

    offset.add(Type.INTEGER.getByteSize());
  }

  /**
   * Serializes the given integer value.
   * @param val The value to serialize.
   * @return The serialized integer value.
   */
  public static byte[] serializeInt(int val) {
    byte[] buffer = new byte[Type.INTEGER.getByteSize()];

    buffer[0] = (byte) ((val >> 24) & 0xFF);
    buffer[1] = (byte) ((val >> 16) & 0xFF);
    buffer[2] = (byte) ((val >> 8) & 0xFF);
    buffer[3] = (byte) (val & 0xFF);

    return buffer;
  }

  /**
   * This method deserializes a float from the given byte array from the given offset,
   * and increments the offset appropriately.
   * @param buffer The byte buffer to deserialize from.
   * @param offset The offset to deserialize from.
   * @return The deserialized float.
   */
  public static float deserializeFloat(byte[] buffer,
                                   MutableInt offset)
  {
    int offsetInt = offset.intValue();
    int val = ((((int) buffer[0 + offsetInt]) & 0xFF) << 24) |
           ((((int) buffer[1 + offsetInt]) & 0xFF) << 16) |
           ((((int) buffer[2 + offsetInt]) & 0xFF) << 8)  |
           (((int) buffer[3 + offsetInt]) & 0xFF);

    offset.add(Type.FLOAT.getByteSize());
    return Float.intBitsToFloat(val);
  }

  /**
   * This method serializes the given float to the given byte buffer to the given offset,
   * the method also increments the offset appropriately.
   * @param valf The value to serialize.
   * @param buffer The byte buffer to serialize to.
   * @param offset The offset in the buffer to serialize to and also to increment appropriately.
   */
  public static void serializeFloat(float valf,
                                  byte[] buffer,
                                  MutableInt offset)
  {
    int offsetInt = offset.intValue();
    int val = Float.floatToIntBits(valf);

    buffer[0 + offsetInt] = (byte) ((val >> 24) & 0xFF);
    buffer[1 + offsetInt] = (byte) ((val >> 16) & 0xFF);
    buffer[2 + offsetInt] = (byte) ((val >> 8) & 0xFF);
    buffer[3 + offsetInt] = (byte) (val & 0xFF);

    offset.add(Type.FLOAT.getByteSize());
  }

  public static byte[] serializeFloat(float valf)
  {
    byte[] buffer = new byte[Type.FLOAT.getByteSize()];
    int val = Float.floatToIntBits(valf);

    buffer[0] = (byte) ((val >> 24) & 0xFF);
    buffer[1] = (byte) ((val >> 16) & 0xFF);
    buffer[2] = (byte) ((val >> 8) & 0xFF);
    buffer[3] = (byte) (val & 0xFF);

    return buffer;
  }

  /**
   * This method deserializes a short from the given byte array from the given offset,
   * and increments the offset appropriately.
   * @param buffer The byte buffer to deserialize from.
   * @param offset The offset to deserialize from.
   * @return The deserialized short.
   */
  public static short deserializeShort(byte[] buffer,
                                       MutableInt offset)
  {
    int offsetInt = offset.intValue();
    short val = (short) (((((int) buffer[0 + offsetInt]) & 0xFF) << 8)  |
                (((int) buffer[1 + offsetInt]) & 0xFF));

    offset.add(Type.SHORT.getByteSize());
    return val;
  }

  /**
   * This method serializes the given short to the given byte buffer to the given offset,
   * the method also increments the offset appropriately.
   * @param val The value to serialize.
   * @param buffer The byte buffer to serialize to.
   * @param offset The offset in the buffer to serialize to and also to increment appropriately.
   */
  public static void serializeShort(short val,
                                    byte[] buffer,
                                    MutableInt offset)
  {
    int offsetInt = offset.intValue();
    buffer[0 + offsetInt] = (byte) ((val >> 8) & 0xFF);
    buffer[1 + offsetInt] = (byte) (val & 0xFF);

    offset.add(Type.SHORT.getByteSize());
  }

  public static byte[] serializeShort(short val)
  {
    byte[] buffer = new byte[Type.SHORT.getByteSize()];

    buffer[0] = (byte) ((val >> 8) & 0xFF);
    buffer[1] = (byte) (val & 0xFF);

    return buffer;
  }

  /**
   * This method deserializes a byte from the given byte array from the given offset,
   * and increments the offset appropriately.
   * @param buffer The byte buffer to deserialize from.
   * @param offset The offset to deserialize from.
   * @return The deserialized byte.
   */
  public static byte deserializeByte(byte[] buffer,
                                     MutableInt offset)
  {
    byte val = buffer[offset.intValue()];

    offset.add(Type.BYTE.getByteSize());
    return val;
  }


  /**
   * This method serializes the given byte to the given byte buffer to the given offset,
   * the method also increments the offset appropriately.
   * @param val The value to serialize.
   * @param buffer The byte buffer to serialize to.
   * @param offset The offset in the buffer to serialize to and also to increment appropriately.
   */
  public static void serializeByte(byte val,
                                   byte[] buffer,
                                   MutableInt offset)
  {
    buffer[offset.intValue()] = val;

    offset.add(Type.BYTE.getByteSize());
  }

  public static byte[] serializeByte(byte val)
  {
    return new byte[]{val};
  }

  /**
   * This method deserializes a boolean from the given byte array from the given offset,
   * and increments the offset appropriately.
   * @param buffer The byte buffer to deserialize from.
   * @param offset The offset to deserialize from.
   * @return The deserialized boolean.
   */
  public static boolean deserializeBoolean(byte[] buffer,
                                           MutableInt offset)
  {
    boolean val = buffer[offset.intValue()] != 0;

    offset.add(Type.BOOLEAN.getByteSize());
    return val;
  }

  /**
   * This method serializes the given boolean to the given byte buffer to the given offset,
   * the method also increments the offset appropriately.
   * @param val The value to serialize.
   * @param buffer The byte buffer to serialize to.
   * @param offset The offset in the buffer to serialize to and also to increment appropriately.
   */
  public static void serializeBoolean(boolean val,
                                      byte[] buffer,
                                      MutableInt offset)
  {
    buffer[offset.intValue()] = (byte) (val ? 1: 0);

    offset.add(Type.BOOLEAN.getByteSize());
  }

  public static byte[] serializeBoolean(boolean val)
  {
    return new byte[]{(byte) (val ? 1: 0)};
  }

  /**
   * This method deserializes a character from the given byte array from the given offset,
   * and increments the offset appropriately.
   * @param buffer The byte buffer to deserialize from.
   * @param offset The offset to deserialize from.
   * @return The deserialized character.
   */
  public static char deserializeChar(byte[] buffer,
                                     MutableInt offset)
  {
    int offsetInt = offset.intValue();
    char val = (char) (((((int) buffer[0 + offsetInt]) & 0xFF) << 8)  |
                (((int) buffer[1 + offsetInt]) & 0xFF));

    offset.add(Type.CHAR.getByteSize());
    return val;
  }

  /**
   * This method serializes the given character to the given byte buffer to the given offset,
   * the method also increments the offset appropriately.
   * @param val The value to serialize.
   * @param buffer The byte buffer to serialize to.
   * @param offset The offset in the buffer to serialize to and also to increment appropriately.
   */
  public static void serializeChar(char val,
                                   byte[] buffer,
                                   MutableInt offset)
  {
    int offsetInt = offset.intValue();
    buffer[0 + offsetInt] = (byte) ((val >> 8) & 0xFF);
    buffer[1 + offsetInt] = (byte) (val & 0xFF);

    offset.add(Type.CHAR.getByteSize());
  }

  public static byte[] serializeChar(char val)
  {
    byte[] buffer = new byte[Type.CHAR.getByteSize()];

    buffer[0] = (byte) ((val >> 8) & 0xFF);
    buffer[1] = (byte) (val & 0xFF);

    return buffer;
  }

  /**
   * Utility method for creating getters. This method is useful for creating a {@link GPOGetters} object
   * which can be used to copy POJOs into GPOMutable objects.
   * @param fields The fields to create getters for. The order of the fields in this list will be the same order
   * that the getters will be returned in.
   * @param valueToExpression A map from field names to the corresponding java expression to be used for getting
   * the fields.
   * @param clazz The Class of the POJO to extract values from.
   * @param getterClazz The class of the getter object to create.
   * @param getterMethodClazz The class of the getter method.
   * @return An array of boolean getters for given fields.
   */
  @SuppressWarnings("unchecked")
  public static <T> T[] createGetters(List<String> fields,
                                      Map<String, String> valueToExpression,
                                      Class<?> clazz,
                                      Class<?> getterClazz,
                                      Class<?> getterMethodClazz)
  {
    @SuppressWarnings("unchecked")
    T[] getters = (T[])Array.newInstance(getterMethodClazz, fields.size());

    for(int getterIndex = 0;
        getterIndex < fields.size();
        getterIndex++) {
      String field = fields.get(getterIndex);
      getters[getterIndex] = (T) PojoUtils.constructGetter(clazz, valueToExpression.get(field), getterClazz);
    }

    return getters;
  }

  /**
   * Utility method for creating getters. This method is useful for creating a {@link GPOGetters} object
   * which can be used to copy POJOs into GPOMutable objects.
   * @param fields The fields to create getters for. The order of the fields in this list will be the same order
   * that the getters will be returned in.
   * @param valueToExpression A map from field names to the corresponding java expression to be used for getting
   * the fields.
   * @param clazz The Class of the POJO to extract values from.
   * @return An array of boolean getters for given fields.
   */
  public static Getter<Object, String>[] createGettersString(List<String> fields,
                                                             Map<String, String> valueToExpression,
                                                             Class<?> clazz)
  {
    @SuppressWarnings({"unchecked","rawtypes"})
    Getter<Object, String>[] getters = new Getter[fields.size()];

    for(int getterIndex = 0;
        getterIndex < fields.size();
        getterIndex++) {
      String field = fields.get(getterIndex);
      getters[getterIndex] = PojoUtils.createGetter(clazz, valueToExpression.get(field), String.class);
    }

    return getters;
  }

  /**
   * Utility method for creating getters. This method is useful for creating a {@link GPOGetters} object
   * which can be used to copy POJOs into GPOMutable objects.
   * @param fields The fields to create getters for. The order of the fields in this list will be the same order
   * that the getters will be returned in.
   * @param valueToExpression A map from field names to the corresponding java expression to be used for getting
   * the fields.
   * @param clazz The Class of the POJO to extract values from.
   * @return An array of boolean getters for given fields.
   */
  public static Getter<Object, Object>[] createGettersObject(List<String> fields,
                                                             Map<String, String> valueToExpression,
                                                             Class<?> clazz)
  {
    @SuppressWarnings({"unchecked","rawtypes"})
    Getter<Object, Object>[] getters = new Getter[fields.size()];

    for(int getterIndex = 0;
        getterIndex < fields.size();
        getterIndex++) {
      String field = fields.get(getterIndex);
      getters[getterIndex] = PojoUtils.createGetter(clazz, valueToExpression.get(field), Object.class);
    }

    return getters;
  }

  /**
   * This is a utility method which builds a {@link GPOGetters} object corresponding to the given {@link FieldsDescriptor}.
   * This utility method is helpful for converting POJOs into GPOMutable objects.
   * @param fieldToGetter A map whose keys are field names and whose values correspond to java getter expressions. Field names
   * in this map should be the same as field names in the provided {@link FieldsDescriptor} object.
   * @param fieldsDescriptor A {@link FieldsDescriptor} object which describes the type name and order of fields.
   * @param clazz The Class of the POJO that the getters will be applied to.
   * @return The {@link GPOGetters} object which can be used to convert POJOs into {@link GPOMutable} objects initialized
   * with the same {@link FieldsDescriptor} object.
   */
  public static GPOGetters buildGPOGetters(Map<String, String> fieldToGetter,
                                           FieldsDescriptor fieldsDescriptor,
                                           Class<?> clazz)
  {
    GPOGetters gpoGetters = new GPOGetters();
    Map<Type, List<String>> typeToFields = fieldsDescriptor.getTypeToFields();

    for(Map.Entry<Type, List<String>> entry: typeToFields.entrySet()) {
      Type inputType = entry.getKey();
      GPOType gpoType = GPOType.GPO_TYPE_ARRAY[inputType.ordinal()];
      List<String> fields = entry.getValue();
      gpoType.buildGPOGetters(gpoGetters, fields, fieldToGetter, clazz);
    }

    return gpoGetters;
  }

  /**
   * This is a utility method for converting a POJO to a {@link GPOMutable} object. This method assumes that the provided
   * GPOMutable is initialized with the correct {@link FieldsDescriptor}, and that the given {@link GPOGetters} object has getters
   * specified in the same order as fields in the {@link GPOMutable} object.
   * @param mutable The {@link GPOMutable} object to copy POJO values into. It is assumed this object is initialized with
   * the correct {@link FieldsDescriptor}.
   * @param getters The getters to use when retrieving values from the provided POJO. It is assumed that the getters are
   * specified in the same order as their corresponding fields in the {@link GPOMutable} object.
   * @param object The POJO to extract values from.
   */
  public static void copyPOJOToGPO(GPOMutable mutable, GPOGetters getters, Object object)
  {
    {
      boolean[] tempBools = mutable.getFieldsBoolean();
      GetterBoolean<Object>[] tempGetterBools = getters.gettersBoolean;

      if(tempBools != null) {
        for(int index = 0;
            index < tempBools.length;
            index++) {
          tempBools[index] = tempGetterBools[index].get(object);
        }
      }
    }

    {
      byte[] tempBytes = mutable.getFieldsByte();
      GetterByte<Object>[] tempGetterByte = getters.gettersByte;

      if(tempBytes != null) {
        for(int index = 0;
            index < tempBytes.length;
            index++) {
          tempBytes[index] = tempGetterByte[index].get(object);
        }
      }
    }

    {
      char[] tempChar = mutable.getFieldsCharacter();
      GetterChar<Object>[] tempGetterChar = getters.gettersChar;

      if(tempChar != null) {
        for(int index = 0;
            index < tempChar.length;
            index++) {
          tempChar[index] = tempGetterChar[index].get(object);
        }
      }
    }

    {
      double[] tempDouble = mutable.getFieldsDouble();
      GetterDouble<Object>[] tempGetterDouble = getters.gettersDouble;

      if(tempDouble != null) {
        for(int index = 0;
            index < tempDouble.length;
            index++) {
          tempDouble[index] = tempGetterDouble[index].get(object);
        }
      }
    }

    {
      float[] tempFloat = mutable.getFieldsFloat();
      GetterFloat<Object>[] tempGetterFloat = getters.gettersFloat;

      if(tempFloat != null) {
        for(int index = 0;
            index < tempFloat.length;
            index++) {
          tempFloat[index] = tempGetterFloat[index].get(object);
        }
      }
    }

    {
      int[] tempInt = mutable.getFieldsInteger();
      GetterInt<Object>[] tempGetterInt = getters.gettersInteger;

      if(tempInt != null) {
        for(int index = 0;
            index < tempInt.length;
            index++) {
          tempInt[index] = tempGetterInt[index].get(object);
        }
      }
    }

    {
      long[] tempLong = mutable.getFieldsLong();
      GetterLong<Object>[] tempGetterLong = getters.gettersLong;

      if(tempLong != null) {
        for(int index = 0;
            index < tempLong.length;
            index++) {
          tempLong[index] = tempGetterLong[index].get(object);
        }
      }
    }

    {
      short[] tempShort = mutable.getFieldsShort();
      GetterShort<Object>[] tempGetterShort = getters.gettersShort;

      if(tempShort != null) {
        for(int index = 0;
            index < tempShort.length;
            index++) {
          tempShort[index] = tempGetterShort[index].get(object);
        }
      }
    }

    {
      String[] tempString = mutable.getFieldsString();
      Getter<Object, String>[] tempGetterString = getters.gettersString;

      if(tempString != null) {
        for(int index = 0;
            index < tempString.length;
            index++) {
          tempString[index] = tempGetterString[index].get(object);
        }
      }
    }
  }

  public static void indirectCopy(GPOMutable dest,
                                  GPOMutable src,
                                  IndexSubset indexSubset)
  {
    {
      String[] destString = dest.getFieldsString();
      String[] srcString = src.getFieldsString();
      int[] srcIndex = indexSubset.fieldsStringIndexSubset;
      if(destString != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          destString[index] = srcString[srcIndex[index]];
        }
      }
    }

    {
      boolean[] destBoolean = dest.getFieldsBoolean();
      boolean[] srcBoolean = src.getFieldsBoolean();
      int[] srcIndex = indexSubset.fieldsBooleanIndexSubset;
      if(destBoolean != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          destBoolean[index] = srcBoolean[srcIndex[index]];
        }
      }
    }

    {
      char[] destChar = dest.getFieldsCharacter();
      char[] srcChar = src.getFieldsCharacter();
      int[] srcIndex = indexSubset.fieldsBooleanIndexSubset;
      if(destChar != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          destChar[index] = srcChar[srcIndex[index]];
        }
      }
    }

    {
      byte[] destByte = dest.getFieldsByte();
      byte[] srcByte = src.getFieldsByte();
      int[] srcIndex = indexSubset.fieldsByteIndexSubset;
      if(destByte != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          destByte[index] = srcByte[srcIndex[index]];
        }
      }
    }

    {
      short[] destShort = dest.getFieldsShort();
      short[] srcShort = src.getFieldsShort();
      int[] srcIndex = indexSubset.fieldsShortIndexSubset;
      if(destShort != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          destShort[index] = srcShort[srcIndex[index]];
        }
      }
    }

    {
      int[] destInteger = dest.getFieldsInteger();
      int[] srcInteger = src.getFieldsInteger();
      int[] srcIndex = indexSubset.fieldsIntegerIndexSubset;
      if(destInteger != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          destInteger[index] = srcInteger[srcIndex[index]];
        }
      }
    }

    {
      long[] destLong = dest.getFieldsLong();
      long[] srcLong = src.getFieldsLong();
      int[] srcIndex = indexSubset.fieldsLongIndexSubset;
      if(destLong != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }

          destLong[index] = srcLong[srcIndex[index]];
        }
      }
    }

    {
      float[] destFloat = dest.getFieldsFloat();
      float[] srcFloat = src.getFieldsFloat();
      int[] srcIndex = indexSubset.fieldsFloatIndexSubset;
      if(destFloat != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          destFloat[index] = srcFloat[srcIndex[index]];
        }
      }
    }

    {
      double[] destDouble = dest.getFieldsDouble();
      double[] srcDouble = src.getFieldsDouble();
      int[] srcIndex = indexSubset.fieldsDoubleIndexSubset;
      if(destDouble != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          destDouble[index] = srcDouble[srcIndex[index]];
        }
      }
    }

    {
      Object[] destObject = dest.getFieldsObject();
      Object[] srcObject = src.getFieldsObject();
      int[] srcIndex = indexSubset.fieldsObjectIndexSubset;
      if(destObject != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          destObject[index] = srcObject[srcIndex[index]];
        }
      }
    }
  }

  public static int hashcode(GPOMutable gpo)
  {
    int hashCode = 0;

    {
      String[] stringArray = gpo.getFieldsString();
      if(stringArray != null) {
        for(int index = 0;
            index < stringArray.length;
            index++) {
          hashCode ^= stringArray[index].hashCode();
        }
      }
    }

    {
      boolean[] booleanArray = gpo.getFieldsBoolean();
      if(booleanArray != null) {
        for(int index = 0;
            index < booleanArray.length;
            index++) {
          hashCode ^= booleanArray[index] ? 1: 0;
        }
      }
    }

    {
      char[] charArray = gpo.getFieldsCharacter();
      if(charArray != null) {
        for(int index = 0;
            index < charArray.length;
            index++) {
          hashCode ^= Character.getNumericValue(charArray[index]);
        }
      }
    }

    {
      byte[] byteArray = gpo.getFieldsByte();
      if(byteArray != null) {
        for(int index = 0;
            index < byteArray.length;
            index++) {
          hashCode ^= byteArray[index];
        }
      }
    }

    {
      short[] shortArray = gpo.getFieldsShort();
      if(shortArray != null) {
        for(int index = 0;
            index < shortArray.length;
            index++) {
          hashCode ^= shortArray[index];
        }
      }
    }

    {
      int[] integerArray = gpo.getFieldsInteger();
      if(integerArray != null) {
        for(int index = 0;
            index < integerArray.length;
            index++) {
          hashCode ^= integerArray[index];
        }
      }
    }

    {
      long[] longArray = gpo.getFieldsLong();
      if(longArray != null) {
        for(int index = 0;
            index < longArray.length;
            index++) {
          hashCode ^= longArray[index];
        }
      }
    }

    {
      float[] floatArray = gpo.getFieldsFloat();
      if(floatArray != null) {
        for(int index = 0;
            index < floatArray.length;
            index++) {
          hashCode ^= Float.floatToIntBits(floatArray[index]);
        }
      }
    }

    {
      double[] doubleArray = gpo.getFieldsDouble();
      if(doubleArray != null) {
        for(int index = 0;
            index < doubleArray.length;
            index++) {
          hashCode ^= Double.doubleToLongBits(doubleArray[index]);
        }
      }
    }

    {
      Object[] objectArray = gpo.getFieldsObject();
      if(objectArray != null) {
        for(int index = 0;
            index < objectArray.length;
            index++) {
          hashCode ^= objectArray[index].hashCode();
        }
      }
    }

    return hashCode;
  }

  public static int indirectHashcode(GPOMutable gpo,
                                     IndexSubset indexSubset)
  {
    int hashCode = 0;

    {
      String[] stringArray = gpo.getFieldsString();
      int[] srcIndex = indexSubset.fieldsStringIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          hashCode ^= stringArray[srcIndex[index]].hashCode();
        }
      }
    }

    {
      boolean[] booleanArray = gpo.getFieldsBoolean();
      int[] srcIndex = indexSubset.fieldsBooleanIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          hashCode ^= booleanArray[srcIndex[index]] ? 1: 0;
        }
      }
    }

    {
      char[] charArray = gpo.getFieldsCharacter();
      int[] srcIndex = indexSubset.fieldsCharacterIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          hashCode ^= Character.getNumericValue(charArray[srcIndex[index]]);
        }
      }
    }

    {
      byte[] byteArray = gpo.getFieldsByte();
      int[] srcIndex = indexSubset.fieldsByteIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          hashCode ^= byteArray[srcIndex[index]];
        }
      }
    }

    {
      short[] shortArray = gpo.getFieldsShort();
      int[] srcIndex = indexSubset.fieldsShortIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          hashCode ^= shortArray[srcIndex[index]];
        }
      }
    }

    {
      int[] integerArray = gpo.getFieldsInteger();
      int[] srcIndex = indexSubset.fieldsIntegerIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          hashCode ^= integerArray[srcIndex[index]];
        }
      }
    }

    {
      long[] longArray = gpo.getFieldsLong();
      int[] srcIndex = indexSubset.fieldsLongIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          hashCode ^= longArray[srcIndex[index]];
        }
      }
    }

    {
      float[] floatArray = gpo.getFieldsFloat();
      int[] srcIndex = indexSubset.fieldsFloatIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          hashCode ^= Float.floatToIntBits(floatArray[srcIndex[index]]);
        }
      }
    }

    {
      double[] doubleArray = gpo.getFieldsDouble();
      int[] srcIndex = indexSubset.fieldsDoubleIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          hashCode ^= Double.doubleToLongBits(doubleArray[srcIndex[index]]);
        }
      }
    }

    {
      Object[] objectArray = gpo.getFieldsObject();
      int[] srcIndex = indexSubset.fieldsObjectIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          hashCode ^= objectArray[srcIndex[index]].hashCode();
        }
      }
    }

    return hashCode;
  }

  public static boolean equals(GPOMutable dest,
                               GPOMutable src)
  {
    {
      String[] destString = dest.getFieldsString();
      String[] srcString = src.getFieldsString();
      if(destString != null) {
        for(int index = 0;
            index < srcString.length;
            index++) {
          if(!destString[index].equals(srcString[index])) {
            return false;
          }
        }
      }
    }

    {
      boolean[] destBoolean = dest.getFieldsBoolean();
      boolean[] srcBoolean = src.getFieldsBoolean();
      if(destBoolean != null) {
        for(int index = 0;
            index < srcBoolean.length;
            index++) {
          if(destBoolean[index] != srcBoolean[index]) {
            return false;
          }
        }
      }
    }

    {
      char[] destChar = dest.getFieldsCharacter();
      char[] srcChar = src.getFieldsCharacter();
      if(destChar != null) {
        for(int index = 0;
            index < srcChar.length;
            index++) {
          if(destChar[index] != srcChar[index]) {
            return false;
          }
        }
      }
    }

    {
      byte[] destByte = dest.getFieldsByte();
      byte[] srcByte = src.getFieldsByte();
      if(destByte != null) {
        for(int index = 0;
            index < srcByte.length;
            index++) {
          if(destByte[index] != srcByte[index]) {
            return false;
          }
        }
      }
    }

    {
      short[] destShort = dest.getFieldsShort();
      short[] srcShort = src.getFieldsShort();
      if(destShort != null) {
        for(int index = 0;
            index < srcShort.length;
            index++) {
          if(destShort[index] != srcShort[index]) {
            return false;
          }
        }
      }
    }

    {
      int[] destInteger = dest.getFieldsInteger();
      int[] srcInteger = src.getFieldsInteger();
      if(destInteger != null) {
        for(int index = 0;
            index < srcInteger.length;
            index++) {
          if(destInteger[index] != srcInteger[index]) {
            return false;
          }
        }
      }
    }

    {
      long[] destLong = dest.getFieldsLong();
      long[] srcLong = src.getFieldsLong();
      if(destLong != null) {
        for(int index = 0;
            index < srcLong.length;
            index++) {
          if(destLong[index] != srcLong[index]) {
            return false;
          }
        }
      }
    }

    {
      float[] destFloat = dest.getFieldsFloat();
      float[] srcFloat = src.getFieldsFloat();
      if(destFloat != null) {
        for(int index = 0;
            index < srcFloat.length;
            index++) {
          if(destFloat[index] != srcFloat[index]) {
            return false;
          }
        }
      }
    }

    {
      double[] destDouble = dest.getFieldsDouble();
      double[] srcDouble = src.getFieldsDouble();
      if(destDouble != null) {
        for(int index = 0;
            index < srcDouble.length;
            index++) {
          if(destDouble[index] != srcDouble[index]) {
            return false;
          }
        }
      }
    }

    {
      Object[] destObject = dest.getFieldsObject();
      Object[] srcObject = src.getFieldsObject();
      if(destObject != null) {
        for(int index = 0;
            index < srcObject.length;
            index++) {
          if(!destObject[index].equals(srcObject[index])) {
            return false;
          }
        }
      }
    }

    return true;
  }

  public static boolean subsetEquals(GPOMutable dest,
                                     GPOMutable src,
                                     IndexSubset indexSubset)
  {
    {
      String[] destString = dest.getFieldsString();
      String[] srcString = src.getFieldsString();
      int[] srcIndex = indexSubset.fieldsStringIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(!destString[srcIndex[index]].equals(srcString[srcIndex[index]])) {
            return false;
          }
        }
      }
    }

    {
      boolean[] destBoolean = dest.getFieldsBoolean();
      boolean[] srcBoolean = src.getFieldsBoolean();
      int[] srcIndex = indexSubset.fieldsBooleanIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(destBoolean[srcIndex[index]] != srcBoolean[srcIndex[index]]) {
            return false;
          }
        }
      }
    }

    {
      char[] destChar = dest.getFieldsCharacter();
      char[] srcChar = src.getFieldsCharacter();
      int[] srcIndex = indexSubset.fieldsBooleanIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(destChar[srcIndex[index]] != srcChar[srcIndex[index]]) {
            return false;
          }
        }
      }
    }

    {
      byte[] destByte = dest.getFieldsByte();
      byte[] srcByte = src.getFieldsByte();
      int[] srcIndex = indexSubset.fieldsByteIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(destByte[srcIndex[index]] != srcByte[srcIndex[index]]) {
            return false;
          }
        }
      }
    }

    {
      short[] destShort = dest.getFieldsShort();
      short[] srcShort = src.getFieldsShort();
      int[] srcIndex = indexSubset.fieldsShortIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(destShort[srcIndex[index]] != srcShort[srcIndex[index]]) {
            return false;
          }
        }
      }
    }

    {
      int[] destInteger = dest.getFieldsInteger();
      int[] srcInteger = src.getFieldsInteger();
      int[] srcIndex = indexSubset.fieldsIntegerIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(destInteger[srcIndex[index]] != srcInteger[srcIndex[index]]) {
            return false;
          }
        }
      }
    }

    {
      long[] destLong = dest.getFieldsLong();
      long[] srcLong = src.getFieldsLong();
      int[] srcIndex = indexSubset.fieldsLongIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(destLong[srcIndex[index]] != srcLong[srcIndex[index]]) {
            return false;
          }
        }
      }
    }

    {
      float[] destFloat = dest.getFieldsFloat();
      float[] srcFloat = src.getFieldsFloat();
      int[] srcIndex = indexSubset.fieldsFloatIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(destFloat[srcIndex[index]] != srcFloat[srcIndex[index]]) {
            return false;
          }
        }
      }
    }

    {
      double[] destDouble = dest.getFieldsDouble();
      double[] srcDouble = src.getFieldsDouble();
      int[] srcIndex = indexSubset.fieldsDoubleIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(destDouble[srcIndex[index]] != srcDouble[srcIndex[index]]) {
            return false;
          }
        }
      }
    }

    {
      Object[] destObject = dest.getFieldsObject();
      Object[] srcObject = src.getFieldsObject();
      int[] srcIndex = indexSubset.fieldsObjectIndexSubset;
      if(srcIndex != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(!destObject[srcIndex[index]].equals(srcObject[srcIndex[index]])) {
            return false;
          }
        }
      }
    }

    return true;
  }

  public static boolean indirectEquals(GPOMutable dest,
                                       GPOMutable src,
                                       IndexSubset indexSubset)
  {
    {
      String[] destString = dest.getFieldsString();
      String[] srcString = src.getFieldsString();
      int[] srcIndex = indexSubset.fieldsStringIndexSubset;
      if(destString != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(!destString[index].equals(srcString[srcIndex[index]])) {
            return false;
          }
        }
      }
    }

    {
      boolean[] destBoolean = dest.getFieldsBoolean();
      boolean[] srcBoolean = src.getFieldsBoolean();
      int[] srcIndex = indexSubset.fieldsBooleanIndexSubset;
      if(destBoolean != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(destBoolean[index] != srcBoolean[srcIndex[index]]) {
            return false;
          }
        }
      }
    }

    {
      char[] destChar = dest.getFieldsCharacter();
      char[] srcChar = src.getFieldsCharacter();
      int[] srcIndex = indexSubset.fieldsBooleanIndexSubset;
      if(destChar != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(destChar[index] != srcChar[srcIndex[index]]) {
            return false;
          }
        }
      }
    }

    {
      byte[] destByte = dest.getFieldsByte();
      byte[] srcByte = src.getFieldsByte();
      int[] srcIndex = indexSubset.fieldsByteIndexSubset;
      if(destByte != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(destByte[index] != srcByte[srcIndex[index]]) {
            return false;
          }
        }
      }
    }

    {
      short[] destShort = dest.getFieldsShort();
      short[] srcShort = src.getFieldsShort();
      int[] srcIndex = indexSubset.fieldsShortIndexSubset;
      if(destShort != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(destShort[index] != srcShort[srcIndex[index]]) {
            return false;
          }
        }
      }
    }

    {
      int[] destInteger = dest.getFieldsInteger();
      int[] srcInteger = src.getFieldsInteger();
      int[] srcIndex = indexSubset.fieldsIntegerIndexSubset;
      if(destInteger != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(destInteger[index] != srcInteger[srcIndex[index]]) {
            return false;
          }
        }
      }
    }

    {
      long[] destLong = dest.getFieldsLong();
      long[] srcLong = src.getFieldsLong();
      int[] srcIndex = indexSubset.fieldsLongIndexSubset;
      if(destLong != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(destLong[index] != srcLong[srcIndex[index]]) {
            return false;
          }
        }
      }
    }

    {
      float[] destFloat = dest.getFieldsFloat();
      float[] srcFloat = src.getFieldsFloat();
      int[] srcIndex = indexSubset.fieldsFloatIndexSubset;
      if(destFloat != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(destFloat[index] != srcFloat[srcIndex[index]]) {
            return false;
          }
        }
      }
    }

    {
      double[] destDouble = dest.getFieldsDouble();
      double[] srcDouble = src.getFieldsDouble();
      int[] srcIndex = indexSubset.fieldsDoubleIndexSubset;
      if(destDouble != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(destDouble[index] != srcDouble[srcIndex[index]]) {
            return false;
          }
        }
      }
    }

    {
      Object[] destObject = dest.getFieldsObject();
      Object[] srcObject = src.getFieldsObject();
      int[] srcIndex = indexSubset.fieldsObjectIndexSubset;
      if(destObject != null) {
        for(int index = 0;
            index < srcIndex.length;
            index++) {
          if(srcIndex[index] == -1) {
            continue;
          }
          if(!destObject[index].equals(srcObject[srcIndex[index]])) {
            return false;
          }
        }
      }
    }

    return true;
  }

  public static void zeroFillNumeric(GPOMutable value)
  {
    if(value.getFieldsByte() != null) {
      Arrays.fill(value.getFieldsByte(), (byte) 0);
    }

    if(value.getFieldsShort() != null) {
      Arrays.fill(value.getFieldsShort(), (short) 0);
    }

    if(value.getFieldsInteger() != null) {
      Arrays.fill(value.getFieldsInteger(), 0);
    }

    if(value.getFieldsLong() != null) {
      Arrays.fill(value.getFieldsLong(), 0L);
    }

    if(value.getFieldsFloat() != null) {
      Arrays.fill(value.getFieldsFloat(), 0.0f);
    }

    if(value.getFieldsDouble() != null) {
      Arrays.fill(value.getFieldsDouble(), 0.0);
    }
  }

  public static IndexSubset computeSubIndices(FieldsDescriptor child,
                                              FieldsDescriptor parent)
  {
    IndexSubset indexSubset = new IndexSubset();

    for(Map.Entry<Type, List<String>> entry: child.getTypeToFields().entrySet()) {
      Type type = entry.getKey();
      List<String> childFields = entry.getValue();
      List<String> parentFields = parent.getTypeToFields().get(type);

      int size = child.getTypeToSize().get(type);
      int[] indices;
      if(child.getTypeToFields().get(type) != null &&
         child.getCompressedTypes().contains(type)) {
        indices = new int[1];
      }
      else {
        indices = new int[size];

        for(int index = 0;
            index < size;
            index++) {
          if(parentFields == null) {
            indices[index] = -1;
          }
          else {
            indices[index] = parentFields.indexOf(childFields.get(index));
          }
        }
      }

      switch(type) {
        case BOOLEAN: {
          indexSubset.fieldsBooleanIndexSubset = indices;
          break;
        }
        case CHAR: {
          indexSubset.fieldsCharacterIndexSubset = indices;
          break;
        }
        case STRING: {
          indexSubset.fieldsStringIndexSubset = indices;
          break;
        }
        case BYTE: {
          indexSubset.fieldsByteIndexSubset = indices;
          break;
        }
        case SHORT: {
          indexSubset.fieldsShortIndexSubset = indices;
          break;
        }
        case INTEGER: {
          indexSubset.fieldsIntegerIndexSubset = indices;
          break;
        }
        case LONG: {
          indexSubset.fieldsLongIndexSubset = indices;
          break;
        }
        case FLOAT: {
          indexSubset.fieldsFloatIndexSubset = indices;
          break;
        }
        case DOUBLE: {
          indexSubset.fieldsDoubleIndexSubset = indices;
          break;
        }
        default: {
          throw new UnsupportedOperationException("Type " + type);
        }
      }
    }

    return indexSubset;
  }

  /**
   * This object is used to define the subset of data contained in a {@link GPOMutable}
   * object that we are interested in.
   */
  public static class IndexSubset implements Serializable
  {
    private static final long serialVersionUID = 201506251015L;

    /**
     * Index of boolean fields of interest. Null if there are no boolean fields of interest.
     */
    public int[] fieldsBooleanIndexSubset;
    /**
     * Index of character fields of interest. Null if there are no character fields of interest.
     */
    public int[] fieldsCharacterIndexSubset;
    /**
     * Index of byte fields of interest. Null if there are no byte fields of interest.
     */
    public int[] fieldsByteIndexSubset;
    /**
     * Index of short fields of interest. Null if there are no short fields of interest.
     */
    public int[] fieldsShortIndexSubset;
    /**
     * Index of integer fields of interest. Null if there are no integer fields of interest.
     */
    public int[] fieldsIntegerIndexSubset;
    /**
     * Index of long fields of interest. Null if there are no long fields of interest.
     */
    public int[] fieldsLongIndexSubset;
    /**
     * Index of float fields of interest. Null if there are no float fields of interest.
     */
    public int[] fieldsFloatIndexSubset;
    /**
     * Index of double fields of interest. Null if there are no double fields of interest.
     */
    public int[] fieldsDoubleIndexSubset;
    /**
     * Index of String fields of interest. Null if there are no String fields of interest.
     */
    public int[] fieldsStringIndexSubset;
    /**
     * Index of Object fields of interest. Null if there are no Object fields of interest.
     */
    public int[] fieldsObjectIndexSubset;

    public IndexSubset()
    {
      //Do nothing
    }

    @Override
    public String toString()
    {
      return "IndexSubset{" + "fieldsBooleanIndexSubset=" + fieldsBooleanIndexSubset + ", fieldsCharacterIndexSubset=" + fieldsCharacterIndexSubset + ", fieldsByteIndexSubset=" + fieldsByteIndexSubset + ", fieldsShortIndexSubset=" + fieldsShortIndexSubset + ", fieldsIntegerIndexSubset=" + fieldsIntegerIndexSubset + ", fieldsLongIndexSubset=" + fieldsLongIndexSubset + ", fieldsFloatIndexSubset=" + fieldsFloatIndexSubset + ", fieldsDoubleIndexSubset=" + fieldsDoubleIndexSubset + ", fieldsStringIndexSubset=" + fieldsStringIndexSubset + '}';
    }
  }

  public static Map<String, Object> getDestringedData(FieldsDescriptor fd,
                                                      Map<String, String> stringMap)
  {
    Map<String, Object> fieldToData = Maps.newHashMap();
    Map<String, Type> fieldToType = fd.getFieldToType();

    for(Map.Entry<String, String> entry: stringMap.entrySet()) {
      Object objValue;
      String valueString = entry.getValue();
      Type valueType = fieldToType.get(entry.getKey());

      switch(valueType) {
        case BOOLEAN:
        {
          objValue = Boolean.valueOf(valueString);
          break;
        }
        case BYTE:
        {
          objValue = Byte.valueOf(valueString);
          break;
        }
        case SHORT:
        {
          objValue = Short.valueOf(valueString);
          break;
        }
        case INTEGER:
        {
          objValue = Integer.valueOf(valueString);
          break;
        }
        case LONG:
        {
          objValue = Long.valueOf(valueString);
          break;
        }
        case FLOAT:
        {
          objValue = Float.valueOf(valueString);
          break;
        }
        case DOUBLE:
        {
          objValue = Double.valueOf(valueString);
          break;
        }
        case STRING:
        {
          objValue = valueString;
          break;
        }
        case OBJECT:
          throw new UnsupportedOperationException("The given type " + entry.getValue() + " is unsupported.");
        default:
          throw new UnsupportedOperationException("The given type " + entry.getValue() + " is unsupported.");
      }

      fieldToData.put(entry.getKey(), objValue);
    }

    return fieldToData;
  }

  public static Map<String, Object> convertToMapIntersection(GPOMutable gpo, Fields fields)
  {
    Map<String, Object> values = Maps.newHashMap();

    for(String field: fields.getFields()) {
      if(!gpo.getFieldDescriptor().getFields().getFields().contains(field)) {
        continue;
      }

      Object valueObj = gpo.getField(field);
      values.put(field, valueObj);
    }

    return values;
  }
}
