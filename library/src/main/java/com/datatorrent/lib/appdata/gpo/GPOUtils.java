/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.gpo;

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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.mutable.MutableInt;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    switch(type) {
      case BOOLEAN: {
        Boolean val;

        try {
          val = jo.getBoolean(index);
        }
        catch(JSONException ex) {
          throw new IllegalArgumentException("The key " + field + " does not have a valid bool value.", ex);
        }

        gpo.setField(field, val);
        break;
      }
      case BYTE: {
        int val;

        try {
          val = jo.getInt(index);
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
        break;
      }
      case SHORT: {
        int val;

        try {
          val = jo.getInt(index);
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
        break;
      }
      case INTEGER: {
        int val;

        try {
          val = jo.getInt(index);
        }
        catch(JSONException ex) {
          throw new IllegalArgumentException("The key "
                                             + field
                                             + " does not have a valid int value.",
                                             ex);
        }

        gpo.setField(field, val);
        break;
      }
      case LONG: {
        long val;

        try {
          val = jo.getLong(index);
        }
        catch(JSONException ex) {
          throw new IllegalArgumentException("The key "
                                             + field
                                             + " does not have a valid long value.",
                                             ex);
        }

        gpo.setField(field, val);
        break;
      }
      case CHAR: {
        String val;

        try {
          val = jo.getString(index);
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
        break;
      }
      case STRING: {
        String val;

        try {
          val = jo.getString(index);
        }
        catch(JSONException ex) {
          throw new IllegalArgumentException("The key "
                                             + field
                                             + " does not have a valid string value.",
                                             ex);
        }

        gpo.setField(field, val);
        break;
      }
      case DOUBLE: {
        Double val;

        try {
          val = jo.getDouble(index);
        }
        catch(JSONException ex) {
          throw new IllegalArgumentException("The key "
                                             + field
                                             + " does not have a valid double value.",
                                             ex);
        }

        gpo.setField(field, val);
        break;
      }
      case FLOAT: {
        Float val;

        try {
          val = (float)jo.getDouble(index);
        }
        catch(JSONException ex) {
          throw new IllegalArgumentException("The key "
                                             + field
                                             + " does not have a valid double value.",
                                             ex);
        }

        gpo.setField(field, val);
        break;
      }
      default:
        throw new UnsupportedOperationException("This is not supported for type: " + type);
    }
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

      gpo.setField(field, val);
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

      gpo.setField(field, val);
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

      gpo.setField(field, val);
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

      if(fieldType == Type.BOOLEAN) {
        jo.put(field, gpo.getFieldBool(field));
      }
      else if(fieldType == Type.CHAR) {
        jo.put(field, ((Character) gpo.getFieldChar(field)).toString());
      }
      else if(fieldType == Type.STRING) {
        jo.put(field, gpo.getFieldString(field));
      }
      else if(fieldType == Type.BYTE) {
        jo.put(field, resultFormatter.format(gpo.getFieldByte(field)));
      }
      else if(fieldType == Type.SHORT) {
        jo.put(field, resultFormatter.format(gpo.getFieldShort(field)));
      }
      else if(fieldType == Type.INTEGER) {
        jo.put(field, resultFormatter.format(gpo.getFieldInt(field)));
      }
      else if(fieldType == Type.LONG) {
        jo.put(field, resultFormatter.format(gpo.getFieldLong(field)));
      }
      else if(fieldType == Type.FLOAT) {
        jo.put(field, resultFormatter.format(gpo.getFieldFloat(field)));
      }
      else if(fieldType == Type.DOUBLE) {
        jo.put(field, resultFormatter.format(gpo.getFieldDouble(field)));
      }
      else {
        throw new UnsupportedOperationException("The type " + fieldType + " is not supported.");
      }
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
   * Computes the number of bytes required to serialize the given {@link GPOMutable} object.
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
   * @return The serialize {@link GPOMutable} object.
   */
  public static byte[] serialize(GPOMutable gpo)
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

    return sbytes;
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

      switch(type) {
      case BOOLEAN: {
        serializeBoolean(gpo.getFieldBool(field),
                         sbytes,
                         offset);
        break;
      }
      case BYTE: {
        serializeByte(gpo.getFieldByte(field),
                      sbytes,
                      offset);
        break;
      }
      case SHORT: {
        serializeShort(gpo.getFieldShort(field),
                      sbytes,
                      offset);
        break;
      }
      case INTEGER: {
        serializeInt(gpo.getFieldInt(field),
                     sbytes,
                     offset);
        break;
      }
      case LONG: {
        serializeLong(gpo.getFieldLong(field),
                     sbytes,
                     offset);
        break;
      }
      case CHAR: {
        serializeChar(gpo.getFieldChar(field),
                      sbytes,
                      offset);
        break;
      }
      case STRING: {
        serializeString(gpo.getFieldString(field),
                        sbytes,
                        offset);
        break;
      }
      case FLOAT: {
        serializeFloat(gpo.getFieldFloat(field),
                       sbytes,
                       offset);
        break;
      }
      case DOUBLE: {
        serializeDouble(gpo.getFieldDouble(field),
                        sbytes,
                        offset);
        break;
      }
      default:
        throw new UnsupportedOperationException("The field " + field + " doesn't have a valid type.");
      }
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
                                       int offset)
  {
    GPOMutable gpo = new GPOMutable(fd);
    MutableInt offsetM = new MutableInt(offset);

    boolean[] fieldsBoolean = gpo.getFieldsBoolean();
    if(fieldsBoolean != null) {
      for(int index = 0;
          index < fieldsBoolean.length;
          index++) {
        fieldsBoolean[index] = deserializeBoolean(serializedGPO, offsetM);
      }
    }

    char[] fieldsCharacter = gpo.getFieldsCharacter();
    if(fieldsCharacter != null) {
      for(int index = 0;
          index < fieldsCharacter.length;
          index++) {
        fieldsCharacter[index] = deserializeChar(serializedGPO, offsetM);
      }
    }

    byte[] fieldsByte = gpo.getFieldsByte();
    if(fieldsByte != null) {
      for(int index = 0;
          index < fieldsByte.length;
          index++) {
        fieldsByte[index] = deserializeByte(serializedGPO, offsetM);
      }
    }

    short[] fieldsShort = gpo.getFieldsShort();
    if(fieldsShort != null) {
      for(int index = 0;
          index < fieldsShort.length;
          index++) {
        fieldsShort[index] = deserializeShort(serializedGPO, offsetM);
      }
    }

    int[] fieldsInteger = gpo.getFieldsInteger();
    if(fieldsInteger != null) {
      for(int index = 0;
          index < fieldsInteger.length;
          index++) {
        fieldsInteger[index] = deserializeInt(serializedGPO, offsetM);
      }
    }

    long[] fieldsLong = gpo.getFieldsLong();
    if(fieldsLong != null) {
      for(int index = 0;
          index < fieldsLong.length;
          index++) {
        fieldsLong[index] = deserializeLong(serializedGPO, offsetM);
      }
    }

    float[] fieldsFloat = gpo.getFieldsFloat();
    if(fieldsFloat != null) {
      for(int index = 0;
          index < fieldsFloat.length;
          index++) {
        fieldsFloat[index] = deserializeFloat(serializedGPO, offsetM);
      }
    }

    double[] fieldsDouble = gpo.getFieldsDouble();
    if(fieldsDouble != null) {
      for(int index = 0;
          index < fieldsDouble.length;
          index++) {
        fieldsDouble[index] = deserializeDouble(serializedGPO, offsetM);
      }
    }

    String[] fieldsString = gpo.getFieldsString();
    if(fieldsString != null) {
      for(int index = 0;
          index < fieldsString.length;
          index++) {
        fieldsString[index] = deserializeString(serializedGPO, offsetM);
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

      switch(type)
      {
        case BOOLEAN: {
          boolean val = deserializeBoolean(serializedGPO, offsetM);
          gpo.setField(field, val);
          break;
        }
        case BYTE: {
          byte val = deserializeByte(serializedGPO, offsetM);
          gpo.setField(field, val);
          break;
        }
        case CHAR: {
          char val = deserializeChar(serializedGPO, offsetM);
          gpo.setField(field, val);
          break;
        }
        case SHORT: {
          short val = deserializeShort(serializedGPO, offsetM);
          gpo.setField(field, val);
          break;
        }
        case INTEGER: {
          int val = deserializeInt(serializedGPO, offsetM);
          gpo.setField(field, val);
          break;
        }
        case LONG: {
          long val = deserializeLong(serializedGPO, offsetM);
          gpo.setField(field, val);
          break;
        }
        case FLOAT: {
          float val = deserializeFloat(serializedGPO, offsetM);
          gpo.setField(field, val);
          break;
        }
        case DOUBLE: {
          double val = deserializeDouble(serializedGPO, offsetM);
          gpo.setField(field, val);
          break;
        }
        case STRING: {
          String val = deserializeString(serializedGPO, offsetM);
          gpo.setField(field, val);
          break;
        }
        default:
          throw new UnsupportedOperationException("Cannot deserialize type " + type);
      }
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
    long val = ((((long) buffer[0 + offset.intValue()]) & 0xFFL) << 56) |
           ((((long) buffer[1 + offset.intValue()]) & 0xFFL) << 48) |
           ((((long) buffer[2 + offset.intValue()]) & 0xFFL) << 40) |
           ((((long) buffer[3 + offset.intValue()]) & 0xFFL) << 32) |
           ((((long) buffer[4 + offset.intValue()]) & 0xFFL) << 24) |
           ((((long) buffer[5 + offset.intValue()]) & 0xFFL) << 16) |
           ((((long) buffer[6 + offset.intValue()]) & 0xFFL) << 8)  |
           (((long) buffer[7 + offset.intValue()]) & 0xFFL);

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
    buffer[0 + offset.intValue()] = (byte) ((val >> 56) & 0xFFL);
    buffer[1 + offset.intValue()] = (byte) ((val >> 48) & 0xFFL);
    buffer[2 + offset.intValue()] = (byte) ((val >> 40) & 0xFFL);
    buffer[3 + offset.intValue()] = (byte) ((val >> 32) & 0xFFL);
    buffer[4 + offset.intValue()] = (byte) ((val >> 24) & 0xFFL);
    buffer[5 + offset.intValue()] = (byte) ((val >> 16) & 0xFFL);
    buffer[6 + offset.intValue()] = (byte) ((val >> 8) & 0xFFL);
    buffer[7 + offset.intValue()] = (byte) (val & 0xFFL);

    offset.add(Type.LONG.getByteSize());
  }

  /**
   * Serializes the given long value to an array of bytes.
   * @param val The long value to serialize.
   * @return The serialized long value.
   */
  public static byte[] serializeLong(long val)
  {
    byte[] bytes = new byte[Type.LONG.getByteSize()];
    serializeLong(val, bytes, new MutableInt(0));
    return bytes;
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
    long val = (((long) buffer[0 + offset.intValue()]) & 0xFFL) << 56 |
           ((((long) buffer[1 + offset.intValue()]) & 0xFFL) << 48) |
           ((((long) buffer[2 + offset.intValue()]) & 0xFFL) << 40) |
           ((((long) buffer[3 + offset.intValue()]) & 0xFFL) << 32) |
           ((((long) buffer[4 + offset.intValue()]) & 0xFFL) << 24) |
           ((((long) buffer[5 + offset.intValue()]) & 0xFFL) << 16) |
           ((((long) buffer[6 + offset.intValue()]) & 0xFFL) << 8)  |
           (((long) buffer[7 + offset.intValue()]) & 0xFFL);

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

    buffer[0 + offset.intValue()] = (byte) ((val >> 56) & 0xFFL);
    buffer[1 + offset.intValue()] = (byte) ((val >> 48) & 0xFFL);
    buffer[2 + offset.intValue()] = (byte) ((val >> 40) & 0xFFL);
    buffer[3 + offset.intValue()] = (byte) ((val >> 32) & 0xFFL);
    buffer[4 + offset.intValue()] = (byte) ((val >> 24) & 0xFFL);
    buffer[5 + offset.intValue()] = (byte) ((val >> 16) & 0xFFL);
    buffer[6 + offset.intValue()] = (byte) ((val >> 8) & 0xFFL);
    buffer[7 + offset.intValue()] = (byte) (val & 0xFFL);

    offset.add(Type.DOUBLE.getByteSize());
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
    int val = ((((int) buffer[0 + offset.intValue()]) & 0xFF) << 24) |
           ((((int) buffer[1 + offset.intValue()]) & 0xFF) << 16) |
           ((((int) buffer[2 + offset.intValue()]) & 0xFF) << 8)  |
           (((int) buffer[3 + offset.intValue()]) & 0xFF);

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
    buffer[0 + offset.intValue()] = (byte) ((val >> 24) & 0xFF);
    buffer[1 + offset.intValue()] = (byte) ((val >> 16) & 0xFF);
    buffer[2 + offset.intValue()] = (byte) ((val >> 8) & 0xFF);
    buffer[3 + offset.intValue()] = (byte) (val & 0xFF);

    offset.add(Type.INTEGER.getByteSize());
  }

  /**
   * Serializes the given integer value.
   * @param val The value to serialize.
   * @return The serialized integer value.
   */
  public static byte[] serializeInt(int val) {
    byte[] bytes = new byte[Type.INTEGER.getByteSize()];
    serializeInt(val, bytes, new MutableInt(0));
    return bytes;
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
    int val = ((((int) buffer[0 + offset.intValue()]) & 0xFF) << 24) |
           ((((int) buffer[1 + offset.intValue()]) & 0xFF) << 16) |
           ((((int) buffer[2 + offset.intValue()]) & 0xFF) << 8)  |
           (((int) buffer[3 + offset.intValue()]) & 0xFF);

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
    int val = Float.floatToIntBits(valf);

    buffer[0 + offset.intValue()] = (byte) ((val >> 24) & 0xFF);
    buffer[1 + offset.intValue()] = (byte) ((val >> 16) & 0xFF);
    buffer[2 + offset.intValue()] = (byte) ((val >> 8) & 0xFF);
    buffer[3 + offset.intValue()] = (byte) (val & 0xFF);

    offset.add(Type.FLOAT.getByteSize());
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
    short val = (short) (((((int) buffer[0 + offset.intValue()]) & 0xFF) << 8)  |
                (((int) buffer[1 + offset.intValue()]) & 0xFF));

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
    buffer[0 + offset.intValue()] = (byte) ((val >> 8) & 0xFF);
    buffer[1 + offset.intValue()] = (byte) (val & 0xFF);

    offset.add(Type.SHORT.getByteSize());
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
    char val = (char) (((((int) buffer[0 + offset.intValue()]) & 0xFF) << 8)  |
                (((int) buffer[1 + offset.intValue()]) & 0xFF));

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
    buffer[0 + offset.intValue()] = (byte) ((val >> 8) & 0xFF);
    buffer[1 + offset.intValue()] = (byte) (val & 0xFF);

    offset.add(Type.CHAR.getByteSize());
  }

  /**
   * Utility method for creating boolean getters. This method is useful for creating a {@link GPOGetters} object
   * which can be used to copy POJOs into GPOMutable objects.
   * @param fields The fields to create getters for. The order of the fields in this list will be the same order
   * that the getters will be returned in.
   * @param valueToExpression A map from field names to the corresponding java expression to be used for getting
   * the fields.
   * @param clazz The Class of the POJO to extract values from.
   * @return An array of boolean getters for given fields.
   */
  @SuppressWarnings({"unchecked","rawtypes"})
  public static GetterBoolean<Object>[] createGetterBoolean(List<String> fields,
                                                            Map<String, String> valueToExpression,
                                                            Class<?> clazz)
  {
    GetterBoolean<Object>[] gettersBoolean = new GetterBoolean[fields.size()];

    for(int getterIndex = 0;
        getterIndex < fields.size();
        getterIndex++) {
      String field = fields.get(getterIndex);
      gettersBoolean[getterIndex] = PojoUtils.createGetterBoolean(clazz, valueToExpression.get(field));
    }

    return gettersBoolean;
  }

  /**
   * Utility method for creating string getters. This method is useful for creating a {@link GPOGetters} object
   * which can be used to copy POJOs into GPOMutable objects.
   * @param fields The fields to create getters for. The order of the fields in this list will be the same order
   * that the getters will be returned in.
   * @param valueToExpression A map from field names to the corresponding java expression to be used for getting
   * the fields.
   * @param clazz The Class of the POJO to extract values from.
   * @return An array of string getters for given fields.
   */
  @SuppressWarnings({"unchecked","rawtypes"})
  public static Getter<Object, String>[] createGetterString(List<String> fields,
                                                            Map<String, String> valueToExpression,
                                                            Class<?> clazz)
  {
    Getter<Object, String>[] gettersString = new Getter[fields.size()];

    for(int getterIndex = 0;
        getterIndex < fields.size();
        getterIndex++) {
      String field = fields.get(getterIndex);
      gettersString[getterIndex] = PojoUtils.createGetter(clazz, valueToExpression.get(field), String.class);
    }

    return gettersString;
  }

  /**
   * Utility method for creating char getters. This method is useful for creating a {@link GPOGetters} object
   * which can be used to copy POJOs into GPOMutable objects.
   * @param fields The fields to create getters for. The order of the fields in this list will be the same order
   * that the getters will be returned in.
   * @param valueToExpression A map from field names to the corresponding java expression to be used for getting
   * the fields.
   * @param clazz The Class of the POJO to extract values from.
   * @return An array of char getters for given fields.
   */
  @SuppressWarnings({"unchecked","rawtypes"})
  public static GetterChar<Object>[] createGetterChar(List<String> fields,
                                                      Map<String, String> valueToExpression,
                                                      Class<?> clazz)
  {
    GetterChar<Object>[] gettersChar = new GetterChar[fields.size()];

    for(int getterIndex = 0;
        getterIndex < fields.size();
        getterIndex++) {
      String field = fields.get(getterIndex);
      gettersChar[getterIndex] = PojoUtils.createGetterChar(clazz, valueToExpression.get(field));
    }

    return gettersChar;
  }

  /**
   * Utility method for creating byte getters. This method is useful for creating a {@link GPOGetters} object
   * which can be used to copy POJOs into GPOMutable objects.
   * @param fields The fields to create getters for. The order of the fields in this list will be the same order
   * that the getters will be returned in.
   * @param valueToExpression A map from field names to the corresponding java expression to be used for getting
   * the fields.
   * @param clazz The Class of the POJO to extract values from.
   * @return An array of byte getters for given fields.
   */
  @SuppressWarnings({"unchecked","rawtypes"})
  public static GetterByte<Object>[] createGetterByte(List<String> fields,
                                                      Map<String, String> valueToExpression,
                                                      Class<?> clazz)
  {
    GetterByte<Object>[] gettersByte = new GetterByte[fields.size()];

    for(int getterIndex = 0;
        getterIndex < fields.size();
        getterIndex++) {
      String field = fields.get(getterIndex);
      gettersByte[getterIndex] = PojoUtils.createGetterByte(clazz, valueToExpression.get(field));
    }

    return gettersByte;
  }

  /**
   * Utility method for creating short getters. This method is useful for creating a {@link GPOGetters} object
   * which can be used to copy POJOs into GPOMutable objects.
   * @param fields The fields to create getters for. The order of the fields in this list will be the same order
   * that the getters will be returned in.
   * @param valueToExpression A map from field names to the corresponding java expression to be used for getting
   * the fields.
   * @param clazz The Class of the POJO to extract values from.
   * @return An array of short getters for given fields.
   */
  @SuppressWarnings({"unchecked","rawtypes"})
  public static GetterShort<Object>[] createGetterShort(List<String> fields,
                                                        Map<String, String> valueToExpression,
                                                        Class<?> clazz)
  {
    GetterShort<Object>[] gettersShort = new GetterShort[fields.size()];

    for(int getterIndex = 0;
        getterIndex < fields.size();
        getterIndex++) {
      String field = fields.get(getterIndex);
      gettersShort[getterIndex] = PojoUtils.createGetterShort(clazz, valueToExpression.get(field));
    }

    return gettersShort;
  }

  /**
   * Utility method for creating integer getters. This method is useful for creating a {@link GPOGetters} object
   * which can be used to copy POJOs into GPOMutable objects.
   * @param fields The fields to create getters for. The order of the fields in this list will be the same order
   * that the getters will be returned in.
   * @param valueToExpression A map from field names to the corresponding java expression to be used for getting
   * the fields.
   * @param clazz The Class of the POJO to extract values from.
   * @return An array of integer getters for given fields.
   */
  @SuppressWarnings({"unchecked","rawtypes"})
  public static GetterInt<Object>[] createGetterInt(List<String> fields,
                                                    Map<String, String> valueToExpression,
                                                    Class<?> clazz)
  {
    GetterInt<Object>[] gettersInt = new GetterInt[fields.size()];

    for(int getterIndex = 0;
        getterIndex < fields.size();
        getterIndex++) {
      String field = fields.get(getterIndex);
      gettersInt[getterIndex] = PojoUtils.createGetterInt(clazz, valueToExpression.get(field));
    }

    return gettersInt;
  }

  /**
   * Utility method for creating long getters. This method is useful for creating a {@link GPOGetters} object
   * which can be used to copy POJOs into GPOMutable objects.
   * @param fields The fields to create getters for. The order of the fields in this list will be the same order
   * that the getters will be returned in.
   * @param valueToExpression A map from field names to the corresponding java expression to be used for getting
   * the fields.
   * @param clazz The Class of the POJO to extract values from.
   * @return An array of long getters for given fields.
   */
  @SuppressWarnings({"unchecked","rawtypes"})
  public static GetterLong<Object>[] createGetterLong(List<String> fields,
                                                      Map<String, String> valueToExpression,
                                                      Class<?> clazz)
  {
    GetterLong<Object>[] gettersLong = new GetterLong[fields.size()];

    for(int getterIndex = 0;
        getterIndex < fields.size();
        getterIndex++) {
      String field = fields.get(getterIndex);
      gettersLong[getterIndex] = PojoUtils.createGetterLong(clazz, valueToExpression.get(field));
    }

    return gettersLong;
  }

  /**
   * Utility method for creating float getters. This method is useful for creating a {@link GPOGetters} object
   * which can be used to copy POJOs into GPOMutable objects.
   * @param fields The fields to create getters for. The order of the fields in this list will be the same order
   * that the getters will be returned in.
   * @param valueToExpression A map from field names to the corresponding java expression to be used for getting
   * the fields.
   * @param clazz The Class of the POJO to extract values from.
   * @return An array of float getters for given fields.
   */
  @SuppressWarnings({"unchecked","rawtypes"})
  public static GetterFloat<Object>[] createGetterFloat(List<String> fields,
                                                        Map<String, String> valueToExpression,
                                                        Class<?> clazz)
  {
    GetterFloat<Object>[] gettersFloat = new GetterFloat[fields.size()];

    for(int getterIndex = 0;
        getterIndex < fields.size();
        getterIndex++) {
      String field = fields.get(getterIndex);
      gettersFloat[getterIndex] = PojoUtils.createGetterFloat(clazz, valueToExpression.get(field));
    }

    return gettersFloat;
  }

  /**
   * Utility method for creating double getters. This method is useful for creating a {@link GPOGetters} object
   * which can be used to copy POJOs into GPOMutable objects.
   * @param fields The fields to create getters for. The order of the fields in this list will be the same order
   * that the getters will be returned in.
   * @param valueToExpression A map from field names to the corresponding java expression to be used for getting
   * the fields.
   * @param clazz The Class of the POJO to extract values from.
   * @return An array of double getters for given fields.
   */
  @SuppressWarnings({"unchecked","rawtypes"})
  public static GetterDouble<Object>[] createGetterDouble(List<String> fields,
                                                          Map<String, String> valueToExpression,
                                                          Class<?> clazz)
  {
    GetterDouble<Object>[] gettersDouble = new GetterDouble[fields.size()];

    for(int getterIndex = 0;
        getterIndex < fields.size();
        getterIndex++) {
      String field = fields.get(getterIndex);
      gettersDouble[getterIndex] = PojoUtils.createGetterDouble(clazz, valueToExpression.get(field));
    }

    return gettersDouble;
  }

  /**
   * Utility method for creating object getters. This method is useful for creating a {@link GPOGetters} object
   * which can be used to copy POJOs into GPOMutable objects.
   * @param fields The fields to create getters for. The order of the fields in this list will be the same order
   * that the getters will be returned in.
   * @param valueToExpression A map from field names to the corresponding java expression to be used for getting
   * the fields.
   * @param clazz The Class of the POJO to extract values from.
   * @return An array of object getters for given fields.
   */
  @SuppressWarnings({"unchecked","rawtypes"})
  public static Getter<Object, Object>[] createGetterObject(List<String> fields,
                                                            Map<String, String> valueToExpression,
                                                            Class<?> clazz)
  {
    Getter<Object, Object>[] gettersObject = new Getter[fields.size()];

    for(int getterIndex = 0;
        getterIndex < fields.size();
        getterIndex++) {
      String field = fields.get(getterIndex);
      gettersObject[getterIndex] = PojoUtils.createGetter(clazz, valueToExpression.get(field), Object.class);
    }

    return gettersObject;
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
      List<String> fields = entry.getValue();

      switch(inputType) {
        case BOOLEAN: {
          gpoGetters.gettersBoolean = createGetterBoolean(fields,
                                                          fieldToGetter,
                                                          clazz);
          break;
        }
        case STRING: {
          gpoGetters.gettersString = createGetterString(fields,
                                                        fieldToGetter,
                                                        clazz);
          break;
        }
        case CHAR: {
          gpoGetters.gettersChar = createGetterChar(fields,
                                                    fieldToGetter,
                                                    clazz);
          break;
        }
        case DOUBLE: {
          gpoGetters.gettersDouble = createGetterDouble(fields,
                                                        fieldToGetter,
                                                        clazz);
          break;
        }
        case FLOAT: {
          gpoGetters.gettersFloat = createGetterFloat(fields,
                                                      fieldToGetter,
                                                      clazz);
          break;
        }
        case LONG: {
          gpoGetters.gettersLong = createGetterLong(fields,
                                                    fieldToGetter,
                                                    clazz);
          break;
        }
        case INTEGER: {
          gpoGetters.gettersInteger = createGetterInt(fields,
                                                      fieldToGetter,
                                                      clazz);

          break;
        }
        case SHORT: {
          gpoGetters.gettersShort = createGetterShort(fields,
                                                      fieldToGetter,
                                                      clazz);

          break;
        }
        case BYTE: {
          gpoGetters.gettersByte = createGetterByte(fields,
                                                    fieldToGetter,
                                                    clazz);

          break;
        }
        case OBJECT: {
          gpoGetters.gettersObject = createGetterObject(fields,
                                                        fieldToGetter,
                                                        clazz);

          break;
        }
        default: {
          throw new IllegalArgumentException("The type " + inputType + " is not supported.");
        }
      }
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

  private static final Logger LOG = LoggerFactory.getLogger(GPOUtils.class);
}
