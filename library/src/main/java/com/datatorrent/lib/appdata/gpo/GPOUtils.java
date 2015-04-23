/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.gpo;

import com.datatorrent.lib.appdata.schemas.AppDataFormatter;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
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

public class GPOUtils
{
  private static final Logger logger = LoggerFactory.getLogger(GPOUtils.class);

  public static Map<String, Type> buildTypeMap(JSONObject jo) throws Exception
  {
    Map<String, Type> fieldToType = Maps.newHashMap();
    for(Iterator keys = jo.keys();
        keys.hasNext();) {
      String key = (String)keys.next();
      String val = jo.getString(key);
      Type type = Type.getType(val);

      if(type == null) {
        throw new IllegalArgumentException("The given type "
                                           + val
                                           + " is not a valid type.");
      }

      fieldToType.put(key, type);
    }

    return fieldToType;
  }

  public static boolean typeMapValidator(Map<String, String> typeMap)
  {
    for(Map.Entry<String, String> entry: typeMap.entrySet()) {
      String key = entry.getKey();
      String val = entry.getValue();

      if(key == null) {
        logger.error("Null key is not valid.");
        return false;
      }

      if(val == null) {
        logger.error("Null val is not valid.");
        return false;
      }
    }

    return true;
  }

  public static Map<String, Type> buildTypeMap(Map<String, String> stringTypeMap)
  {
    Map<String, Type> typeMap = Maps.newHashMap();

    for(Map.Entry<String, String> entry: stringTypeMap.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();

      if(key == null) {
        throw new IllegalArgumentException("A key cannot be null in the type map.");
      }

      if(value == null) {
        throw new IllegalArgumentException("A value cannot be null in the type map.");
      }

      Type type = Type.getType(value);

      if(type == null) {
        throw new IllegalArgumentException(value + " is not a valid type name.");
      }

      typeMap.put(key, type);
    }

    return typeMap;
  }

  public static GPOMutable deserialize(FieldsDescriptor fieldsDescriptor,
                                       JSONObject dpou)
  {
    GPOMutable gpo = new GPOMutable(fieldsDescriptor);
    Iterator itr = dpou.keys();

    while(itr.hasNext()) {
      String field = (String) itr.next();
      setFieldFromJSON(gpo, field, dpou);
    }

    return gpo;
  }

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

  public static JSONObject serializeJSONObject(GPOMutable gpo, Fields fields, AppDataFormatter adFormatter) throws JSONException
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
        jo.put(field, adFormatter.format(gpo.getFieldByte(field)));
      }
      else if(fieldType == Type.SHORT) {
        jo.put(field, adFormatter.format(gpo.getFieldShort(field)));
      }
      else if(fieldType == Type.INTEGER) {
        jo.put(field, adFormatter.format(gpo.getFieldInt(field)));
      }
      else if(fieldType == Type.LONG) {
        jo.put(field, adFormatter.format(gpo.getFieldLong(field)));
      }
      else if(fieldType == Type.FLOAT) {
        jo.put(field, adFormatter.format(gpo.getFieldFloat(field)));
      }
      else if(fieldType == Type.DOUBLE) {
        jo.put(field, adFormatter.format(gpo.getFieldDouble(field)));
      }
      else {
        throw new UnsupportedOperationException("The type " + fieldType + " is not supported.");
      }
    }

    return jo;
  }

  public static JSONObject serializeJSONObject(GPOMutable gpo, AppDataFormatter adFormatter) throws JSONException
  {
    return serializeJSONObject(gpo, gpo.getFieldDescriptor().getFields(), adFormatter);
  }

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

  ////String

  public static String deserializeString(byte[] buffer,
                                         MutableInt offset)
  {
    int length = deserializeInt(buffer,
                                offset);

    String val = new String(buffer, offset.intValue(), length);
    offset.add(length);
    return val;
  }

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

  ////Long

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

  ////Double

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

  ////Int

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

  ////Float

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

  ////Short

  public static short deserializeShort(byte[] buffer,
                                       MutableInt offset)
  {
    short val = (short) (((((int) buffer[0 + offset.intValue()]) & 0xFF) << 8)  |
                (((int) buffer[1 + offset.intValue()]) & 0xFF));

    offset.add(Type.SHORT.getByteSize());
    return val;
  }

  public static void serializeShort(short val,
                                    byte[] buffer,
                                    MutableInt offset)
  {
    buffer[0 + offset.intValue()] = (byte) ((val >> 8) & 0xFF);
    buffer[1 + offset.intValue()] = (byte) (val & 0xFF);

    offset.add(Type.SHORT.getByteSize());
  }

  ////Byte

  public static byte deserializeByte(byte[] buffer,
                                     MutableInt offset)
  {
    byte val = buffer[offset.intValue()];

    offset.add(Type.BYTE.getByteSize());
    return val;
  }

  public static void serializeByte(byte val,
                                   byte[] buffer,
                                   MutableInt offset)
  {
    buffer[offset.intValue()] = val;

    offset.add(Type.BYTE.getByteSize());
  }

  ////Boolean

  public static boolean deserializeBoolean(byte[] buffer,
                                        MutableInt offset)
  {
    boolean val = buffer[offset.intValue()] != 0;

    offset.add(Type.BOOLEAN.getByteSize());
    return val;
  }

  public static void serializeBoolean(boolean val,
                                      byte[] buffer,
                                      MutableInt offset)
  {
    buffer[offset.intValue()] = (byte) (val ? 1: 0);

    offset.add(Type.BOOLEAN.getByteSize());
  }

  ////Char

  public static char deserializeChar(byte[] buffer,
                                     MutableInt offset)
  {
    char val = (char) (((((int) buffer[0 + offset.intValue()]) & 0xFF) << 8)  |
                (((int) buffer[1 + offset.intValue()]) & 0xFF));

    offset.add(Type.CHAR.getByteSize());
    return val;
  }

  public static void serializeChar(char val,
                                    byte[] buffer,
                                    MutableInt offset)
  {
    buffer[0 + offset.intValue()] = (byte) ((val >> 8) & 0xFF);
    buffer[1 + offset.intValue()] = (byte) (val & 0xFF);

    offset.add(Type.CHAR.getByteSize());
  }
}
