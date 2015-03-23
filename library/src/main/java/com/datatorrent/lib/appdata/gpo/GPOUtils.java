/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.gpo;

import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GPOUtils
{
  private static final Logger logger = LoggerFactory.getLogger(GPOUtils.class);

  private static ByteBuffer BB_2 = ByteBuffer.allocate(2);
  private static ByteBuffer BB_4 = ByteBuffer.allocate(4);
  private static ByteBuffer BB_8 = ByteBuffer.allocate(8);

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

  public static GPOImmutable deserializeIm(FieldsDescriptor fieldsDescriptor,
                                           JSONObject dpou)
  {
    GPOMutable gpoMutable = deserialize(fieldsDescriptor, dpou);
    return new GPOImmutable(gpoMutable);
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
    if(type == Type.BOOLEAN) {
      Boolean val;

      try {
        val = jo.getBoolean(index);
      }
      catch(JSONException ex) {
        throw new IllegalArgumentException("The key " + field + " does not have a valid bool value.", ex);
      }

      gpo.setField(field, val);
    }
    else if(type == Type.BYTE) {
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
    }
    else if(type == Type.SHORT) {
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
    }
    else if(type == Type.INTEGER) {
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
    }
    else if(type == Type.LONG) {
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
    }
    else if(type == Type.CHAR) {
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
    }
    else if(type == Type.STRING) {
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
    }
    else if(type == Type.DOUBLE) {
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
    }
    else if(type == Type.FLOAT) {
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

  public static JSONObject serializeJSONObject(GPOMutable gpo, Fields fields) throws JSONException
  {
    JSONObject jo = new JSONObject();
    FieldsDescriptor fd = gpo.getFieldDescriptor();

    for(String field: fields.getFields()) {
      Type fieldType = fd.getType(field);

      if(fieldType == Type.BOOLEAN) {
        jo.put(field, gpo.getFieldBool(field));
      }
      else if(fieldType == Type.CHAR) {
        jo.put(field, gpo.getFieldChar(field).toString());
      }
      else if(fieldType == Type.STRING) {
        jo.put(field, gpo.getFieldString(field));
      }
      else if(fieldType == Type.BYTE) {
        jo.put(field, gpo.getFieldByte(field));
      }
      else if(fieldType == Type.SHORT) {
        jo.put(field, gpo.getFieldShort(field));
      }
      else if(fieldType == Type.INTEGER) {
        jo.put(field, gpo.getFieldInt(field));
      }
      else if(fieldType == Type.LONG) {
        jo.put(field, gpo.getFieldLong(field));
      }
      else if(fieldType == Type.FLOAT) {
        jo.put(field, gpo.getFieldFloat(field));
      }
      else if(fieldType == Type.DOUBLE) {
        jo.put(field, gpo.getFieldDouble(field));
      }
    }

    return jo;
  }

  public static byte[] serialize(GPOMutable gpo)
  {
    GPOByteArrayList byteList = new GPOByteArrayList();

    for(String field: gpo.getFieldDescriptor().getFields().getFields()) {
      Type type = gpo.getFieldDescriptor().getType(field);

      if(type == Type.BOOLEAN) {
        boolean boolVal = gpo.getFieldBool(field);
        byte byteVal = boolVal ? (byte) 1: (byte) 0;
        byteList.add(byteVal);
      }
      else if(type == Type.BYTE) {
        byteList.add(gpo.getFieldByte(field));
      }
      else if(type == Type.SHORT) {
        BB_2.putShort(gpo.getFieldShort(field));
        byteList.add(BB_2.array());
        BB_2.clear();
      }
      else if(type == Type.INTEGER) {
        BB_4.putInt(gpo.getFieldInt(field));
        byteList.add(BB_4.array());
        BB_4.clear();
      }
      else if(type == Type.LONG) {
        BB_8.putLong(gpo.getFieldLong(field));
        byteList.add(BB_8.array());
        BB_8.clear();
      }
      else if(type == Type.CHAR) {
        BB_2.putChar(gpo.getFieldChar(field));
        byteList.add(BB_2.array());
        BB_2.clear();
      }
      else if(type == Type.STRING) {
        byte[] stringBytes = gpo.getFieldString(field).getBytes();

        int length = stringBytes.length;
        BB_4.putInt(length);
        byteList.add(BB_4.array());
        BB_4.clear();

        byteList.add(stringBytes);
      }
      else if(type == Type.FLOAT) {
        BB_4.putFloat(gpo.getFieldFloat(field));
        byteList.add(BB_4.array());
        BB_4.clear();
      }
      else if(type == Type.DOUBLE) {
        BB_8.putDouble(gpo.getFieldDouble(field));
        byteList.add(BB_8.array());
        BB_8.clear();
      }
    }

    return byteList.toByteArray();
  }

  public static GPOMutable deserialize(FieldsDescriptor fieldsDescriptor,
                                       byte[] serializedGPO)
  {
    GPOMutable gpo = new GPOMutable(fieldsDescriptor);

    int offset = 0;

    for(String field: fieldsDescriptor.getFields().getFields()) {
      Type type = fieldsDescriptor.getType(field);

      if(type == Type.BOOLEAN) {
        boolean val = serializedGPO[offset] == (byte) 1;
        gpo.setField(field, val);
        offset++;
      }
      else if(type == Type.BYTE) {
        byte val = serializedGPO[offset];
        gpo.setField(field, val);
        offset++;
      }
      else if(type == Type.SHORT) {
        BB_2.get(serializedGPO, offset, 2);
        BB_2.rewind();
        short val = BB_2.getShort();
        BB_2.clear();
        gpo.setField(field, val);
        offset += 2;
      }
      else if(type == Type.INTEGER) {
        BB_4.get(serializedGPO, offset, 4);
        BB_4.rewind();
        int val = BB_4.getInt();
        BB_4.clear();
        gpo.setField(field, val);
        offset += 4;
      }
      else if(type == Type.LONG) {
        BB_8.get(serializedGPO, offset, 8);
        BB_8.rewind();
        long val = BB_8.getLong();
        BB_8.clear();
        gpo.setField(field, val);
        offset += 8;
      }
      else if(type == Type.CHAR) {
        BB_2.get(serializedGPO, offset, 2);
        BB_2.rewind();
        char val = BB_2.getChar();
        BB_2.clear();
        gpo.setField(field, val);
        offset += 2;
      }
      else if(type == Type.STRING) {
        BB_4.get(serializedGPO, offset, 4);
        BB_4.rewind();
        int val = BB_4.getInt();
        BB_4.clear();
        gpo.setField(field, val);
        offset += 4;
      }
    }

    return gpo;
  }


  public static long deserializeLong(byte[] buffer,
                                     int offset)
  {
    return (((long) buffer[0 + offset]) & 0xFFL) << 56 |
           (((long) buffer[1 + offset]) & 0xFFL) << 48 |
           (((long) buffer[2 + offset]) & 0xFFL) << 40 |
           (((long) buffer[3 + offset]) & 0xFFL) << 32 |
           (((long) buffer[4 + offset]) & 0xFFL) << 24 |
           (((long) buffer[5 + offset]) & 0xFFL) << 16 |
           (((long) buffer[6 + offset]) & 0xFFL) << 8  |
           (((long) buffer[7 + offset]) & 0xFFL) ;
  }

  public static int deserializeInt(byte[] buffer,
                                   int offset)
  {
    return (((int) buffer[0 + offset]) & 0xFF) << 24 |
           (((int) buffer[1 + offset]) & 0xFF) << 16 |
           (((int) buffer[2 + offset]) & 0xFF) << 8  |
           (((int) buffer[3 + offset]) & 0xFF) ;
  }
}
