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

import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.appdata.schemas.ResultFormatter;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterByte;
import com.datatorrent.lib.util.PojoUtils.GetterChar;

import static com.datatorrent.lib.appdata.gpo.GPOUtils.*;

/**
 * This is a helper class that reduces the need for switch statements in may utility method in {@link GPOUtils}.
 */
abstract class GPOType
{
  public static final GPOType[] GPO_TYPE_ARRAY;

  static
  {
    GPO_TYPE_ARRAY = new GPOType[Type.values().length];
    Type[] types = Type.values();

    for(int index = 0;
        index < types.length;
        index++) {
      Type type = types[index];

      switch(type) {
        case BOOLEAN: {
          GPO_TYPE_ARRAY[index] = new BooleanT();
          break;
        }
        case CHAR: {
          GPO_TYPE_ARRAY[index] = new CharT();
          break;
        }
        case STRING: {
          GPO_TYPE_ARRAY[index] = new StringT();
          break;
        }
        case OBJECT: {
          GPO_TYPE_ARRAY[index] = new ObjectT();
          break;
        }
        case BYTE: {
          GPO_TYPE_ARRAY[index] = new ByteT();
          break;
        }
        case SHORT: {
          GPO_TYPE_ARRAY[index] = new ShortT();
          break;
        }
        case INTEGER: {
          GPO_TYPE_ARRAY[index] = new IntegerT();
          break;
        }
        case LONG: {
          GPO_TYPE_ARRAY[index] = new LongT();
          break;
        }
        case FLOAT: {
          GPO_TYPE_ARRAY[index] = new FloatT();
          break;
        }
        case DOUBLE: {
          GPO_TYPE_ARRAY[index] = new DoubleT();
          break;
        }
        default:
          throw new UnsupportedOperationException("Type " + type);
      }
    }
  }

  public abstract void setFieldFromJSON(GPOMutable gpo, String field, JSONArray jo, int index);
  public abstract void serializeJSONObject(JSONObject jo, GPOMutable gpo, String field, ResultFormatter resultFormatter) throws JSONException;
  public abstract void serialize(GPOMutable gpo, String field, byte[] sbytes, MutableInt offset);
  public abstract void deserialize(GPOMutable gpo, String Field, byte[] serializedGPO, MutableInt offset);
  public abstract void buildGPOGetters(GPOGetters gpoGetters, List<String> fields, Map<String, String> fieldToGetter, Class<?> clazz);
  public abstract byte[] serialize(Object object);
  public abstract Object deserialize(byte[] object, MutableInt offset);

  public static class BooleanT extends GPOType
  {
    public static final BooleanT INSTANCE = new BooleanT();

    private BooleanT()
    {
    }

    @Override
    public void setFieldFromJSON(GPOMutable gpo, String field, JSONArray jo, int index)
    {
        Boolean val;

        try {
          val = jo.getBoolean(index);
        }
        catch(JSONException ex) {
          throw new IllegalArgumentException("The key " + field + " does not have a valid bool value.", ex);
        }

        gpo.setFieldGeneric(field, val);
    }

    @Override
    public void serializeJSONObject(JSONObject jo, GPOMutable gpo, String field, ResultFormatter resultFormatter) throws JSONException
    {
        jo.put(field, gpo.getFieldBool(field));
    }

    @Override
    public void serialize(GPOMutable gpo, String field, byte[] sbytes, MutableInt offset)
    {
        serializeBoolean(gpo.getFieldBool(field),
                         sbytes,
                         offset);
    }

    @Override
    public void deserialize(GPOMutable gpo, String field, byte[] serializedGPO, MutableInt offset)
    {
      boolean val = deserializeBoolean(serializedGPO, offset);
      gpo.setField(field, val);
    }

    @Override
    public void buildGPOGetters(GPOGetters gpoGetters, List<String> fields, Map<String, String> fieldToGetter, Class<?> clazz)
    {
      gpoGetters.gettersBoolean = createGetters(fields,
                                                fieldToGetter,
                                                clazz,
                                                boolean.class,
                                                GetterBoolean.class);
    }

    @Override
    public byte[] serialize(Object object)
    {
      return GPOUtils.serializeBoolean((Boolean) object);
    }

    @Override
    public Object deserialize(byte[] object, MutableInt offset)
    {
      return GPOUtils.deserializeBoolean(object, offset);
    }
  }

  public static class CharT extends GPOType
  {
    public static final CharT INSTANCE = new CharT();

    private CharT()
    {
    }

    @Override
    public void setFieldFromJSON(GPOMutable gpo, String field, JSONArray jo, int index)
    {
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

    @Override
    public void serializeJSONObject(JSONObject jo, GPOMutable gpo, String field, ResultFormatter resultFormatter) throws JSONException
    {
      jo.put(field, ((Character)gpo.getFieldChar(field)).toString());
    }

    @Override
    public void serialize(GPOMutable gpo, String field, byte[] sbytes, MutableInt offset)
    {
      serializeChar(gpo.getFieldChar(field),
                    sbytes,
                    offset);
    }

    @Override
    public void deserialize(GPOMutable gpo, String field, byte[] serializedGPO, MutableInt offset)
    {
      char val = deserializeChar(serializedGPO, offset);
      gpo.setField(field, val);
    }

    @Override
    public void buildGPOGetters(GPOGetters gpoGetters, List<String> fields, Map<String, String> fieldToGetter, Class<?> clazz)
    {
      gpoGetters.gettersChar = createGetters(fields,
                                             fieldToGetter,
                                             clazz,
                                             char.class,
                                             GetterChar.class);
    }

    @Override
    public byte[] serialize(Object object)
    {
      return GPOUtils.serializeChar((Character) object);
    }

    @Override
    public Object deserialize(byte[] object, MutableInt offset)
    {
      return GPOUtils.deserializeChar(object, offset);
    }
  }

  public static class StringT extends GPOType
  {
    public static final StringT INSTANCE = new StringT();

    private StringT()
    {
    }

    @Override
    public void setFieldFromJSON(GPOMutable gpo, String field, JSONArray jo, int index)
    {
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

    @Override
    public void serializeJSONObject(JSONObject jo, GPOMutable gpo, String field, ResultFormatter resultFormatter) throws JSONException
    {
      jo.put(field, gpo.getFieldString(field));
    }

    @Override
    public void serialize(GPOMutable gpo, String field, byte[] sbytes, MutableInt offset)
    {
      serializeString(gpo.getFieldString(field),
                      sbytes,
                      offset);
    }

    @Override
    public void deserialize(GPOMutable gpo, String field, byte[] serializedGPO, MutableInt offset)
    {
      String val = deserializeString(serializedGPO, offset);
      gpo.setField(field, val);
    }

    @Override
    public void buildGPOGetters(GPOGetters gpoGetters, List<String> fields, Map<String, String> fieldToGetter, Class<?> clazz)
    {
      gpoGetters.gettersString = createGettersString(fields,
                                                     fieldToGetter,
                                                     clazz);
    }

    @Override
    public byte[] serialize(Object object)
    {
      return GPOUtils.serializeString((String) object);
    }

    @Override
    public Object deserialize(byte[] object, MutableInt offset)
    {
      return GPOUtils.deserializeString(object, offset);
    }
  }

  public static class ObjectT extends GPOType
  {
    public static final ObjectT INSTANCE = new ObjectT();

    private ObjectT()
    {
    }

    @Override
    public void setFieldFromJSON(GPOMutable gpo, String field, JSONArray jo, int index)
    {
      throw new UnsupportedOperationException("Type " + Type.OBJECT);
    }

    @Override
    public void serializeJSONObject(JSONObject jo, GPOMutable gpo, String field, ResultFormatter resultFormatter) throws JSONException
    {
      throw new UnsupportedOperationException("Type " + Type.OBJECT);
    }

    @Override
    public void serialize(GPOMutable gpo, String field, byte[] sbytes, MutableInt offset)
    {
      throw new UnsupportedOperationException("Type " + Type.OBJECT);
    }

    @Override
    public void deserialize(GPOMutable gpo, String Field, byte[] serializedGPO, MutableInt offset)
    {
      throw new UnsupportedOperationException("Type " + Type.OBJECT);
    }

    @Override
    public void buildGPOGetters(GPOGetters gpoGetters, List<String> fields, Map<String, String> fieldToGetter, Class<?> clazz)
    {
      gpoGetters.gettersObject = createGettersObject(fields,
                                                     fieldToGetter,
                                                     clazz);
    }

    @Override
    public byte[] serialize(Object object)
    {
      throw new UnsupportedOperationException("This operation is not supported for objects.");
    }

    @Override
    public Object deserialize(byte[] object, MutableInt offset)
    {
      throw new UnsupportedOperationException("This operation is not supported for objects.");
    }
  }

  public static class ByteT extends GPOType
  {
    public static final ByteT INSTANCE = new ByteT();

    private ByteT()
    {
    }

    @Override
    public void setFieldFromJSON(GPOMutable gpo, String field, JSONArray jo, int index)
    {
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

    @Override
    public void serializeJSONObject(JSONObject jo, GPOMutable gpo, String field, ResultFormatter resultFormatter) throws JSONException
    {
      jo.put(field, resultFormatter.format(gpo.getFieldByte(field)));
    }

    @Override
    public void serialize(GPOMutable gpo, String field, byte[] sbytes, MutableInt offset)
    {
      serializeByte(gpo.getFieldByte(field),
                    sbytes,
                    offset);
    }

    @Override
    public void deserialize(GPOMutable gpo, String field, byte[] serializedGPO, MutableInt offset)
    {
      byte val = deserializeByte(serializedGPO, offset);
      gpo.setField(field, val);
    }

    @Override
    public void buildGPOGetters(GPOGetters gpoGetters, List<String> fields, Map<String, String> fieldToGetter, Class<?> clazz)
    {
      gpoGetters.gettersByte = createGetters(fields,
                                             fieldToGetter,
                                             clazz,
                                             byte.class,
                                             GetterByte.class);
    }

    @Override
    public byte[] serialize(Object object)
    {
      return GPOUtils.serializeByte((Byte) object);
    }

    @Override
    public Object deserialize(byte[] object, MutableInt offset)
    {
      return GPOUtils.deserializeByte(object, offset);
    }
  }

  public static class ShortT extends GPOType
  {
    public static final ShortT INSTANCE = new ShortT();

    private ShortT()
    {
    }

    @Override
    public void setFieldFromJSON(GPOMutable gpo, String field, JSONArray jo, int index)
    {
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

    @Override
    public void serializeJSONObject(JSONObject jo, GPOMutable gpo, String field, ResultFormatter resultFormatter) throws JSONException
    {
      jo.put(field, resultFormatter.format(gpo.getFieldShort(field)));
    }

    @Override
    public void serialize(GPOMutable gpo, String field, byte[] sbytes, MutableInt offset)
    {
      serializeShort(gpo.getFieldShort(field),
                     sbytes,
                     offset);
    }

    @Override
    public void deserialize(GPOMutable gpo, String field, byte[] serializedGPO, MutableInt offset)
    {
      short val = deserializeShort(serializedGPO, offset);
      gpo.setField(field, val);
    }

    @Override
    public void buildGPOGetters(GPOGetters gpoGetters, List<String> fields, Map<String, String> fieldToGetter, Class<?> clazz)
    {
      gpoGetters.gettersShort = createGetters(fields,
                                              fieldToGetter,
                                              clazz,
                                              short.class,
                                              PojoUtils.GetterShort.class);
    }

    @Override
    public byte[] serialize(Object object)
    {
      return GPOUtils.serializeShort((Short) object);
    }

    @Override
    public Object deserialize(byte[] object, MutableInt offset)
    {
      return GPOUtils.deserializeShort(object, offset);
    }
  }

  public static class IntegerT extends GPOType
  {
    public static final IntegerT INSTANCE = new IntegerT();

    private IntegerT()
    {
    }

    @Override
    public void setFieldFromJSON(GPOMutable gpo, String field, JSONArray jo, int index)
    {
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

    @Override
    public void serializeJSONObject(JSONObject jo, GPOMutable gpo, String field, ResultFormatter resultFormatter) throws JSONException
    {
      jo.put(field, resultFormatter.format(gpo.getFieldInt(field)));
    }

    @Override
    public void serialize(GPOMutable gpo, String field, byte[] sbytes, MutableInt offset)
    {
      serializeInt(gpo.getFieldInt(field),
                   sbytes,
                   offset);
    }

    @Override
    public void deserialize(GPOMutable gpo, String field, byte[] serializedGPO, MutableInt offset)
    {
      int val = deserializeInt(serializedGPO, offset);
      gpo.setField(field, val);
    }

    @Override
    public void buildGPOGetters(GPOGetters gpoGetters, List<String> fields, Map<String, String> fieldToGetter, Class<?> clazz)
    {
      gpoGetters.gettersInteger = createGetters(fields,
                                                fieldToGetter,
                                                clazz,
                                                int.class,
                                                PojoUtils.GetterInt.class);
    }

    @Override
    public byte[] serialize(Object object)
    {
      return GPOUtils.serializeInt((Integer) object);
    }

    @Override
    public Object deserialize(byte[] object, MutableInt offset)
    {
      return GPOUtils.deserializeInt(object, offset);
    }
  }

  public static class LongT extends GPOType
  {
    public static final LongT INSTANCE = new LongT();

    private LongT()
    {
    }

    @Override
    public void setFieldFromJSON(GPOMutable gpo, String field, JSONArray jo, int index)
    {
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

    @Override
    public void serializeJSONObject(JSONObject jo, GPOMutable gpo, String field, ResultFormatter resultFormatter) throws JSONException
    {
      jo.put(field, resultFormatter.format(gpo.getFieldLong(field)));
    }

    @Override
    public void serialize(GPOMutable gpo, String field, byte[] sbytes, MutableInt offset)
    {
      serializeLong(gpo.getFieldLong(field),
                    sbytes,
                    offset);
    }

    @Override
    public void deserialize(GPOMutable gpo, String field, byte[] serializedGPO, MutableInt offset)
    {
      long val = deserializeLong(serializedGPO, offset);
      gpo.setField(field, val);
    }

    @Override
    public void buildGPOGetters(GPOGetters gpoGetters, List<String> fields, Map<String, String> fieldToGetter, Class<?> clazz)
    {
      gpoGetters.gettersLong = createGetters(fields,
                                             fieldToGetter,
                                             clazz,
                                             long.class,
                                             PojoUtils.GetterLong.class);
    }

    @Override
    public byte[] serialize(Object object)
    {
      return GPOUtils.serializeLong((Long) object);
    }

    @Override
    public Object deserialize(byte[] object, MutableInt offset)
    {
      return GPOUtils.deserializeLong(object, offset);
    }
  }

  public static class FloatT extends GPOType
  {
    public static final FloatT INSTANCE = new FloatT();

    private FloatT()
    {
    }

    @Override
    public void setFieldFromJSON(GPOMutable gpo, String field, JSONArray jo, int index)
    {
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

      gpo.setFieldGeneric(field, val);
    }

    @Override
    public void serializeJSONObject(JSONObject jo, GPOMutable gpo, String field, ResultFormatter resultFormatter) throws JSONException
    {
      jo.put(field, resultFormatter.format(gpo.getFieldFloat(field)));
    }

    @Override
    public void serialize(GPOMutable gpo, String field, byte[] sbytes, MutableInt offset)
    {
      serializeFloat(gpo.getFieldFloat(field),
                     sbytes,
                     offset);
    }

    @Override
    public void deserialize(GPOMutable gpo, String field, byte[] serializedGPO, MutableInt offset)
    {
      float val = deserializeFloat(serializedGPO, offset);
      gpo.setField(field, val);
    }

    @Override
    public void buildGPOGetters(GPOGetters gpoGetters, List<String> fields, Map<String, String> fieldToGetter, Class<?> clazz)
    {
      gpoGetters.gettersFloat = createGetters(fields,
                                              fieldToGetter,
                                              clazz,
                                              float.class,
                                              PojoUtils.GetterFloat.class);
    }

    @Override
    public byte[] serialize(Object object)
    {
      return GPOUtils.serializeFloat((Float) object);
    }

    @Override
    public Object deserialize(byte[] object, MutableInt offset)
    {
      return GPOUtils.deserializeFloat(object, offset);
    }
  }

  public static class DoubleT extends GPOType
  {
    public static final DoubleT INSTANCE = new DoubleT();

    private DoubleT()
    {
    }

    @Override
    public void setFieldFromJSON(GPOMutable gpo, String field, JSONArray jo, int index)
    {
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

      gpo.setFieldGeneric(field, val);
    }

    @Override
    public void serializeJSONObject(JSONObject jo, GPOMutable gpo, String field, ResultFormatter resultFormatter) throws JSONException
    {
      jo.put(field, resultFormatter.format(gpo.getFieldDouble(field)));
    }

    @Override
    public void serialize(GPOMutable gpo, String field, byte[] sbytes, MutableInt offset)
    {
      serializeDouble(gpo.getFieldDouble(field),
                      sbytes,
                      offset);
    }

    @Override
    public void deserialize(GPOMutable gpo, String field, byte[] serializedGPO, MutableInt offset)
    {
      double val = deserializeDouble(serializedGPO, offset);
      gpo.setField(field, val);
    }

    @Override
    public void buildGPOGetters(GPOGetters gpoGetters, List<String> fields, Map<String, String> fieldToGetter, Class<?> clazz)
    {
      gpoGetters.gettersDouble = createGetters(fields,
                                               fieldToGetter,
                                               clazz,
                                               double.class,
                                               PojoUtils.GetterDouble.class);
    }

    @Override
    public byte[] serialize(Object object)
    {
      return GPOUtils.serializeDouble((Double) object);
    }

    @Override
    public Object deserialize(byte[] object, MutableInt offset)
    {
      return GPOUtils.deserializeDouble(object, offset);
    }
  }
}
