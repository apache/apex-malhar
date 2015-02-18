/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class DynamicPrimitiveObject
{
  public static final String TYPE_BOOLEAN = "boolean";
  public static final String TYPE_CHAR = "char";
  public static final String TYPE_BYTE = "byte";
  public static final String TYPE_SHORT = "short";
  public static final String TYPE_INTEGER = "integer";
  public static final String TYPE_LONG = "long";
  public static final String TYPE_FLOAT = "float";
  public static final String TYPE_DOUBLE = "double";

  private Map<String, Boolean> fieldToBoolean;
  private Map<String, Character> fieldToCharacter;
  private Map<String, Byte> fieldToByte;
  private Map<String, Short> fieldToShort;
  private Map<String, Integer> fieldToInteger;
  private Map<String, Long> fieldToLong;
  private Map<String, Float> fieldToFloat;
  private Map<String, Double> fieldToDouble;

  public static final Set<String> TYPES =
  Collections.unmodifiableSet(Sets.newHashSet(TYPE_BOOLEAN, TYPE_CHAR, TYPE_BYTE,
                                              TYPE_SHORT, TYPE_INTEGER, TYPE_LONG,
                                              TYPE_FLOAT, TYPE_DOUBLE));

  private Map<String, String> fieldToType;

  public DynamicPrimitiveObject(Map<String, String> fieldToType)
  {
    setFieldToType(fieldToType);
  }

  private void setFieldToType(Map<String, String> fieldToType)
  {
    for(Map.Entry<String, String> entry: fieldToType.entrySet()) {
      String field = entry.getKey();
      String type = entry.getValue();

      if(entry.getKey() == null) {
        throw new NullPointerException("The given map cannot contain a null key: " + entry);
      }

      if(type == null) {
        throw new NullPointerException("The given map cannot contain a null value: " + entry);
      }

      if(!TYPES.contains(type)) {
        throw new UnsupportedOperationException(type + " is not a valid type.");
      }
    }

    this.fieldToType = Maps.newHashMap(fieldToType);
  }

  public void setField(String field, Object val)
  {
    String type = fieldToType.get(field);

    if(type.equals(TYPE_BOOLEAN)) {
      fieldToBoolean.put(field, (Boolean) val);
    }
    else if(type.equals(TYPE_CHAR)) {
      fieldToCharacter.put(field, (Character) val);
    }
    else if(type.equals(TYPE_BYTE)) {
      fieldToByte.put(field, (Byte) val);
    }
    else if(type.equals(TYPE_SHORT)) {
      fieldToShort.put(field, (Short) val);
    }
    else if(type.equals(TYPE_INTEGER)) {
      fieldToInteger.put(field, (Integer) val);
    }
    else if(type.equals(TYPE_LONG)) {
      fieldToLong.put(field, (Long) val);
    }
    else if(type.equals(TYPE_FLOAT)) {
      fieldToFloat.put(field, (Float) val);
    }
    else if(type.equals(TYPE_DOUBLE)) {
      fieldToDouble.put(field, (Double) val);
    }
    else {
      throw new IllegalArgumentException(field + " is not a valid field of this object.");
    }
  }

  public Object getField(String field)
  {
    String type = fieldToType.get(field);

    if(type.equals(TYPE_BOOLEAN)) {
      return fieldToBoolean.get(field);
    }
    else if(type.equals(TYPE_CHAR)) {
      return fieldToCharacter.get(field);
    }
    else if(type.equals(TYPE_BYTE)) {
      return fieldToByte.get(field);
    }
    else if(type.equals(TYPE_SHORT)) {
      return fieldToShort.get(field);
    }
    else if(type.equals(TYPE_INTEGER)) {
      return fieldToInteger.get(field);
    }
    else if(type.equals(TYPE_LONG)) {
      return fieldToLong.get(field);
    }
    else if(type.equals(TYPE_FLOAT)) {
      return fieldToFloat.get(field);
    }
    else if(type.equals(TYPE_DOUBLE)) {
      return fieldToDouble.get(field);
    }
    else {
      throw new IllegalArgumentException(field + " is not a valid field of this object.");
    }
  }

  public void setField(String field, Boolean val)
  {
    throwInvalidField(field, TYPE_BOOLEAN);
    fieldToBoolean.put(field, val);
  }

  public Boolean getFieldBool(String field)
  {
    throwInvalidField(field, TYPE_BOOLEAN);
    return fieldToBoolean.get(field);
  }

  public void setField(String field, Character val)
  {
    throwInvalidField(field, TYPE_CHAR);
    fieldToCharacter.put(field, val);
  }

  public Character getFieldChar(String field)
  {
    throwInvalidField(field, TYPE_CHAR);
    return fieldToCharacter.get(field);
  }

  public void setField(String field, Byte val)
  {
    throwInvalidField(field, TYPE_BYTE);
    fieldToByte.put(field, val);
  }

  public Byte getFieldByte(String field)
  {
    throwInvalidField(field, TYPE_BYTE);
    return fieldToByte.get(field);
  }

  public void setField(String field, Short val)
  {
    throwInvalidField(field, TYPE_SHORT);
    fieldToShort.put(field, val);
  }

  public Short getFieldShort(String field)
  {
    throwInvalidField(field, TYPE_SHORT);
    return fieldToShort.get(field);
  }

  public void setField(String field, Integer val)
  {
    throwInvalidField(field, TYPE_INTEGER);
    fieldToInteger.put(field, val);
  }

  public Integer getFieldInt(String field)
  {
    throwInvalidField(field, TYPE_INTEGER);
    return fieldToInteger.get(field);
  }

  public void setField(String field, Long val)
  {
    throwInvalidField(field, TYPE_LONG);
    fieldToLong.put(field, val);
  }

  public Long getFieldLong(String field)
  {
    throwInvalidField(field, TYPE_LONG);
    return fieldToLong.get(field);
  }

  public void setField(String field, Float val)
  {
    throwInvalidField(field, TYPE_FLOAT);
    fieldToFloat.put(field, val);
  }

  public Float getFieldFloat(String field)
  {
    throwInvalidField(field, TYPE_FLOAT);
    return fieldToFloat.get(field);
  }

  public void setField(String field, Double val)
  {
    throwInvalidField(field, TYPE_DOUBLE);
    fieldToDouble.put(field, val);
  }

  public Double getFieldDouble(String field)
  {
    throwInvalidField(field, TYPE_DOUBLE);
    return fieldToDouble.get(field);
  }

  private void throwInvalidField(String field, String type)
  {
    String fieldType = fieldToType.get(field);
    if(fieldType == null || !fieldType.equals(type)) {
      throw new IllegalArgumentException(field + " is not a valid field of type " +
                                         type + " on this object.");
    }
  }
}
