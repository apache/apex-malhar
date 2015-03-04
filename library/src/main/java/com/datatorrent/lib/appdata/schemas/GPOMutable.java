/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GPOMutable
{
  private Map<String, Boolean> fieldToBoolean;
  private Map<String, Character> fieldToCharacter;
  private Map<String, Byte> fieldToByte;
  private Map<String, Short> fieldToShort;
  private Map<String, Integer> fieldToInteger;
  private Map<String, Long> fieldToLong;
  private Map<String, Float> fieldToFloat;
  private Map<String, Double> fieldToDouble;

  private Map<String, Type> fieldToType;

  public GPOMutable(Map<String, Type> fieldToType)
  {
    setFieldToType(fieldToType);
  }

  private void setFieldToType(Map<String, Type> fieldToType)
  {
    for(Map.Entry<String, Type> entry: fieldToType.entrySet()) {
      String field = entry.getKey();
      Type type = entry.getValue();

      if(field == null) {
        throw new NullPointerException("The given map cannot contain a null key: " + entry);
      }

      if(type == null) {
        throw new NullPointerException("The given map cannot contain a null value: " + entry);
      }
    }

    this.fieldToType = Maps.newHashMap(fieldToType);
  }

  public Map<String, Type> getFieldToType()
  {
    return Collections.unmodifiableMap(fieldToType);
  }

  public void setField(String field, Object val)
  {
    Type type = fieldToType.get(field);

    if(type.equals(Type.BOOLEAN)) {
      fieldToBoolean.put(field, (Boolean) val);
    }
    else if(type.equals(Type.CHAR)) {
      fieldToCharacter.put(field, (Character) val);
    }
    else if(type.equals(Type.BYTE)) {
      fieldToByte.put(field, (Byte) val);
    }
    else if(type.equals(Type.SHORT)) {
      fieldToShort.put(field, (Short) val);
    }
    else if(type.equals(Type.INTEGER)) {
      fieldToInteger.put(field, (Integer) val);
    }
    else if(type.equals(Type.LONG)) {
      fieldToLong.put(field, (Long) val);
    }
    else if(type.equals(Type.FLOAT)) {
      fieldToFloat.put(field, (Float) val);
    }
    else if(type.equals(Type.DOUBLE)) {
      fieldToDouble.put(field, (Double) val);
    }
    else {
      throw new IllegalArgumentException(field + " is not a valid field of this object.");
    }
  }

  public Object getField(String field)
  {
    Type type = fieldToType.get(field);

    if(type.equals(Type.BOOLEAN)) {
      return fieldToBoolean.get(field);
    }
    else if(type.equals(Type.CHAR)) {
      return fieldToCharacter.get(field);
    }
    else if(type.equals(Type.BYTE)) {
      return fieldToByte.get(field);
    }
    else if(type.equals(Type.SHORT)) {
      return fieldToShort.get(field);
    }
    else if(type.equals(Type.INTEGER)) {
      return fieldToInteger.get(field);
    }
    else if(type.equals(Type.LONG)) {
      return fieldToLong.get(field);
    }
    else if(type.equals(Type.FLOAT)) {
      return fieldToFloat.get(field);
    }
    else if(type.equals(Type.DOUBLE)) {
      return fieldToDouble.get(field);
    }
    else {
      throw new IllegalArgumentException(field + " is not a valid field of this object.");
    }
  }

  public void setField(String field, Boolean val)
  {
    throwInvalidField(field, Type.BOOLEAN);
    fieldToBoolean.put(field, val);
  }

  public Boolean getFieldBool(String field)
  {
    throwInvalidField(field, Type.BOOLEAN);
    return fieldToBoolean.get(field);
  }

  public void setField(String field, Character val)
  {
    throwInvalidField(field, Type.CHAR);
    fieldToCharacter.put(field, val);
  }

  public Character getFieldChar(String field)
  {
    throwInvalidField(field, Type.CHAR);
    return fieldToCharacter.get(field);
  }

  public void setField(String field, Byte val)
  {
    throwInvalidField(field, Type.BYTE);
    fieldToByte.put(field, val);
  }

  public Byte getFieldByte(String field)
  {
    throwInvalidField(field, Type.BYTE);
    return fieldToByte.get(field);
  }

  public void setField(String field, Short val)
  {
    throwInvalidField(field, Type.SHORT);
    fieldToShort.put(field, val);
  }

  public Short getFieldShort(String field)
  {
    throwInvalidField(field, Type.SHORT);
    return fieldToShort.get(field);
  }

  public void setField(String field, Integer val)
  {
    throwInvalidField(field, Type.INTEGER);
    fieldToInteger.put(field, val);
  }

  public Integer getFieldInt(String field)
  {
    throwInvalidField(field, Type.INTEGER);
    return fieldToInteger.get(field);
  }

  public void setField(String field, Long val)
  {
    throwInvalidField(field, Type.LONG);
    fieldToLong.put(field, val);
  }

  public Long getFieldLong(String field)
  {
    throwInvalidField(field, Type.LONG);
    return fieldToLong.get(field);
  }

  public void setField(String field, Float val)
  {
    throwInvalidField(field, Type.FLOAT);
    fieldToFloat.put(field, val);
  }

  public Float getFieldFloat(String field)
  {
    throwInvalidField(field, Type.FLOAT);
    return fieldToFloat.get(field);
  }

  public void setField(String field, Double val)
  {
    throwInvalidField(field, Type.DOUBLE);
    fieldToDouble.put(field, val);
  }

  public Double getFieldDouble(String field)
  {
    throwInvalidField(field, Type.DOUBLE);
    return fieldToDouble.get(field);
  }

  private void throwInvalidField(String field, Type type)
  {
    Type fieldType = fieldToType.get(field);
    if(fieldType == null || !fieldType.equals(type)) {
      throw new IllegalArgumentException(field + " is not a valid field of type " +
                                         type + " on this object.");
    }
  }
}
