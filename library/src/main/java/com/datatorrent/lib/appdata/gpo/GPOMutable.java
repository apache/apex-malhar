/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.gpo;

import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GPOMutable implements Serializable
{
  private static final Logger logger = LoggerFactory.getLogger(GPOMutable.class);
  private static final long serialVersionUID = 201503231207L;

  private Map<String, Boolean> fieldToBoolean;
  private Map<String, Character> fieldToCharacter;
  private Map<String, String> fieldToString;
  private Map<String, Byte> fieldToByte;
  private Map<String, Short> fieldToShort;
  private Map<String, Integer> fieldToInteger;
  private Map<String, Long> fieldToLong;
  private Map<String, Float> fieldToFloat;
  private Map<String, Double> fieldToDouble;

  private FieldsDescriptor fieldDescriptor;

  public GPOMutable()
  {
    //For kryo
  }

  public GPOMutable(GPOMutable gpo)
  {
    this(gpo.getFieldDescriptor());

    for(String field: this.getFieldDescriptor().getFields().getFields()) {
      this.setField(field, gpo.getField(field));
    }
  }

  public GPOMutable(GPOMutable gpo,
                    Fields subsetFields)
  {
    this(gpo.getFieldDescriptor().getSubset(subsetFields));

    for(String field: this.getFieldDescriptor().getFields().getFields()) {
      this.setField(field, gpo.getField(field));
    }
  }

  public GPOMutable(FieldsDescriptor fieldDescriptor)
  {
    setFieldDescriptor(fieldDescriptor);
  }

  private void setFieldDescriptor(FieldsDescriptor fieldDescriptor)
  {
    Preconditions.checkNotNull(fieldDescriptor);
    this.fieldDescriptor = fieldDescriptor;
  }

  public FieldsDescriptor getFieldDescriptor()
  {
    return fieldDescriptor;
  }

  public void setField(String field, Object val)
  {
    Type type = fieldDescriptor.getType(field);

    if(type == null) {
      throw new IllegalArgumentException(field + " is not a valid field of this object.");
    }

    if(type.equals(Type.BOOLEAN)) {
      if(fieldToBoolean == null) {
         fieldToBoolean = Maps.newHashMap();
      }
      fieldToBoolean.put(field, (Boolean) val);
    }
    else if(type.equals(Type.CHAR)) {
      if(fieldToCharacter == null) {
         fieldToCharacter = Maps.newHashMap();
      }
      fieldToCharacter.put(field, (Character) val);
    }
    else if(type.equals(Type.STRING)) {
      if(fieldToString == null) {
        fieldToString = Maps.newHashMap();
      }
      fieldToString.put(field, (String) val);
    }
    else if(type.equals(Type.BYTE)) {
      if(fieldToByte == null) {
        fieldToByte = Maps.newHashMap();
      }
      fieldToByte.put(field, (Byte) val);
    }
    else if(type.equals(Type.SHORT)) {
      if(fieldToShort == null) {
        fieldToShort = Maps.newHashMap();
      }
      fieldToShort.put(field, (Short) val);
    }
    else if(type.equals(Type.INTEGER)) {
      if(fieldToInteger == null) {
        fieldToInteger = Maps.newHashMap();
      }
      fieldToInteger.put(field, (Integer) val);
    }
    else if(type.equals(Type.LONG)) {
      if(fieldToLong == null) {
        fieldToLong = Maps.newHashMap();
      }
      fieldToLong.put(field, (Long) val);
    }
    else if(type.equals(Type.FLOAT)) {
      if(fieldToFloat == null) {
        fieldToFloat = Maps.newHashMap();
      }
      fieldToFloat.put(field, (Float) val);
    }
    else if(type.equals(Type.DOUBLE)) {
      if(fieldToDouble == null) {
        fieldToDouble = Maps.newHashMap();
      }
      fieldToDouble.put(field, (Double) val);
    }
    else {
      throw new IllegalArgumentException(field + " is not a valid field of this object.");
    }
  }

  public Object getField(String field)
  {
    Type type = fieldDescriptor.getType(field);

    if(type == null) {
      throw new IllegalArgumentException(field + " is not a valid field of this object.");
    }

    if(type.equals(Type.BOOLEAN)) {
      if(fieldToBoolean == null) {
        return null;
      }
      return fieldToBoolean.get(field);
    }
    else if(type.equals(Type.CHAR)) {
      if(fieldToCharacter == null) {
        return null;
      }
      return fieldToCharacter.get(field);
    }
    else if(type.equals(Type.BYTE)) {
      if(fieldToByte == null) {
        return null;
      }
      return fieldToByte.get(field);
    }
    else if(type.equals(Type.SHORT)) {
      if(fieldToShort == null) {
        return null;
      }
      return fieldToShort.get(field);
    }
    else if(type.equals(Type.INTEGER)) {
      if(fieldToInteger == null) {
        return null;
      }
      return fieldToInteger.get(field);
    }
    else if(type.equals(Type.LONG)) {
      if(fieldToLong == null) {
        return null;
      }
      return fieldToLong.get(field);
    }
    else if(type.equals(Type.FLOAT)) {
      if(fieldToFloat == null) {
        return null;
      }
      return fieldToFloat.get(field);
    }
    else if(type.equals(Type.DOUBLE)) {
      if(fieldToDouble == null) {
        return null;
      }
      return fieldToDouble.get(field);
    }
    else if(type.equals(Type.STRING)) {
      if(fieldToString == null) {
        return null;
      }

      return fieldToString.get(field);
    }
    else {
      throw new IllegalArgumentException(field + " is not a valid field of this object.");
    }
  }

  public void setField(String field, Boolean val)
  {
    throwInvalidField(field, Type.BOOLEAN);

    if(fieldToBoolean == null) {
      fieldToBoolean = Maps.newHashMap();
    }

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

    if(fieldToCharacter == null) {
      fieldToCharacter = Maps.newHashMap();
    }

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

    if(fieldToByte == null) {
      fieldToByte = Maps.newHashMap();
    }

    fieldToByte.put(field, val);
  }

  public String getFieldString(String field)
  {
    throwInvalidField(field, Type.STRING);
    return fieldToString.get(field);
  }

  public void setField(String field, String val)
  {
    throwInvalidField(field, Type.STRING);

    if(fieldToString == null) {
      fieldToString = Maps.newHashMap();
    }

    fieldToString.put(field, val);
  }

  public Byte getFieldByte(String field)
  {
    throwInvalidField(field, Type.BYTE);
    return fieldToByte.get(field);
  }

  public void setField(String field, Short val)
  {
    throwInvalidField(field, Type.SHORT);

    if(fieldToShort == null) {
      fieldToShort = Maps.newHashMap();
    }

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

    if(fieldToInteger == null) {
      fieldToInteger = Maps.newHashMap();
    }

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

    if(fieldToLong == null) {
      fieldToLong = Maps.newHashMap();
    }

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

    if(fieldToFloat == null) {
      fieldToFloat = Maps.newHashMap();
    }

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

    if(fieldToDouble == null) {
      fieldToDouble = Maps.newHashMap();
    }

    fieldToDouble.put(field, val);
  }

  public Double getFieldDouble(String field)
  {
    throwInvalidField(field, Type.DOUBLE);
    return fieldToDouble.get(field);
  }

  private void throwInvalidField(String field, Type type)
  {
    Type fieldType = fieldDescriptor.getType(field);
    if(fieldType == null || !fieldType.equals(type)) {
      throw new IllegalArgumentException(field + " is not a valid field of type " +
                                         type + " on this object.");
    }
  }

  @Override
  public int hashCode()
  {
    int hash = 5;
    hash = 19 * hash + (this.fieldToBoolean != null ? this.fieldToBoolean.hashCode() : 0);
    hash = 19 * hash + (this.fieldToCharacter != null ? this.fieldToCharacter.hashCode() : 0);
    hash = 19 * hash + (this.fieldToString != null ? this.fieldToString.hashCode() : 0);
    hash = 19 * hash + (this.fieldToByte != null ? this.fieldToByte.hashCode() : 0);
    hash = 19 * hash + (this.fieldToShort != null ? this.fieldToShort.hashCode() : 0);
    hash = 19 * hash + (this.fieldToInteger != null ? this.fieldToInteger.hashCode() : 0);
    hash = 19 * hash + (this.fieldToLong != null ? this.fieldToLong.hashCode() : 0);
    hash = 19 * hash + (this.fieldToFloat != null ? this.fieldToFloat.hashCode() : 0);
    hash = 19 * hash + (this.fieldToDouble != null ? this.fieldToDouble.hashCode() : 0);
    hash = 19 * hash + (this.fieldDescriptor != null ? this.fieldDescriptor.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    if(obj == null) {
      return false;
    }

    if(!(obj instanceof GPOMutable)) {
      return false;
    }

    final GPOMutable other = (GPOMutable)obj;
    if(this.fieldToBoolean != other.fieldToBoolean && (this.fieldToBoolean == null || !this.fieldToBoolean.equals(other.fieldToBoolean))) {
      return false;
    }

    if(this.fieldToCharacter != other.fieldToCharacter && (this.fieldToCharacter == null || !this.fieldToCharacter.equals(other.fieldToCharacter))) {
      return false;
    }

    if(this.fieldToString != other.fieldToString && (this.fieldToString == null || !this.fieldToString.equals(other.fieldToString))) {
      return false;
    }

    if(this.fieldToByte != other.fieldToByte && (this.fieldToByte == null || !this.fieldToByte.equals(other.fieldToByte))) {
      return false;
    }

    if(this.fieldToShort != other.fieldToShort && (this.fieldToShort == null || !this.fieldToShort.equals(other.fieldToShort))) {
      return false;
    }

    if(this.fieldToInteger != other.fieldToInteger && (this.fieldToInteger == null || !this.fieldToInteger.equals(other.fieldToInteger))) {
      return false;
    }

    if(this.fieldToLong != other.fieldToLong && (this.fieldToLong == null || !this.fieldToLong.equals(other.fieldToLong))) {
      return false;
    }

    if(this.fieldToFloat != other.fieldToFloat && (this.fieldToFloat == null || !this.fieldToFloat.equals(other.fieldToFloat))) {
      return false;
    }

    if(this.fieldToDouble != other.fieldToDouble && (this.fieldToDouble == null || !this.fieldToDouble.equals(other.fieldToDouble))) {
      return false;
    }

    if(this.fieldDescriptor != other.fieldDescriptor && (this.fieldDescriptor == null || !this.fieldDescriptor.equals(other.fieldDescriptor))) {
      return false;
    }

    return true;
  }

  @Override
  public String toString()
  {
    return "GPOMutable{" + "fieldToBoolean=" + fieldToBoolean + ", fieldToCharacter=" + fieldToCharacter + ", fieldToString=" + fieldToString + ", fieldToByte=" + fieldToByte + ", fieldToShort=" + fieldToShort + ", fieldToInteger=" + fieldToInteger + ", fieldToLong=" + fieldToLong + ", fieldToFloat=" + fieldToFloat + ", fieldToDouble=" + fieldToDouble + ", fieldDescriptor=" + fieldDescriptor + '}';
  }
}
