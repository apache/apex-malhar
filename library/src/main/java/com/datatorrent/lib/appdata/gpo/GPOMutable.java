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

import java.util.Arrays;

import com.google.common.base.Preconditions;

import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;

/**
 * {@link GPOMutable} is an object which stores primitive fields.
 * Each field has a primitive value stored in a primitive array and a name.
 * Fields of a particular primitive type are stored in the same array. The order of fields
 * in the array is the same as the lexicographic ordering of the field names.
 */
public class GPOMutable implements Serializable
{
  private static final long serialVersionUID = 201503231207L;

  //Primitive arrays holding primitive values
  private boolean[] fieldsBoolean;
  private char[] fieldsCharacter;

  private byte[] fieldsByte;
  private short[] fieldsShort;
  private int[] fieldsInteger;
  private long[] fieldsLong;

  private float[] fieldsFloat;
  private double[] fieldsDouble;

  private String[] fieldsString;
  private Object[] fieldsObject;

  private transient FieldsDescriptor fieldDescriptor;

  /**
   * Constructor for kryo serialization
   */
  private GPOMutable()
  {
    //For kryo
  }

  /**
   * A copy constructor.
   * @param gpo The {@link GPOMutable} object to copy.
   */
  public GPOMutable(GPOMutable gpo)
  {
    this(gpo.getFieldDescriptor());

    initialize();

    //copying primitive values.

    {
      boolean[] oldFieldsBoolean = gpo.getFieldsBoolean();
      if(oldFieldsBoolean != null) {
        System.arraycopy(oldFieldsBoolean, 0, fieldsBoolean, 0, fieldsBoolean.length);
      }
    }

    {
      char[] oldFieldsChar = gpo.getFieldsCharacter();
      if(oldFieldsChar != null) {
        System.arraycopy(oldFieldsChar, 0, fieldsCharacter, 0, fieldsCharacter.length);
      }
    }

    {
      byte[] oldFieldsByte = gpo.getFieldsByte();
      if(oldFieldsByte != null) {
        System.arraycopy(oldFieldsByte, 0, fieldsByte, 0, fieldsByte.length);
      }
    }

    {
      short[] oldFieldsShort = gpo.getFieldsShort();
      if(oldFieldsShort != null) {
        System.arraycopy(oldFieldsShort, 0, fieldsShort, 0, fieldsShort.length);
      }
    }

    {
      int[] oldFieldsInteger = gpo.getFieldsInteger();
      if(oldFieldsInteger != null) {
        System.arraycopy(oldFieldsInteger, 0, fieldsInteger, 0, fieldsInteger.length);
      }
    }

    {
      long[] oldFieldsLong = gpo.getFieldsLong();
      if(oldFieldsLong != null) {
        System.arraycopy(oldFieldsLong, 0, fieldsLong, 0, fieldsLong.length);
      }
    }

    {
      float[] oldFieldsFloat = gpo.getFieldsFloat();
      if(oldFieldsFloat != null) {
        System.arraycopy(oldFieldsFloat, 0, fieldsFloat, 0, fieldsFloat.length);
      }
    }

    {
      double[] oldFieldsDouble = gpo.getFieldsDouble();
      if(oldFieldsDouble != null) {
        System.arraycopy(oldFieldsDouble, 0, fieldsDouble, 0, fieldsDouble.length);
      }
    }

    {
      String[] oldFieldsString = gpo.getFieldsString();
      if(oldFieldsString != null) {
        System.arraycopy(oldFieldsString, 0, fieldsString, 0, fieldsString.length);
      }
    }

    {
      Object[] oldFieldsObject = gpo.getFieldsObject();
      if(oldFieldsObject != null) {
        System.arraycopy(oldFieldsObject, 0, fieldsObject, 0, fieldsObject.length);
      }
    }
  }

  /**
   * This constructor copys the specified {@link Fields} from the specified {@link GPOMutable} object.
   * @param gpo The {@link GPOMutable} object to copy fields from.
   * @param subsetFields The fields to copy from the given GPOMutable object.
   */
  public GPOMutable(GPOMutable gpo,
                    Fields subsetFields)
  {
    this(gpo.getFieldDescriptor().getSubset(subsetFields));

    initialize();

    for(String field: this.getFieldDescriptor().getFields().getFields()) {
      this.setFieldGeneric(field, gpo.getField(field));
    }
  }

  /**
   * This creates a GPOMutable object based off the given {@link FieldsDescriptor}.
   * The values of the fields are initialized to their default primitive values.
   * @param fieldDescriptor The {@link FieldsDescriptor} object describing the fields
   * in this {@link GPOMutable} object.
   */
  public GPOMutable(FieldsDescriptor fieldDescriptor)
  {
    setFieldDescriptor(fieldDescriptor);

    initialize();
  }

  /**
   * Helper function to initialize the prinitive arrays in the {@link GPOMutable} object.
   */
  private void initialize()
  {
    for(Type type: fieldDescriptor.getTypeToFields().keySet()) {
      int size = fieldDescriptor.getTypeToSize().get(type);
      switch(type) {
        case BOOLEAN: {
          fieldsBoolean = new boolean[size];
          break;
        }
        case CHAR: {
          fieldsCharacter = new char[size];
          break;
        }
        case STRING: {
          fieldsString = new String[size];
          break;
        }
        case BYTE: {
          fieldsByte = new byte[size];
          break;
        }
        case SHORT: {
          fieldsShort = new short[size];
          break;
        }
        case INTEGER: {
          fieldsInteger = new int[size];
          break;
        }
        case LONG: {
          fieldsLong = new long[size];
          break;
        }
        case FLOAT: {
          fieldsFloat = new float[size];
          break;
        }
        case DOUBLE: {
          fieldsDouble = new double[size];
          break;
        }
        case OBJECT: {
          fieldsObject = new Object[size];
          break;
        }
        default:
          throw new UnsupportedOperationException("The type " + type + " is not supported.");
      }
    }
  }

  /**
   * Gets the primitive array holding boolean fields.
   * @return The primitive array holding boolean fields.
   */
  public boolean[] getFieldsBoolean()
  {
    return fieldsBoolean;
  }

  /**
   * Gets the primitive array holding char fields.
   * @return The primitive array holding char fields.
   */
  public char[] getFieldsCharacter()
  {
    return fieldsCharacter;
  }

  /**
   * Gets the primitive array holding byte fields.
   * @return The primitive array holding byte fields.
   */
  public byte[] getFieldsByte()
  {
    return fieldsByte;
  }

  /**
   * Gets the primitive array holding short fields.
   * @return The primitive array holding short fields.
   */
  public short[] getFieldsShort()
  {
    return fieldsShort;
  }

  /**
   * Gets the primitive array holding integer fields.
   * @return The primitive array holding integer fields.
   */
  public int[] getFieldsInteger()
  {
    return fieldsInteger;
  }

  /**
   * Gets the primitive array holding long fields.
   * @return The primitive array holding long fields.
   */
  public long[] getFieldsLong()
  {
    return fieldsLong;
  }

  /**
   * Gets the primitive array holding float fields.
   * @return The primitive array holding float fields.
   */
  public float[] getFieldsFloat()
  {
    return fieldsFloat;
  }

  /**
   * Gets the primitive array holding double fields.
   * @return The primitive array holding double fields.
   */
  public double[] getFieldsDouble()
  {
    return fieldsDouble;
  }

  /**
   * Gets the primitive array holding string fields.
   * @return The primitive array holding string fields.
   */
  public String[] getFieldsString()
  {
    return fieldsString;
  }

  /**
   * Gets the array holding object fields.
   * @return The array holding object fields.
   */
  public Object[] getFieldsObject()
  {
    return fieldsObject;
  }

  /**
   * Sets the {@link FieldsDescriptor} on this {@link GPOMutable}.
   * @param fieldDescriptor The {@link FieldsDescriptor} to set on this {@link GPOMutable}.
   */
  public final void setFieldDescriptor(FieldsDescriptor fieldDescriptor)
  {
    this.fieldDescriptor = Preconditions.checkNotNull(fieldDescriptor);
  }

  /**
   * Gets the {@link FieldsDescriptor} on this {@link GPOMutable}.
   * @return The {@link FieldsDescriptor} for this {@link GPOMutable}.
   */
  public final FieldsDescriptor getFieldDescriptor()
  {
    return fieldDescriptor;
  }

  /**
   * Sets the specified field to the given value.
   * @param field The name of the field to set.
   * @param val The value to set the field to.
   */
  public final void setFieldGeneric(String field, Object val)
  {
    Type type = fieldDescriptor.getType(field);

    if(type == null) {
      throw new IllegalArgumentException(field + " is not a valid field of this object.");
    }

    int index = fieldDescriptor.getTypeToFieldToIndex().get(type).get(field);

    switch(type) {
      case BOOLEAN: {
        fieldsBoolean[index] = (Boolean) val;
        break;
      }
      case CHAR: {
        fieldsCharacter[index] = (Character) val;
        break;
      }
      case STRING: {
        fieldsString[index] = (String) val;
        break;
      }
      case BYTE: {
        fieldsByte[index] = (Byte) val;
        break;
      }
      case SHORT: {
        fieldsShort[index] = (Short) val;
        break;
      }
      case INTEGER: {
        fieldsInteger[index] = (Integer) val;
        break;
      }
      case LONG: {
        fieldsLong[index] = (Long) val;
        break;
      }
      case FLOAT: {
        fieldsFloat[index] = (Float) val;
        break;
      }
      case DOUBLE: {
        fieldsDouble[index] = (Double) val;
        break;
      }
      case OBJECT: {
        fieldsObject[index] = val;
        break;
      }
      default:
        throw new UnsupportedOperationException("The type " + type + " is not supported.");
    }
  }

  /**
   * Gets the value of the specified field.
   * @param field The field whose value needs to be retrieved.
   * @return The value of the specified field.
   */
  public Object getField(String field)
  {
    Type type = fieldDescriptor.getType(field);

    if(type == null) {
      throw new IllegalArgumentException(field + " is not a valid field of this object.");
    }

    int index = fieldDescriptor.getTypeToFieldToIndex().get(type).get(field);

    switch(type) {
      case BOOLEAN: {
        return fieldsBoolean[index];
      }
      case CHAR: {
        return fieldsCharacter[index];
      }
      case STRING: {
        return fieldsString[index];
      }
      case BYTE: {
        return fieldsByte[index];
      }
      case SHORT: {
        return fieldsShort[index];
      }
      case INTEGER: {
        return fieldsInteger[index];
      }
      case LONG: {
        return fieldsLong[index];
      }
      case FLOAT: {
        return fieldsFloat[index];
      }
      case DOUBLE: {
        return fieldsDouble[index];
      }
      default:
        throw new UnsupportedOperationException("The type " + type + " is not supported.");
    }
  }

  /**
   * Sets the given field to the given boolean value.
   * @param field The field.
   * @param val The value.
   */
  public void setField(String field, boolean val)
  {
    throwInvalidField(field, Type.BOOLEAN);
    fieldsBoolean[fieldDescriptor.getTypeToFieldToIndex().get(Type.BOOLEAN).get(field)] = val;
  }

  /**
   * Gets the boolean value of the given field.
   * @param field The field.
   * @return The value.
   */
  public boolean getFieldBool(String field)
  {
    throwInvalidField(field, Type.BOOLEAN);
    return fieldsBoolean[fieldDescriptor.getTypeToFieldToIndex().get(Type.BOOLEAN).get(field)];
  }

  /**
   * Sets the given field to the given object value.
   * @param field The field.
   * @param val The value.
   */
  public void setFieldObject(String field, Object val)
  {
    throwInvalidField(field, Type.OBJECT);
    fieldsObject[fieldDescriptor.getTypeToFieldToIndex().get(Type.OBJECT).get(field)] = val;
  }

  /**
   * Gets the object value of the given field.
   * @param field The field.
   * @return The value.
   */
  public Object getFieldObject(String field)
  {
    throwInvalidField(field, Type.OBJECT);
    return fieldsObject[fieldDescriptor.getTypeToFieldToIndex().get(Type.OBJECT).get(field)];
  }

  /**
   * Set the char value of the given field.
   * @param field The field.
   * @param val The value.
   */
  public void setField(String field, char val)
  {
    throwInvalidField(field, Type.CHAR);
    fieldsCharacter[fieldDescriptor.getTypeToFieldToIndex().get(Type.CHAR).get(field)] = val;
  }

  /**
   * Gets the char value of the given field.
   * @param field The field.
   * @return The value.
   */
  public char getFieldChar(String field)
  {
    throwInvalidField(field, Type.CHAR);
    return fieldsCharacter[fieldDescriptor.getTypeToFieldToIndex().get(Type.CHAR).get(field)];
  }

  /**
   * Sets the byte value of the givne field.
   * @param field The field.
   * @param val The value.
   */
  public void setField(String field, byte val)
  {
    throwInvalidField(field, Type.BYTE);
    fieldsByte[fieldDescriptor.getTypeToFieldToIndex().get(Type.BYTE).get(field)] = val;
  }

  /**
   * Gets the byte value of the given field.
   * @param field The field.
   * @return The value.
   */
  public byte getFieldByte(String field)
  {
    throwInvalidField(field, Type.BYTE);
    return fieldsByte[fieldDescriptor.getTypeToFieldToIndex().get(Type.BYTE).get(field)];
  }

  /**
   * Set the string value of the given field.
   * @param field The field.
   * @param val The value.
   */
  public void setField(String field, String val)
  {
    throwInvalidField(field, Type.STRING);
    fieldsString[fieldDescriptor.getTypeToFieldToIndex().get(Type.STRING).get(field)] = val;
  }

  /**
   * Gets the string value of the given field.
   * @param field The field.
   * @return The value.
   */
  public String getFieldString(String field)
  {
    throwInvalidField(field, Type.STRING);
    return fieldsString[fieldDescriptor.getTypeToFieldToIndex().get(Type.STRING).get(field)];
  }

  /**
   * Sets the short value of the given field.
   * @param field The field.
   * @param val The value.
   */
  public void setField(String field, short val)
  {
    throwInvalidField(field, Type.SHORT);
    fieldsShort[fieldDescriptor.getTypeToFieldToIndex().get(Type.SHORT).get(field)] = val;
  }

  /**
   * Gets the short value of the given field.
   * @param field The field.
   * @return The value.
   */
  public short getFieldShort(String field)
  {
    throwInvalidField(field, Type.SHORT);
    return fieldsShort[fieldDescriptor.getTypeToFieldToIndex().get(Type.SHORT).get(field)];
  }

  /**
   * Sets the int value of the given field.
   * @param field The field.
   * @param val The value.
   */
  public void setField(String field, int val)
  {
    throwInvalidField(field, Type.INTEGER);
    fieldsInteger[fieldDescriptor.getTypeToFieldToIndex().get(Type.INTEGER).get(field)] = val;
  }

  /**
   * Gets the int value of the given field.
   * @param field The field.
   * @return The value.
   */
  public int getFieldInt(String field)
  {
    throwInvalidField(field, Type.INTEGER);
    return fieldsInteger[fieldDescriptor.getTypeToFieldToIndex().get(Type.INTEGER).get(field)];
  }

  /**
   * Sets the long value of the given field.
   * @param field The field.
   * @param val The value.
   */
  public void setField(String field, long val)
  {
    throwInvalidField(field, Type.LONG);
    fieldsLong[fieldDescriptor.getTypeToFieldToIndex().get(Type.LONG).get(field)] = val;
  }

  /**
   * Gets the long value of the given field.
   * @param field The field.
   * @return The value.
   */
  public long getFieldLong(String field)
  {
    throwInvalidField(field, Type.LONG);
    return fieldsLong[fieldDescriptor.getTypeToFieldToIndex().get(Type.LONG).get(field)];
  }

  /**
   * Sets the float value of the given field.
   * @param field The field.
   * @param val The float value.
   */
  public void setField(String field, float val)
  {
    throwInvalidField(field, Type.FLOAT);
    fieldsFloat[fieldDescriptor.getTypeToFieldToIndex().get(Type.FLOAT).get(field)] = val;
  }

  /**
   * Gets the float value of the given field.
   * @param field The field.
   * @return The value.
   */
  public float getFieldFloat(String field)
  {
    throwInvalidField(field, Type.FLOAT);
    return fieldsFloat[fieldDescriptor.getTypeToFieldToIndex().get(Type.FLOAT).get(field)];
  }

  /**
   * Sets the double value of the given field.
   * @param field The field.
   * @param val The value.
   */
  public void setField(String field, double val)
  {
    throwInvalidField(field, Type.DOUBLE);
    fieldsDouble[fieldDescriptor.getTypeToFieldToIndex().get(Type.DOUBLE).get(field)] = val;
  }

  /**
   * Gets the double value of the given field.
   * @param field The field.
   * @return The value.
   */
  public double getFieldDouble(String field)
  {
    throwInvalidField(field, Type.DOUBLE);
    return fieldsDouble[fieldDescriptor.getTypeToFieldToIndex().get(Type.DOUBLE).get(field)];
  }

  /**
   * This is a helper method which throws an exception if the given field of the
   * given type doesn't exist for this {@link GPOMutable} object.
   * @param field The name of the field.
   * @param type The type of the field.
   */
  private void throwInvalidField(String field, Type type)
  {
    Type fieldType = fieldDescriptor.getType(field);
    if(fieldType == null || !fieldType.equals(type)) {
      throw new IllegalArgumentException(field + " is not a valid field of type " +
                                         type + " on this object.");
    }
  }

  public void applyObjectPayloadFix()
  {
    if(fieldDescriptor.getSerdePayloadFix() != null) {
      fieldDescriptor.getSerdePayloadFix().fix(fieldsObject);
    }
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 23 * hash + Arrays.hashCode(this.fieldsBoolean);
    hash = 23 * hash + Arrays.hashCode(this.fieldsCharacter);
    hash = 23 * hash + Arrays.hashCode(this.fieldsByte);
    hash = 23 * hash + Arrays.hashCode(this.fieldsShort);
    hash = 23 * hash + Arrays.hashCode(this.fieldsInteger);
    hash = 23 * hash + Arrays.hashCode(this.fieldsLong);
    hash = 23 * hash + Arrays.hashCode(this.fieldsFloat);
    hash = 23 * hash + Arrays.hashCode(this.fieldsDouble);
    hash = 23 * hash + Arrays.deepHashCode(this.fieldsString);
    hash = 23 * hash + (this.fieldDescriptor != null ? this.fieldDescriptor.hashCode() : 0);
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
    if(!Arrays.equals(this.fieldsBoolean, other.fieldsBoolean)) {
      return false;
    }
    if(!Arrays.equals(this.fieldsCharacter, other.fieldsCharacter)) {
      return false;
    }
    if(!Arrays.equals(this.fieldsByte, other.fieldsByte)) {
      return false;
    }
    if(!Arrays.equals(this.fieldsShort, other.fieldsShort)) {
      return false;
    }
    if(!Arrays.equals(this.fieldsInteger, other.fieldsInteger)) {
      return false;
    }
    if(!Arrays.equals(this.fieldsLong, other.fieldsLong)) {
      return false;
    }
    if(!Arrays.equals(this.fieldsFloat, other.fieldsFloat)) {
      return false;
    }
    if(!Arrays.equals(this.fieldsDouble, other.fieldsDouble)) {
      return false;
    }
    if(!Arrays.deepEquals(this.fieldsString, other.fieldsString)) {
      return false;
    }
    if(!Arrays.deepEquals(this.fieldsObject, other.fieldsObject)) {
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
    return "GPOMutable{" + "fieldsBoolean=" + Arrays.toString(fieldsBoolean) + ", fieldsCharacter=" + Arrays.toString(fieldsCharacter) + ", fieldsByte=" + Arrays.toString(fieldsByte) + ", fieldsShort=" + Arrays.toString(fieldsShort) + ", fieldsInteger=" + Arrays.toString(fieldsInteger) + ", fieldsLong=" + Arrays.toString(fieldsLong) + ", fieldsFloat=" + Arrays.toString(fieldsFloat) + ", fieldsDouble=" + Arrays.toString(fieldsDouble) + ", fieldsString=" + Arrays.toString(fieldsString) + ", fieldDescriptor=" + fieldDescriptor + '}';
  }
}
