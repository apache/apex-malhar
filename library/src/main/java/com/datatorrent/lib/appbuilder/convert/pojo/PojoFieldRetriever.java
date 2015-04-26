/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.appbuilder.convert.pojo;

import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import javax.validation.constraints.NotNull;

import java.util.Map;

public abstract class PojoFieldRetriever
{
  @NotNull
  private String fqClassName;

  @NotNull
  private Map<String, Type> fieldToType;

  protected Map<String, GetterBoolean> fieldToGetterBoolean;
  protected Map<String, GetterByte> fieldToGetterByte;
  protected Map<String, GetterChar> fieldToGetterChar;
  protected Map<String, GetterDouble> fieldToGetterDouble;
  protected Map<String, GetterFloat> fieldToGetterFloat;
  protected Map<String, GetterInt> fieldToGetterInt;
  protected Map<String, GetterLong> fieldToGetterLong;
  protected Map<String, GetterShort> fieldToGetterShort;
  protected Map<String, GetterString> fieldToGetterString;
  protected Map<String, GetterObject> fieldToGetterObject;

  private boolean isSetup = false;

  public PojoFieldRetriever()
  {
  }

  protected abstract void setup();

  private void callSetup()
  {
    if(!isSetup) {
      setup();
      isSetup = true;
    }
  }

  public Map<String, Type> getFieldToType()
  {
    return fieldToType;
  }

  public void setFieldToType(@NotNull Map<String, Type> fieldToType)
  {
    for(Map.Entry<String, Type> entry: fieldToType.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    fieldToType = Maps.newHashMap(fieldToType);
  }

  public String getFQClassName()
  {
    return fqClassName;
  }

  public void setFQClassName(String fqClassName)
  {
    this.fqClassName = Preconditions.checkNotNull(fqClassName);
  }

  public Object get(String field, Object pojo)
  {
    Object result;

    Type fieldType = this.fieldToType.get(field);

    Preconditions.checkArgument(fieldType == null, field + " is not a valid field.");
    callSetup();

    switch(fieldType) {
      case BOOLEAN:
      {
        result = (Boolean) getBoolean(field, pojo);
        break;
      }
      case CHAR:
      {
        result = (Character) getChar(field, pojo);
        break;
      }
      case STRING:
      {
        result = getString(field, pojo);
        break;
      }
      case BYTE:
      {
        result = (Byte) getByte(field, pojo);
        break;
      }
      case SHORT:
      {
        result = (Short) getShort(field, pojo);
        break;
      }
      case INTEGER:
      {
        result = (Integer) getInt(field, pojo);
        break;
      }
      case LONG:
      {
        result = (Long) getLong(field, pojo);
        break;
      }
      case FLOAT:
      {
        result = (Float) getFloat(field, pojo);
        break;
      }
      case DOUBLE:
      {
        result = (Double) getDouble(field, pojo);
        break;
      }
      case OBJECT:
      {
        result = getObject(field, pojo);
        break;
      }
      default:
        throw new UnsupportedOperationException("Field type " + fieldType + " is not supported.");
    }

    return result;
  }

  public boolean getBoolean(String field, Object pojo)
  {
    throwInvalidField(field, Type.BOOLEAN);
    callSetup();
    return fieldToGetterBoolean.get(field).get(pojo);
  }

  public char getChar(String field, Object pojo)
  {
    throwInvalidField(field, Type.CHAR);
    callSetup();
    return fieldToGetterChar.get(field).get(pojo);
  }

  public byte getByte(String field, Object pojo)
  {
    throwInvalidField(field, Type.BYTE);
    callSetup();
    return fieldToGetterByte.get(field).get(pojo);
  }

  public short getShort(String field, Object pojo)
  {
    throwInvalidField(field, Type.SHORT);
    callSetup();
    return fieldToGetterShort.get(field).get(pojo);
  }

  public int getInt(String field, Object pojo)
  {
    throwInvalidField(field, Type.INTEGER);
    callSetup();
    return fieldToGetterInt.get(field).get(pojo);
  }

  public long getLong(String field, Object pojo)
  {
    throwInvalidField(field, Type.LONG);
    callSetup();
    return fieldToGetterLong.get(field).get(pojo);
  }

  public float getFloat(String field, Object pojo)
  {
    throwInvalidField(field, Type.FLOAT);
    callSetup();
    return fieldToGetterFloat.get(field).get(pojo);
  }

  public double getDouble(String field, Object pojo)
  {
    throwInvalidField(field, Type.DOUBLE);
    callSetup();
    return fieldToGetterDouble.get(field).get(pojo);
  }

  public String getString(String field, Object pojo)
  {
    throwInvalidField(field, Type.STRING);
    callSetup();
    return fieldToGetterString.get(field).get(pojo);
  }

  public Object getObject(String field, Object pojo)
  {
    throwInvalidField(field, Type.OBJECT);
    callSetup();
    return fieldToGetterObject.get(field).get(pojo);
  }

  private void throwInvalidField(String field, Type type)
  {
    Type fieldType = fieldToType.get(field);

    Preconditions.checkArgument(fieldType != null, "There is no field called " + field);
    Preconditions.checkArgument(fieldType == type, "The field " + field +
                                                   " is of type " + type +
                                                   " no type " + fieldType);
  }
}
