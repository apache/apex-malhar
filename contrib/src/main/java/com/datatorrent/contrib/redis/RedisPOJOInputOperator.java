/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.FieldInfo.SupportType;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Setter;
import com.datatorrent.lib.util.PojoUtils.SetterBoolean;
import com.datatorrent.lib.util.PojoUtils.SetterDouble;
import com.datatorrent.lib.util.PojoUtils.SetterFloat;
import com.datatorrent.lib.util.PojoUtils.SetterInt;
import com.datatorrent.lib.util.PojoUtils.SetterLong;
import com.datatorrent.lib.util.PojoUtils.SetterShort;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * This is a Redis input operator, which scans all keys in Redis store It
 * converts Value stored as map to Plain Old Java Object. It outputs
 * KeyValuePair with POJO as value
 * <p>
 * This output adapter Reads from RedisStore stored as <Key, Map> It outputs a
 * Key value pair <key, POJO> as tuples.
 * </p>
 *
 * @displayName Redis POJO Input Operator
 * @category Store
 * @tags output operator, key value
 *
 */
@Evolving
public class RedisPOJOInputOperator extends AbstractRedisInputOperator<KeyValPair<String, Object>>
{
  protected final Map<String, Object> map = new HashMap<String, Object>();
  private ArrayList<FieldInfo> dataColumns;
  private transient ArrayList<Object> setters;
  private boolean isFirstTuple = true;
  private String outputClass;
  private Class<?> objectClass;

  public RedisPOJOInputOperator()
  {
    super();
    setters = new ArrayList<Object>();
  }

  @SuppressWarnings("unchecked")
  private Object convertMapToObject(Map<String, String> tuple)
  {
    try {
      Object mappedObject = objectClass.newInstance();
      for (int i = 0; i < dataColumns.size(); i++) {
        final SupportType type = dataColumns.get(i).getType();
        final String columnName = dataColumns.get(i).getColumnName();

        if (i < setters.size()) {
          String value = tuple.get(columnName);
          switch (type) {
            case STRING:
              ((Setter<Object, String>) setters.get(i)).set(mappedObject, value);
              break;
            case BOOLEAN:
              ((SetterBoolean) setters.get(i)).set(mappedObject, Boolean.parseBoolean(value));
              break;
            case SHORT:
              ((SetterShort) setters.get(i)).set(mappedObject, Short.parseShort(value));
              break;
            case INTEGER:
              ((SetterInt) setters.get(i)).set(mappedObject, Integer.parseInt(value));
              break;
            case LONG:
              ((SetterLong) setters.get(i)).set(mappedObject, Long.parseLong(value));
              break;
            case FLOAT:
              ((SetterFloat) setters.get(i)).set(mappedObject, Float.parseFloat(value));
              break;
            case DOUBLE:
              ((SetterDouble) setters.get(i)).set(mappedObject, Double.parseDouble(value));
              break;
            default:
              break;
          }
        }
      }
      return mappedObject;
    } catch (Exception e) {
      DTThrowable.wrapIfChecked(e);
    }
    return null;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
  }

  public void processFirstTuple(Map<String, String> value) throws ClassNotFoundException
  {
    objectClass = Class.forName(getOutputClass());

    final int size = dataColumns.size();
    for (int i = 0; i < size; i++) {
      final SupportType type = dataColumns.get(i).getType();
      final String getterExpression = dataColumns.get(i).getPojoFieldExpression();
      final Object setter;
      switch (type) {
        case STRING:
          setter = PojoUtils.createSetter(objectClass, getterExpression, String.class);
          break;
        case BOOLEAN:
          setter = PojoUtils.createSetterBoolean(objectClass, getterExpression);
          break;
        case SHORT:
          setter = PojoUtils.createSetterShort(objectClass, getterExpression);
          break;
        case INTEGER:
          setter = PojoUtils.createSetterInt(objectClass, getterExpression);
          break;
        case LONG:
          setter = PojoUtils.createSetterLong(objectClass, getterExpression);
          break;
        case FLOAT:
          setter = PojoUtils.createSetterFloat(objectClass, getterExpression);
          break;
        case DOUBLE:
          setter = PojoUtils.createSetterDouble(objectClass, getterExpression);
          break;
        default:
          setter = PojoUtils.createSetter(objectClass, getterExpression, Object.class);
          break;
      }
      setters.add(setter);
    }
  }

  @Override
  public void processTuples()
  {
    for (String key : keys) {
      if (store.getType(key).equals("hash")) {
        Map<String, String> mapValue = store.getMap(key);
        if (isFirstTuple) {
          try {
            processFirstTuple(mapValue);
          } catch (ClassNotFoundException e) {
            DTThrowable.rethrow(e);
          }
        }
        isFirstTuple = false;
        outputPort.emit(new KeyValPair<String, Object>(key, convertMapToObject(mapValue)));
      }
    }
    keys.clear();
  }

  /*
   * Output class type
   */
  public String getOutputClass()
  {
    return outputClass;
  }

  public void setOutputClass(String outputClass)
  {
    this.outputClass = outputClass;
  }

  /*
   * An arraylist of data column names to be set in Redis store as a Map. Gets
   * column names, column expressions and column data types
   */
  public ArrayList<FieldInfo> getDataColumns()
  {
    return dataColumns;
  }

  public void setDataColumns(ArrayList<FieldInfo> dataColumns)
  {
    this.dataColumns = dataColumns;
  }

  @Override
  public KeyValPair<String, Object> convertToTuple(Map<Object, Object> o)
  {
    // Do nothing for the override, Scan already done in processTuples
    return null;
  }
}
