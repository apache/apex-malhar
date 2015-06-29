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
import java.util.Map.Entry;

import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.FieldInfo.SupportType;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * This is a Redis output operator, which takes a Key and corresponding Plain
 * Old Java Object as input. And writes a Map out to Redis based on Expressions
 * provided.
 * <p>
 * This output adapter takes a Key value pair <key, POJO> as tuples and just
 * writes to the redis store with the key and the value is a Map containing
 * object attributes as <keys,value> Note: Redis output operator should never
 * use the passthrough method because it begins a transaction at beginWindow and
 * commits a transaction at endWindow, and a transaction in Redis blocks all
 * other clients.
 * </p>
 *
 * @displayName Redis POJO Output Operator
 * @category Store
 * @tags output operator, key value
 *
 */
public class RedisPOJOOutputOperator extends AbstractRedisAggregateOutputOperator<KeyValPair<String, Object>>
{
  protected final Map<String, Object> map = new HashMap<String, Object>();
  private ArrayList<FieldInfo> dataColumns;
  private transient ArrayList<Object> getters;
  private boolean isFirstTuple = true;

  public RedisPOJOOutputOperator()
  {
    super();
    getters = new ArrayList<Object>();
  }

  @Override
  public void storeAggregate()
  {
    for (Entry<String, Object> entry : map.entrySet()) {

      Map<String, String> mapObject = convertObjectToMap(entry.getValue());
      store.put(entry.getKey(), mapObject);
    }
  }

  private Map<String, String> convertObjectToMap(Object tuple)
  {

    Map<String, String> mappedObject = new HashMap<String, String>();
    for (int i = 0; i < dataColumns.size(); i++) {
      final SupportType type = dataColumns.get(i).getType();
      final String columnName = dataColumns.get(i).getColumnName();

      if (i < getters.size()) {
        Getter<Object, Object> obj = (Getter<Object, Object>) (getters.get(i));

        Object value = obj.get(tuple);
        mappedObject.put(columnName, value.toString());
      }
    }

    return mappedObject;
  }

  public void processFirstTuple(KeyValPair<String, Object> tuple)
  {
    // Create getters using first value entry in map
    // Entry<String, Object> entry= tuple.entrySet().iterator().next();
    Object value = tuple.getValue();

    final Class<?> fqcn = value.getClass();
    final int size = dataColumns.size();
    for (int i = 0; i < size; i++) {
      final SupportType type = dataColumns.get(i).getType();
      final String getterExpression = dataColumns.get(i).getPojoFieldExpression();
      final Object getter;
      switch (type) {
        case STRING:
          getter = PojoUtils.createGetter(fqcn, getterExpression, String.class);
          break;
        case BOOLEAN:
          getter = PojoUtils.createGetterBoolean(fqcn, getterExpression);
          break;
        case SHORT:
          getter = PojoUtils.createGetterShort(fqcn, getterExpression);
          break;
        case INTEGER:
          getter = PojoUtils.createGetter(fqcn, getterExpression, type.getJavaType());
          break;
        case LONG:
          getter = PojoUtils.createGetterLong(fqcn, getterExpression);
          break;
        case FLOAT:
          getter = PojoUtils.createGetterFloat(fqcn, getterExpression);
          break;
        case DOUBLE:
          getter = PojoUtils.createGetterDouble(fqcn, getterExpression);
          break;
        default:
          getter = PojoUtils.createGetter(fqcn, getterExpression, Object.class);
          break;
      }
      getters.add(getter);
    }
  }

  @Override
  public void processTuple(KeyValPair<String, Object> tuple)
  {
    if (isFirstTuple) {
      processFirstTuple(tuple);
    }

    isFirstTuple = false;
    map.put(tuple.getKey(), tuple.getValue());
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
}
