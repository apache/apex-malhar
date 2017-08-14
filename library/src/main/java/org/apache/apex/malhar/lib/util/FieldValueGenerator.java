/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.apex.malhar.lib.util.PojoUtils.Getter;
import org.apache.apex.malhar.lib.util.PojoUtils.Setter;

/**
 * @since 3.3.0
 */
public class FieldValueGenerator<T extends FieldInfo>
{
  public static interface FieldValueHandler<T extends FieldInfo>
  {
    public void handleFieldValue(T fieldInfo, Object value);
  }

  public static interface ValueConverter<T extends FieldInfo>
  {
    public Object convertValue(T fieldInfo, Object value);
  }

  protected Map<T, Getter<Object, Object>> fieldGetterMap = new HashMap<T, Getter<Object, Object>>();
  protected Map<T, Setter<Object, Object>> fieldSetterMap = new HashMap<T, Setter<Object, Object>>();

  protected Map<String, T> fieldInfoMap = new HashMap<String, T>();

  protected FieldValueGenerator()
  {
  }

  @SuppressWarnings("unchecked")
  public static <T extends FieldInfo> FieldValueGenerator<T> getFieldValueGenerator(final Class<?> clazz, List<T> fieldInfos)
  {
    return new FieldValueGenerator<T>(clazz, fieldInfos);
  }

  protected FieldValueGenerator(final Class<?> clazz, List<T> fieldInfos)
  {
    for (T fieldInfo : fieldInfos) {
      fieldInfoMap.put(fieldInfo.getColumnName(), fieldInfo);

      Getter<Object, Object> getter = PojoUtils.createGetter(clazz, fieldInfo.getPojoFieldExpression(), fieldInfo.getType().getJavaType());
      fieldGetterMap.put(fieldInfo, getter);
    }


    for (T fieldInfo : fieldInfos) {
      Setter<Object, Object> setter = PojoUtils.createSetter(clazz, fieldInfo.getPojoFieldExpression(), fieldInfo.getType().getJavaType());
      fieldSetterMap.put(fieldInfo, setter);
    }
  }

  /**
   * use FieldValueHandler handle the value
   * @param obj
   * @param fieldValueHandler
   * @return
   * @since 3.0.0
 */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void handleFieldsValue(Object obj, FieldValueHandler fieldValueHandler)
  {
    for (Map.Entry<T, Getter<Object, Object>> entry : fieldGetterMap.entrySet()) {
      Getter<Object, Object> getter = entry.getValue();
      if (getter != null) {
        Object value = getter.get(obj);
        fieldValueHandler.handleFieldValue(entry.getKey(), value);
      }
    }
  }

  public Map<String, Object> getFieldsValueAsMap(Object obj)
  {
    Map<String, Object> fieldsValue = new HashMap<String, Object>();
    for (Map.Entry<T, Getter<Object, Object>> entry : fieldGetterMap.entrySet()) {
      Getter<Object, Object> getter = entry.getValue();
      if (getter != null) {
        Object value = getter.get(obj);
        fieldsValue.put(entry.getKey().getColumnName(), value);
      }
    }
    return fieldsValue;
  }

  public void setColumnValue(Object instance, String columnName, Object value, ValueConverter<T> valueConverter)
  {
    T fieldInfo = fieldInfoMap.get(columnName);
    Setter<Object, Object> setter = fieldSetterMap.get(fieldInfo);
    setter.set(instance, valueConverter == null ? value : valueConverter.convertValue(fieldInfo, value));
  }
}
