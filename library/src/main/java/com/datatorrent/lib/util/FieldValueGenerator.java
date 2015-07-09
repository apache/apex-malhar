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
package com.datatorrent.lib.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.Setter;

public class FieldValueGenerator<T extends FieldInfo>
{
  public static interface FieldValueHandler<T extends FieldInfo>
  {
    public void handleFieldValue( T fieldInfo, Object value );
  }
  
  private static final Logger logger = LoggerFactory.getLogger( FieldValueGenerator.class );
  protected Class<?> clazz;
  protected Map<T, Getter<Object,Object>> fieldGetterMap = null;
  protected Map<T, Setter<Object,Object>> fieldSetterMap = null;
  

  public FieldValueGenerator(){}

  @SuppressWarnings("unchecked")
  public static <T extends FieldInfo> FieldValueGenerator<T> getFieldValueGenerator(final Class<?> clazz, List<T> fieldInfos)
  {
    FieldValueGenerator<T> instance = new FieldValueGenerator<T>();
    return getFieldValueGenerator( clazz, fieldInfos, instance );
  }
  
  public static < T extends FieldInfo, I extends FieldValueGenerator<T> > I getFieldValueGenerator(final Class<?> clazz, List<T> fieldInfos, I instance)
  {
    instance.clazz = clazz;

    if( fieldInfos != null )
    {
      instance.fieldGetterMap = new HashMap<T,Getter<Object,Object>>();
      for( T fieldInfo : fieldInfos )
      {
        @SuppressWarnings("unchecked")
        Getter<Object,Object> getter = PojoUtils.createGetter(clazz, fieldInfo.getPojoFieldExpression(), fieldInfo.getType().getJavaType());
        instance.fieldGetterMap.put( fieldInfo, getter );
      }
      
      instance.fieldSetterMap = new HashMap<T,Setter<Object,Object>>();
      for( T fieldInfo : fieldInfos )
      {
        @SuppressWarnings("unchecked")
        Setter<Object,Object> setter = PojoUtils.createSetter(clazz, fieldInfo.getPojoFieldExpression(), fieldInfo.getType().getJavaType());
        instance.fieldSetterMap.put( fieldInfo, setter );
      }
    }
    
    return instance;
  }

  /**
   * use FieldValueHandler handle the value
   *
   * @param obj
   * @param fieldValueHandler
   */
  @SuppressWarnings("unchecked")
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

  /**
   * 
   * @param obj
   * @return a map from columnName to columnValue
   */
  public Map<String, Object> getFieldsValueAsMap( Object obj )
  {
    Map< String, Object > fieldsValue = new HashMap< String, Object>();
    for( Map.Entry< T, Getter<Object,Object>> entry : fieldGetterMap.entrySet() )
    {
      Getter<Object,Object> getter = entry.getValue();
      if( getter != null )
      {
        Object value = getter.get(obj);
        fieldsValue.put(entry.getKey().getColumnName(), value);
      }
    }
    return fieldsValue;
  }
}
