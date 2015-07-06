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
package com.datatorrent.contrib.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.Setter;

public class FieldValueGenerator<T extends FieldInfo>
{
  public static interface FieldValueHandler<T extends FieldInfo>
  {
    public void handleFieldValue( T fieldInfo, Object value );
  }
  
  private static final Logger logger = LoggerFactory.getLogger( FieldValueGenerator.class );
  private Class<?> clazz;
  private Map<T, Getter<Object,Object>> fieldGetterMap = null;
  private Map<T, Setter<Object,Object>> fieldSetterMap = null;

  private FieldValueGenerator(){}
  
  @SuppressWarnings("unchecked")
  public static <T extends FieldInfo> FieldValueGenerator<T> getFieldValueGenerator(final Class<?> clazz, List<T> fieldInfos)
  {
    FieldValueGenerator<T> fieldValueGenerator = new FieldValueGenerator<T>();
    fieldValueGenerator.clazz = clazz;
    
    if( fieldInfos != null )
    {
      fieldValueGenerator.fieldGetterMap = new HashMap<T,Getter<Object,Object>>();
      for( T fieldInfo : fieldInfos )
      {
        Getter<Object,Object> getter = PojoUtils.createGetter(clazz, fieldInfo.getColumnExpression(), fieldInfo.getType().getJavaType());
        fieldValueGenerator.fieldGetterMap.put( fieldInfo, getter );
      }
      
      fieldValueGenerator.fieldSetterMap = new HashMap<T,Setter<Object,Object>>();
      for( T fieldInfo : fieldInfos )
      {
        Setter<Object,Object> setter = PojoUtils.createSetter(clazz, fieldInfo.getColumnExpression(), fieldInfo.getType().getJavaType());
        fieldValueGenerator.fieldSetterMap.put( fieldInfo, setter );
      }
    }
    
    return fieldValueGenerator;
  }


  /**
   * get the object which is serialized.
   * this method will convert the object into a map from column name to column value and then serialize it
   *
   * @param obj
   * @return
   */
  public Object getFieldValues(Object obj)
  {
    //if don't have field information, just convert the whole object to byte[]
    Object convertObj = obj;

    //if fields are specified, convert to map and then convert map to byte[]
    if( fieldGetterMap != null && !fieldGetterMap.isEmpty() )
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
      convertObj = fieldsValue;
    }

    return convertObj;
  }

  public Object getObjectFromValues(Object obj)
  {
    if( fieldGetterMap == null || fieldGetterMap.isEmpty() )
      return obj;

    // the obj in fact is a map, convert from map to object
    try
    {
      Map valueMap = (Map)obj;
      obj = clazz.newInstance();

      for( Map.Entry< T, Setter<Object,Object>> entry : fieldSetterMap.entrySet() )
      {
        T fieldInfo = entry.getKey();
        Setter<Object,Object> setter = entry.getValue();
        if( setter != null )
        {
          setter.set(obj, valueMap.get( fieldInfo.getColumnName() ));
        }
      }
      return obj;
    }
    catch( Exception e )
    {
      logger.warn( "Coverting map to obj exception. ", e );
      return obj;
    }
  }

	/**
	 * use FieldValueHandler handle the value
	 * @param obj
	 * @param fieldValueHandler
	 * @return
	 */
	public void handleFieldsValue( Object obj,  FieldValueHandler fieldValueHandler )
	{
		for( Map.Entry< T, Getter<Object,Object>> entry : fieldGetterMap.entrySet() )
		{
			Getter<Object,Object> getter = entry.getValue();
			if( getter != null )
			{
				Object value = getter.get(obj);
				fieldValueHandler.handleFieldValue(entry.getKey(), value);
			}
		}
	}
}
