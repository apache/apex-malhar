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

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.Setter;

public class FieldValueGenerator< T extends FieldInfo >
{
  public static interface FieldValueHandler<T extends FieldInfo>
  {
    public void handleFieldValue( T fieldInfo, Object value );
  }
  
  public static interface ValueConverter<T extends FieldInfo>
  {
    public Object convertValue( T fieldInfo, Object value );
  }
  
	private Map<T, Getter<Object,Object>> fieldGetterMap = new HashMap<T,Getter<Object,Object>>();
	private Map<T, Setter<Object,Object>> fieldSetterMap = new HashMap<T,Setter<Object,Object>>();
	
	private Map<String, T > fieldInfoMap = new HashMap<String, T >();
	
	private FieldValueGenerator(){}
	
	@SuppressWarnings("unchecked")
  public static < T extends FieldInfo > FieldValueGenerator<T> getFieldValueGenerator(final Class<?> clazz, List<T> fieldInfos)
  {
    FieldValueGenerator<T> fieldValueGenerator = new FieldValueGenerator<T>();
    
    for( T fieldInfo : fieldInfos )
    {
      fieldValueGenerator.fieldInfoMap.put( fieldInfo.getColumnName(), fieldInfo );
      
    	Getter<Object,Object> getter = PojoUtils.createGetter(clazz, fieldInfo.getColumnExpression(), fieldInfo.getType().getJavaType() );
    	fieldValueGenerator.fieldGetterMap.put( fieldInfo, getter );
    }
    
    
    for( T fieldInfo : fieldInfos )
    {
      Setter<Object,Object> setter = PojoUtils.createSetter(clazz, fieldInfo.getColumnExpression(), fieldInfo.getType().getJavaType() );
      fieldValueGenerator.fieldSetterMap.put( fieldInfo, setter );
    }
  
    return fieldValueGenerator;
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
	
	public void setColumnValue( Object instance, String columnName, Object value, ValueConverter<T> valueConverter )
	{
	  T fieldInfo = fieldInfoMap.get(columnName);
	  Setter<Object,Object> setter = fieldSetterMap.get( fieldInfo );
	  setter.set(instance, valueConverter == null ? value : valueConverter.convertValue( fieldInfo, value ) );
	}
}
