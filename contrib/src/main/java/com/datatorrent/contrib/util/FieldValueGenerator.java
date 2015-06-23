/**
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
 *
 */
package com.datatorrent.contrib.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

public class FieldValueGenerator< T extends FieldInfo >
{
  public static interface FieldValueHandler<T extends FieldInfo>
  {
    public void handleFieldValue( T fieldInfo, Object value );
  }
  
	private Map<T, Getter<Object,Object>> fieldGetterMap = new HashMap<T,Getter<Object,Object>>();
	
	private FieldValueGenerator(){}
	
	@SuppressWarnings("unchecked")
  public static < T extends FieldInfo > FieldValueGenerator<T> getFieldValueGenerator(final Class<?> clazz, List<T> fieldInfos)
  {
	  if( fieldInfos == null )
	    return null;
	  
    FieldValueGenerator<T> fieldValueGenerator = new FieldValueGenerator<T>();
    
    for( T fieldInfo : fieldInfos )
    {
    	Getter<Object,Object> getter = PojoUtils.createGetter(clazz, fieldInfo.getColumnExpression(), fieldInfo.getType().getJavaType() );
    	fieldValueGenerator.fieldGetterMap.put( fieldInfo, getter );
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
	
	 /**
   * 
   * @param obj
   * @return a map from columnName to columnValue
   */
  public Map< String, Object > getFieldsValueAsMap( Object obj )
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
