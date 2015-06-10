package com.datatorrent.contrib.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

public class FieldValueGenerator< T extends FieldInfo >
{
	private Map<T, Getter<Object,Object>> fieldGetterMap = new HashMap<T,Getter<Object,Object>>();
	
	private FieldValueGenerator(){}
	
	@SuppressWarnings("unchecked")
  public static < T extends FieldInfo > FieldValueGenerator<T> getFieldValueGenerator(final Class<?> clazz, List<T> fieldInfos)
  {
    FieldValueGenerator<T> fieldValueGenerator = new FieldValueGenerator<T>();
    
    for( T fieldInfo : fieldInfos )
    {
    	Getter<Object,Object> getter = PojoUtils.createGetter(clazz, fieldInfo.getColumnExpression(), fieldInfo.getType().getJavaType() );
    	fieldValueGenerator.fieldGetterMap.put( fieldInfo, getter );
    }
    return fieldValueGenerator;
  }
	
	/**
	 * 
	 * @param obj
	 * @return a map from FieldInfo to columnValue
	 */
	public Map< T, Object > getFieldsValue( Object obj )
	{
		Map< T, Object > fieldsValue = new HashMap< T, Object>();
		for( Map.Entry< T, Getter<Object,Object>> entry : fieldGetterMap.entrySet() )
		{
			Getter<Object,Object> getter = entry.getValue();
			if( getter != null )
			{
				Object value = getter.get(obj);
				fieldsValue.put(entry.getKey(), value);
			}
		}
		return fieldsValue;
	}
}
