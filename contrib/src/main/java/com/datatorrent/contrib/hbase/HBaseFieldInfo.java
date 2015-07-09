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
package com.datatorrent.contrib.hbase;

import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.lib.util.FieldInfo;

public class HBaseFieldInfo extends FieldInfo
{
	private String familyName;
	
	public HBaseFieldInfo()
	{
	}
	
	public HBaseFieldInfo( String columnName, String columnExpression, SupportType type, String familyName )
	{
	  super( columnName, columnExpression, type );
	  setFamilyName( familyName );
	}

	public String getFamilyName()
	{
		return familyName;
	}

	public void setFamilyName(String familyName)
	{
		this.familyName = familyName;
	}
	
	public byte[] toBytes( Object value )
	{
		final SupportType type = getType();
		switch( type )
		{
		case BOOLEAN:
		  return Bytes.toBytes( (Boolean)value );
		  
		case SHORT:
		  return Bytes.toBytes( (Short)value );
		  
		case INTEGER:
		  return Bytes.toBytes( (Integer)value );
		  
		case LONG:
		  return Bytes.toBytes( (Long)value );
		  
		case FLOAT:
		  return Bytes.toBytes( (Float)value );
		  
		case DOUBLE:
		  return Bytes.toBytes( (Double)value );
		  
		case STRING:
		  return Bytes.toBytes( (String)value );
		}
		throw new IllegalArgumentException( "Unsupported type: " + type );
	}
	
}
