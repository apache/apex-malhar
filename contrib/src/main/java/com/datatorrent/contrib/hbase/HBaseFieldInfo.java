package com.datatorrent.contrib.hbase;

import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.contrib.util.FieldInfo;

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
