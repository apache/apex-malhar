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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.FieldInfo.SupportType;

public class TestPOJO
{
  public static List<FieldInfo> getFieldsInfo()
  {
    List<FieldInfo> fieldsInfo = new ArrayList<FieldInfo>();
    fieldsInfo.add( new FieldInfo( "name", "name", SupportType.STRING ) );
    fieldsInfo.add( new FieldInfo( "age", "age", SupportType.INTEGER ) );
    fieldsInfo.add( new FieldInfo( "address", "address", SupportType.STRING ) );
    
    return fieldsInfo;
  }
  
  public static String getRowExpression()
  {
    return "row";
  }
  
  public static TestPOJO from( Map<String,byte[]> map )
  {
    TestPOJO testPOJO = new TestPOJO();
    for( Map.Entry<String, byte[]> entry : map.entrySet() )
    {
      testPOJO.setValue(entry.getKey(), entry.getValue() );
    }
    return testPOJO;
  }
  
  private long rowId = 0;
  private String name;
  private int age;
  private String address;

  public TestPOJO(){}
  
  public TestPOJO(long rowId)
  {
    this(rowId, "name" + rowId, (int) rowId, "address" + rowId);
  }

  public TestPOJO(long rowId, String name, int age, String address)
  {
    this.rowId = rowId;
    setName(name);
    setAge(age);
    setAddress(address);
  }
  
  public void setValue( String fieldName, byte[] value )
  {
    if( "row".equalsIgnoreCase(fieldName) )
    {
      setRow( Bytes.toString(value) );
      return;
    }
    if( "name".equalsIgnoreCase(fieldName))
    {
      setName( Bytes.toString(value));
      return;
    }
    if( "address".equalsIgnoreCase(fieldName))
    {
      setAddress( Bytes.toString(value));
      return;
    }
    if( "age".equalsIgnoreCase(fieldName))
    {
      setAge( Bytes.toInt(value) );
      return;
    }
  }

  public String getRow()
  {
    return String.valueOf(rowId);
  }
  public void setRow( String row )
  {
    setRowId( Long.valueOf(row) );
  }
  public void setRowId( long rowId )
  {
    this.rowId = rowId;
  }
  
  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public Integer getAge()
  {
    return age;
  }

  public void setAge( Integer age)
  {
    this.age = age;
  }

  public String getAddress()
  {
    return address;
  }

  public void setAddress(String address)
  {
    this.address = address;
  }

  public boolean outputFieldsEquals( TestPOJO other )
  {
    if( other == null )
      return false;
    if( !fieldEquals( getName(), other.getName() ) )
      return false;
    if( !fieldEquals( getAge(), other.getAge() ) )
      return false;
    if( !fieldEquals( getAddress(), other.getAddress() ) )
      return false;
    return true;
  }
  
  public boolean completeEquals( TestPOJO other )
  {
    if( other == null )
      return false;
    if( !outputFieldsEquals( other ) )
      return false;
    if( !fieldEquals( getRow(), other.getRow() ) )
      return false;
    return true;
  }
  
  public <T> boolean fieldEquals( T v1, T v2 )
  {
    if( v1 == null && v2 == null )
      return true;
    if( v1 == null || v2 == null )
      return false;
    return v1.equals( v2 );
  }
  
  @Override
  public String toString()
  {
    return String.format( "id={%d}; name={%s}; age={%d}; address={%s}", rowId, name, age, address);
  }
}
