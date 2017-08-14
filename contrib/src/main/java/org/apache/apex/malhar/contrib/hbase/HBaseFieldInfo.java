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
package org.apache.apex.malhar.contrib.hbase;

import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @since 3.1.0
 */

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
    switch (type) {
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
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

  public Object toValue( byte[] bytes )
  {
    final SupportType type = getType();
    switch (type) {
      case BOOLEAN:
        return Bytes.toBoolean( bytes );
      case SHORT:
        return Bytes.toShort( bytes );
      case INTEGER:
        return Bytes.toInt( bytes );
      case LONG:
        return Bytes.toLong( bytes );
      case FLOAT:
        return Bytes.toFloat( bytes );
      case DOUBLE:
        return Bytes.toDouble( bytes );
      case STRING:
        return Bytes.toString( bytes );
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

}
