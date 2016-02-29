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
package com.datatorrent.lib.db.jdbc;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.Map;

import com.google.common.collect.Maps;

import com.datatorrent.lib.util.FieldInfo;

/**
 * A {@link FieldInfo} object for Jdbc. <br/>
 * Includes an SQL type for each field. <br/>
 * An {@link FieldInfo} object used for JDBC output sources must have the SQL data types.
 * This is needed to create correct getters and setters for the POJO,
 * as well as setting the right parameter types in the JDBC prepared statement.
 */
public class JdbcFieldInfo extends FieldInfo
{
  private int sqlType;

  public JdbcFieldInfo(String columnName, String pojoFieldExpression, SupportType type, String sqlTypeStr)
  {
    super(columnName, pojoFieldExpression, type);

    sqlType = JdbcTypes.getSqlDataType(sqlTypeStr);
  }

  public int getSqlType()
  {
    return sqlType;
  }

  public void setSqlType(int sqlType)
  {
    this.sqlType = sqlType;
  }

  private static class JdbcTypes
  {
    private static Map<String, Integer> jdbcTypes;

    static {
      jdbcTypes = Maps.newHashMap();
      for (Field f : Types.class.getFields()) {
        try {
          jdbcTypes.put(f.getName(), f.getInt(null));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    public static int getSqlDataType(String sqlTypeStr)
    {
      return jdbcTypes.get(sqlTypeStr);
    }
  }
}
