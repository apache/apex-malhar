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
 * A {@link FieldInfo} object for Jdbc.
 * Includes an SQL type for each field.
 */
public class JdbcFieldInfo extends FieldInfo
{
  private int sqlType;
  private Map<String, Integer> jdbcTypes;

  public JdbcFieldInfo(String columnName, String pojoFieldExpression, SupportType type, String sqlTypeStr)
  {
    super(columnName, pojoFieldExpression, type);

    jdbcTypes = Maps.newHashMap();
    for (Field f : Types.class.getFields()) {
      try {
        jdbcTypes.put(f.getName(), f.getInt(null));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    sqlType = jdbcTypes.get(sqlTypeStr);
  }

  public int getSqlType()
  {
    return sqlType;
  }

  public void setSqlType(int sqlType)
  {
    this.sqlType = sqlType;
  }
}
