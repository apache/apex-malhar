package com.datatorrent.lib.db.jdbc;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.Map;

import com.google.common.collect.Maps;

import com.datatorrent.lib.util.FieldInfo;

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
