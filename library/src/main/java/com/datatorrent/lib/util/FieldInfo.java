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
package com.datatorrent.lib.util;

import javax.validation.constraints.NotNull;

@SuppressWarnings("rawtypes")
public class FieldInfo
{
  // Columns name set by user.
  @NotNull
  private String columnName;

  // Expressions set by user to get field values from input tuple.
  @NotNull
  private String pojoFieldExpression;

  private SupportType type;

  public FieldInfo()
  {
  }

  public FieldInfo(String columnName, String pojoFieldExpression, SupportType type)
  {
    setColumnName(columnName);
    setPojoFieldExpression(pojoFieldExpression);
    setType(type);
  }

  /**
   * the column name which keep this field.
   */
  public String getColumnName()
  {
    return columnName;
  }

  public void setColumnName(String columnName)
  {
    this.columnName = columnName;
  }

  /**
   * Java expressions that will generate the column value from the POJO.
   * 
   */
  public String getPojoFieldExpression()
  {
    return pojoFieldExpression;
  }

  /**
   * Java expressions that will generate the column value from the POJO.
   * 
   */
  public void setPojoFieldExpression(String expression)
  {
    this.pojoFieldExpression = expression;
  }

  /**
   * the columnName should not duplicate( case-insensitive )
   */
  @Override
  public int hashCode()
  {
    return columnName.toLowerCase().hashCode();
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null || !(obj instanceof FieldInfo))
      return false;
    return columnName.equalsIgnoreCase(((FieldInfo) obj).getColumnName());
  }

  /**
   * the Java type of the column
   */
  public SupportType getType()
  {
    return type;
  }

  /**
   * the Java type of the column
   */
  public void setType(SupportType type)
  {
    this.type = type;
  }

  public static enum SupportType {
    BOOLEAN(Boolean.class), SHORT(Short.class), INTEGER(Integer.class), LONG(Long.class), FLOAT(Float.class), DOUBLE(Double.class), STRING(String.class);

    private Class javaType;

    private SupportType(Class javaType)
    {
      this.javaType = javaType;
    }

    public Class getJavaType()
    {
      return javaType;
    }
  }

}
