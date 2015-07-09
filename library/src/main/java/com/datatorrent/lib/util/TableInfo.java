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

import java.util.List;

import javax.validation.constraints.NotNull;

public class TableInfo< T extends FieldInfo >
{
  //the row or id expression
  private String rowOrIdExpression;

  //this class should be used in configuration which don't support Generic.
  @NotNull
  private List<T> fieldsInfo;

  /**
   * expression for Row or Id
   */
  public String getRowOrIdExpression()
  {
    return rowOrIdExpression;
  }

  /**
   * expression for Row or Id
   */
  public void setRowOrIdExpression(String rowOrIdExpression)
  {
    this.rowOrIdExpression = rowOrIdExpression;
  }

  /**
   * the field information. each field of the tuple related to on field info.
   */
  public List<T> getFieldsInfo()
  {
    return fieldsInfo;
  }

  /**
   * the field information. each field of the tuple related to on field info.
   */
  public void setFieldsInfo(List<T> fieldsInfo)
  {
    this.fieldsInfo = fieldsInfo;
  }
  
  
}
