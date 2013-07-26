/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.streamquery;

import java.util.Map;

public class ColumnIndex implements Index
{
  /**
   * Column/alias name.
   */
  protected String column = null;
  protected String alias = null;
  
  public ColumnIndex(String column, String alias) 
  {
    this.column = column;
    this.alias = alias;
  }
  
  @Override
  public void filter(Map<String, Object> row, Map<String, Object> collect)
  {
    if (column == null) return;
    String name = column;
    if (alias != null) name = alias;
    collect.put(name, row.get(name));
  }
}
