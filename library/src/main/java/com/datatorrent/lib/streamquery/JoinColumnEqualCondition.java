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

/**
 * <p>
 * Equal join condition class. 
 * This compares values of given keys in both row data.
 * <br>
 * <b> Properties : </b> <br>
 * <b. equalkeys : </b> Keys for which value must be compared. <br>
 * <br>
 */
public class JoinColumnEqualCondition  extends Condition
{

  /**
   * column names to be compared. 
   */
  private String column1;
  private String column2;
  public JoinColumnEqualCondition(String column1, String column2) {
    this.column1 = column1;
    this.column2 = column2;
  }
  
  @Override
  public boolean isValidRow(Map<String, Object> row)
  {
    return false;
  }

  @Override
  public boolean isValidJoin(Map<String, Object> row1, Map<String, Object> row2)
  {
    if ((column1 == null)||(column2 == null)) return false;
    if (!row1.containsKey(column1) || !row2.containsKey(column2)) return false;
    Object value1 = row1.get(column1);
    Object value2 = row2.get(column2);
    return value1.equals(value2);
  }
}
