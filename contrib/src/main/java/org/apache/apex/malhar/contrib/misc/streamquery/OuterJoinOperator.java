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
package org.apache.apex.malhar.contrib.misc.streamquery;

/**
 * An operator that provides sql left,right and full outer join metric semantics on live stream. <br>
 * <p>
 * Please refer to {@link org.apache.apex.malhar.lib.misc.streamquery.InnerJoinOperator} for
 * details.
 *
 * <b> Properties : </b> <br>
 * <b> isLeftJoin : </b> Left join flag. <br>
 * <b> isFullJoin : </b> Full join flag. <br>
 * @displayName Outer Join
 * @category Stream Manipulators
 * @tags sql, outer join operator
 * @since 0.3.4
 * @deprecated
 */
@Deprecated
public class OuterJoinOperator extends InnerJoinOperator
{

  private boolean isLeftJoin = true;
  private boolean isFullJoin = false;

  @Override
  public void endWindow()
  {
    // full outer join
    if (isFullJoin) {
      for (int i = 0; i < table1.size(); i++) {
        boolean merged = false;
        for (int j = 0; j < table2.size(); j++) {
          if ((joinCondition == null)
              || (joinCondition.isValidJoin(table1.get(i), table2.get(j)))) {
            merged = true;
          }
        }
        if (!merged) {
          joinRows(table1.get(i), null);
        }
      }
      for (int i = 0; i < table2.size(); i++) {
        boolean merged = false;
        for (int j = 0; j < table1.size(); j++) {
          if ((joinCondition == null)
              || (joinCondition.isValidJoin(table1.get(j), table2.get(i)))) {
            merged = true;
          }
        }
        if (!merged) { // only output non merged rows
          joinRows(null, table2.get(i));
        }
      }
      return;
    }

    // left or right join
    if (isLeftJoin) {
      for (int i = 0; i < table1.size(); i++) {
        boolean merged = false;
        for (int j = 0; j < table2.size(); j++) {
          if ((joinCondition == null)
              || (joinCondition.isValidJoin(table1.get(i), table2.get(j)))) {
            merged = true;
          }
        }
        if (!merged) {
          joinRows(table1.get(i), null);
        }
      }
    } else {
      for (int i = 0; i < table2.size(); i++) {
        boolean merged = false;
        for (int j = 0; j < table1.size(); j++) {
          if ((joinCondition == null) || (joinCondition.isValidJoin(table1.get(j), table2.get(i)))) {
            merged = true;
          }
        }
        if (!merged) { // only output non merged rows
          joinRows(null, table2.get(i));
        }
      }
    }
  }

  public void setLeftJoin()
  {
    isLeftJoin = true;
  }

  public void setRighttJoin()
  {
    isLeftJoin = false;
  }

  public boolean isFullJoin()
  {
    return isFullJoin;
  }

  public void setFullJoin(boolean isFullJoin)
  {
    this.isFullJoin = isFullJoin;
  }
}
