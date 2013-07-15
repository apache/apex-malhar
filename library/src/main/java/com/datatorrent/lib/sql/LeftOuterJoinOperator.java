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
package com.datatorrent.lib.sql;

/**
 * This operator provides sql left outer join operation semantic on live stream. <br>
 * Please refer to {@link com.datatorrent.lib.sql.InnerJoinOperator} for details.
 */
public class LeftOuterJoinOperator extends InnerJoinOperator
{
	@Override
  public void endWindow()
  {
		// Check join of each row
		for (int i=0; i < table1.size(); i++) {
			boolean merged = false;
			for (int j=0; j < table2.size(); j++) {
				if ((joinCondition == null) || (joinCondition.isValidJoin(table1.get(i), table2.get(j)))) {
					joinRows(table1.get(i), table2.get(j));
					merged = true;
				} 
			}
			if (!merged) {
				joinRows(table1.get(i), null);
			}
		}
  }
}
