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
 * This operator provides sql right outer join operation semantic on live stream. <br>
 * Please refer to {@link com.datatorrent.lib.sql.InnerJoinOperator} for details.
 */
public class RightOuterJoinOperator extends InnerJoinOperator
{
	@Override
  public void endWindow()
  {
		// Check join of each row
		for (int i=0; i < table2.size(); i++) {
			boolean merged = false;
			for (int j=0; j < table1.size(); j++) {
				if ((joinCondition == null) || (joinCondition.isValidJoin(table1.get(j), table2.get(i)))) {
					joinRows(table1.get(j), table2.get(i));
					merged = true;
				} 
			}
			if (!merged) {
				joinRows(null, table2.get(i));
			}
		}
  }
}
