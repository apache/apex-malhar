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

import java.util.HashSet;
import java.util.Iterator;
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
public class JoinEqualCondition  implements  JoinCondition
{
  /**
   * Equals keys. 
   */
	HashSet<String> equalKeys = new HashSet<String>();
	
	@Override
  public boolean isValidJoin(Map<String, Object> row1, Map<String, Object> row2)
  {
	  // no keys
		if (equalKeys.size() == 0) return true; 
		
		// check equality for each key
		Iterator<String> iter = equalKeys.iterator();
		while (iter.hasNext()) {
			String key = iter.next();
			Object val1 = row1.get(key);
			Object val2 = row2.get(key);
			if (val1 != null) {
				if (val2 == null) continue;
				else {
					if (!val1.equals(val2)) return false;
					continue;
				}
			} else {
				if (val2 == null) continue;
				else return false;
			}
		}
		
		// done
	  return true;
  }

	/**
	 * Add equal key.
	 */
	public void addEqualKey(String key) {
		equalKeys.add(key);
	}
}
