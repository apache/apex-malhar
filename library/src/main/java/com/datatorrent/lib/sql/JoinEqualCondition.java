package com.datatorrent.lib.sql;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

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
