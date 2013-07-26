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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>SelectColsIndex class.</p>
 *
 * @since 0.3.3
 */
public class SelectColsIndex implements SelectIndex
{
	/**
	 * Column names for filtering row. 
	 */
	private HashMap<String, String>  columns = new HashMap<String, String>();
	
	/**
	 * Add column name.
	 * @param column column to be selected.
	 * @param alais  result name alias 
	 */
	public void addColumn(String column, String alias) 
	{
		columns.put(column, alias);
	}

	@Override
	public ArrayList<HashMap<String, Object>> process(
			ArrayList<HashMap<String, Object>> rows)
	{
		// no column names  
		if (columns.size() == 0) return rows;
		
		// get result  
		ArrayList<HashMap<String, Object>> result = new ArrayList<HashMap<String, Object>>();
		for(HashMap<String, Object> row : rows) {
			HashMap<String, Object> resRow = new HashMap<String, Object>();
			for(Map.Entry<String, Object> entry : row.entrySet()) {
				if (columns.containsKey(entry.getKey())) {
					String alias = columns.get(entry.getKey());
					if (alias == null) alias = entry.getKey();
					resRow.put(alias,  entry.getValue());
				}
			}
			result.add(resRow);
		}
		return result;
	}
}
