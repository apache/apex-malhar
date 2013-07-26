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
import java.util.TreeMap;

/**
 *  Class for implementing order by key name rule. <br>
 *
 * @since 0.3.3
 */
@SuppressWarnings("rawtypes")
public class OrderByRule<T extends Comparable> {
	/**
	 * column name.
	 */
	private String columnName;

	public OrderByRule(String name) {
		columnName = name;	
	}
	
	/**
	 * sort rows.
	 */
	@SuppressWarnings("unchecked")
	public ArrayList<HashMap<String, Object>> sort(
			ArrayList<HashMap<String, Object>> rows) {
		TreeMap<T, ArrayList<HashMap<String, Object>>> sorted = new TreeMap<T, ArrayList<HashMap<String, Object>>>();
        for (int i=0; i < rows.size(); i++) {
        	HashMap<String, Object> row = rows.get(i);
        	if (row.containsKey(columnName)) {
        		T value = (T) row.get(columnName);
        		ArrayList<HashMap<String, Object>> list;
        		if (sorted.containsKey(value)) {
        			list = sorted.get(value);
        		} else {
        			list = new ArrayList<HashMap<String, Object>>();
        			sorted.put(value, list);
        		}
        		list.add(row);
        	} 
        }
        ArrayList<HashMap<String, Object>> result = new ArrayList<HashMap<String, Object>>();
        for (Map.Entry<T, ArrayList<HashMap<String, Object>>> entry : sorted.entrySet()) {
        	result.addAll(entry.getValue());
        }
		return result;
	}

	/**
	 * @return the columnName
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * @param columnName
	 *            the columnName to set
	 */
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
}
