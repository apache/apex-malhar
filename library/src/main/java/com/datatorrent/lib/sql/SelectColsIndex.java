package com.datatorrent.lib.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
