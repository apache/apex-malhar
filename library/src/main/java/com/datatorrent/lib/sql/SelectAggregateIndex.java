package com.datatorrent.lib.sql;

import java.util.ArrayList;
import java.util.HashMap;

public abstract class SelectAggregateIndex implements SelectIndex
{
  /**
   * Column name. 
   */
	private String columnName;
	
	/**
	 * Column alias name.
	 */
	private String columnAlias;
	
	/**
	 * Abstract aggregate function, body must be implemented by sub class.
	 * @param rows of data.
	 * @return aggregate value for given column name.
	 */
	 abstract public Object aggregate(ArrayList<HashMap<String, Object>> rows);
	
	/*
   * This operator aggregates column value. 
   * @param rows Array for row hash map.
   * @return Array containing one element column/aggregate hash amp.
	 */
	@Override
	public ArrayList<HashMap<String, Object>> process(
			ArrayList<HashMap<String, Object>> rows)
	{
		Object aggrVal = aggregate(rows);
		if ((columnAlias == null)||(columnAlias.length() == 0)) {
			columnAlias = columnName;
		}
		HashMap<String, Object> item = new HashMap<String, Object>();
		item.put(columnAlias, aggrVal);
		ArrayList<HashMap<String, Object>> result = new ArrayList<HashMap<String, Object>>();
		result.add(item);
		return result;
	}

	public String getColumnName()
	{
		return columnName;
	}

	public void setColumnName(String columnName)
	{
		this.columnName = columnName;
	}

	public String getColumnAlias()
	{
		return columnAlias;
	}

	public void setColumnAlias(String columnAlias)
	{
		this.columnAlias = columnAlias;
	}

}
