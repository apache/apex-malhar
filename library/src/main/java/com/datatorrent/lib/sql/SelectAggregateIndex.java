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

import java.util.ArrayList;
import java.util.HashMap;

/**
 * 
 * Abstract to class implements {@link SelectIndex} interface.  <br>
 * Abstract function 'aggregate' must be implemented by sub class. <br>.
 */
public abstract class SelectAggregateIndex implements SelectIndex
{
  /**
   * Column name. 
   */
	protected String columnName;
	
	/**
	 * Column alias name.
	 */
	protected String columnAlias;
	
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

	public void addNameAlias(String columnName, String columnAlias) {
		this.columnName = columnName;
		this.columnAlias = columnAlias;
	}
}
