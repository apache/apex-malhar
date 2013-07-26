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

/**
 *  This operator provides sql select query semantic on live data stream. <br>
 *  Stream rows passing condition are emitted on output port stream. <br>
 *  <br>
 *  <b>StateFull : NO,</b> all row data is processed in current time window. <br>
 *  <b>Partitions : Yes, </b> No Input dependency among input rows. <br>
 *  <br>
 * <b>Ports</b>:<br>
 * <b> inport : </b> Input hash map(row) port, expects HashMap&lt;String,Object&gt;<<br>
 * <b> outport : </b> Output hash map(row) port, emits  HashMap&lt;String,Object&gt;<br>
 * <br>
 * <b> Properties : <b> <br>
 * <b> condition : </b> Select condition for selecting rows. <br>
 * <b> columns : </b> Column names/aggregate functions for select. <br>
 * <br>
 *
 * @since 0.3.3
 */
public class SelectOperator extends SqlOperator
{
	/**
	 * Select columns.
	 */
	private SelectIndex columns;
	
	/**
	 * Select condition.
	 */
	private SelectCondition condition;
	
  /**
   * Process rows operator.
   */
	@Override
	public ArrayList<HashMap<String, Object>> processRows(
			ArrayList<HashMap<String, Object>> rows)
	{
		// apply condition first  
		if (condition != null) {
			rows = condition.filetrValidRows(rows);
		}
		
		// apply index filter
		if (columns != null) {
			rows = columns.process(rows);
		}
		
		// done    
    return rows;
	}


	/**
	 * @return the condition
	 */
	public SelectCondition getCondition()
	{
		return condition;
	}

	/**
	 * @param condition the condition to set
	 */
	public void setCondition(SelectCondition condition)
	{
		this.condition = condition;
	}


	/**
	 * @return the columns
	 */
	public SelectIndex getColumns()
	{
		return columns;
	}


	/**
	 * @param columns the columns to set
	 */
	public void setColumns(SelectIndex columns)
	{
		this.columns = columns;
	}

}
