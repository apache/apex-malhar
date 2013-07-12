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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 *  Sql Base Operator, select/update/delete are derived from this operator.
 */
public abstract class SqlOperator implements Operator
{
	/**
	 *  table rows.
	 */
	ArrayList<HashMap<String, Object>> rows;
	
  /**
   * Input port.
   */
	public final transient DefaultInputPort<HashMap<String, Object>> inport = new DefaultInputPort<HashMap<String, Object>>() {
		@Override
		public void process(HashMap<String, Object> tuple)
		{
			rows.add(tuple);
		}
	};
	
	/**
	 * Output port.
	 */
	public final transient DefaultOutputPort<HashMap<String, Object>> outport = 
			new DefaultOutputPort<HashMap<String, Object>>();
	
	@Override
	public void setup(OperatorContext context)
	{
	}

	/**
	 * Process rows function to be implemented by sub class. 
	 */
	abstract public ArrayList<HashMap<String, Object>> processRows(ArrayList<HashMap<String, Object>> rows);
	
	@Override
	public void teardown()
	{
	}

	@Override
	public void beginWindow(long windowId)
	{
		rows = new ArrayList<HashMap<String, Object>>();
	}

	@Override
	public void endWindow()
	{
		rows = processRows(rows);
		for (int i=0; i < rows.size(); i++) {
		    outport.emit(rows.get(i));
		}
	}
}
