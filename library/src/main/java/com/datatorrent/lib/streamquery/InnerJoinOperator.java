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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 * This operator reads table row data from 2 table data input ports. <br>
 * Operator joins row on given condition and selected names, emits 
 * joined result at output port.
 *  <br>
 *  <b>StateFull : NO,</b> all row data is processed in current time window. <br>
 *  <b>Partitions : No, </b> will yield wrong result(s). <br>
 *  <br>
 *  <b>Ports : </b> <br>
 *  <b> inport1 : </b> Input port for table 1, expects HashMap&lt;String, Object&gt; <br>
 *  <b> inport1 : </b> Input port for table 2, expects HashMap&lt;String, Object&gt; <br>
 *  <b> outport : </b> Output joined row port, emits HashMap&lt;String, ArrayList&lt;Object&gt;&gt; <br>
 *  <br>
 *  <b> Properties : </b>
 *  <b> joinCondition : </b> Join condition for table rows. <br>
 *  <b> table1Columns : </b> Columns to be selected from table1. <br>
 *  <b> table2Columns : </b> Columns to be selected from table2. <br>
 *  <br>
 */
public class InnerJoinOperator  implements Operator
{
	/**
	 * Join Condition; 
	 */
	protected JoinCondition joinCondition;
	
	/**
	 * Table1 select columns.
	 */
	protected HashMap<String, String> table1Columns = new HashMap<String, String>();
	
	/**
	 * Table2 select columns.
	 */
	protected HashMap<String, String> table2Columns = new HashMap<String, String>();
	
  /**
   * Collect data rows from input port 1.
   */
	protected ArrayList<Map<String, Object>> table1;
	
	
	/**
	 * Collect data from input port 2. 
	 */
	protected ArrayList<Map<String, Object>> table2;
	
  /**
   * Input port 1.
   */
	public final transient DefaultInputPort<Map<String, Object>> inport1 = new DefaultInputPort<Map<String, Object>>() {
		@Override
		public void process(Map<String, Object> tuple)
		{
      table1.add(tuple);
		}
	};
	
	/**
	 * Input port 2. 
	 */
	public final transient DefaultInputPort<Map<String, Object>> inport2 = new DefaultInputPort<Map<String, Object>>() {
		@Override
		public void process(Map<String, Object> tuple)
		{
	    table2.add(tuple);
		}
	};
	
	/**
	 * Output port.
	 */
	public final transient DefaultOutputPort<HashMap<String, ArrayList<Object>>> outport =  
			new DefaultOutputPort<HashMap<String,  ArrayList<Object>>>();
	
	@Override
  public void setup(OperatorContext arg0)
  { 
  }

	@Override
  public void teardown()
  { 
  }

	@Override
  public void beginWindow(long arg0)
  { 
		table1 = new ArrayList<Map<String, Object>>();
		table2 = new ArrayList<Map<String, Object>>();
  }

	@Override
  public void endWindow()
  {
		// Check join of each row
		for (int i=0; i < table1.size(); i++) {
			for (int j=0; j < table2.size(); j++) {
				if ((joinCondition == null) || (joinCondition.isValidJoin(table1.get(i), table2.get(j)))) {
					joinRows(table1.get(i), table2.get(j));
				}
			}
		}
  }

	/**
   * @return the joinCondition
   */
  public JoinCondition getJoinCondition()
  {
	  return joinCondition;
  }

	/**
   * @param set joinCondition
   */
  public void setJoinCondition(JoinCondition joinCondition)
  {
	  this.joinCondition = joinCondition;
  }
  
  /**
   *  Select table1 column name.
   */
  public void selectTable1Column(String name, String alias) {
  	table1Columns.put(name, alias);
  }
  
  /**
   * Select table2 column name.
   */
  public void selectTable2Column(String name, String alias) {
  	table2Columns.put(name, alias);
  }
  
	/**
	 * Join row from table1 and table2.
	 */
	protected void joinRows(Map<String, Object> row1, Map<String, Object> row2)
  {
		// joined row 
		HashMap<String, ArrayList<Object>> join = new HashMap<String, ArrayList<Object>>();
		
		// select columns from row1   
		if (row1 == null) {
			for (Map.Entry<String, String> entry : table1Columns.entrySet()) {
				String key = entry.getValue();
				if (key == null) key = entry.getKey();
				join.put(key, null);
			}
		} else {
			if (table1Columns.size() == 0) {
				for (Map.Entry<String, Object> entry : row1.entrySet()) {
					ArrayList<Object> list = new ArrayList<Object>();
					list.add(entry.getValue());
					join.put(entry.getKey(), list);
				}
			} else {
				for (Map.Entry<String, String> entry : table1Columns.entrySet()) {
					String key = entry.getKey();
					ArrayList<Object> list = new ArrayList<Object>();
					list.add(row1.get(key));
					if (entry.getValue() != null) join.put(entry.getValue(), list);
					else join.put(key, list);
				}
			}
		}
		
		// check if row2 is null
		if (row2 == null) {
			for (Map.Entry<String, String> entry : table2Columns.entrySet()) {
				String key = entry.getValue();
				if (key == null) key = entry.getKey();
				join.put(key, null);
			}
			outport.emit(join);
			return;
		}
		
		// select rows from table2  
		if (table2Columns.size() == 0) {
			for (Map.Entry<String, Object> entry : row2.entrySet()) {
				ArrayList<Object> list;
				if (join.containsKey(entry.getKey())) {
					list = join.remove(entry.getKey());
				} else {
				  list = new ArrayList<Object>();
				}
				list.add(entry.getValue());
				join.put(entry.getKey(), list);
			}
		} else {
			for (Map.Entry<String, String> entry : table2Columns.entrySet()) {
				String key = entry.getValue();
				if (key == null) key = entry.getKey();
				ArrayList<Object> list;
				if (join.containsKey(key)) {
					list = join.remove(key);
				} else {
				  list = new ArrayList<Object>();
				}
				list.add(row2.get(entry.getKey()));
				join.put(key, list);
			}
		}
		
		// emit row  
		outport.emit(join);
  }
}
