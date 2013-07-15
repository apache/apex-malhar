package com.datatorrent.lib.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

public class InnerJoin  implements Operator
{
	/**
	 * Join Condition; 
	 */
	private JoinCondition joinCondition;
	
	/**
	 * Table1 select columns.
	 */
	private HashSet<String> table1Columns = new HashSet<String>();
	
	/**
	 * Table2 select columns.
	 */
	private HashSet<String> table2Columns = new HashSet<String>();
	
  /**
   * Collect data rows from input port 1.
   */
	private ArrayList<Map<String, Object>> table1 = new ArrayList<Map<String, Object>>();
	
	
	/**
	 * Collect data from input port 2. 
	 */
	private ArrayList<Map<String, Object>> table2 = new ArrayList<Map<String, Object>>();
	
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
  public void selectTable1Column(String name) {
  	table1Columns.add(name);
  }
  
  /**
   * Select table2 column name.
   */
  public void selectTable2Column(String name) {
  	table2Columns.add(name);
  }
  
	/**
	 * Join row from table1 and table2.
	 */
	private void joinRows(Map<String, Object> row1, Map<String, Object> row2)
  {
		// joined row 
		HashMap<String, ArrayList<Object>> join = new HashMap<String, ArrayList<Object>>();
		
		// select columns from row1   
		if (table1Columns.size() == 0) {
			for (Map.Entry<String, Object> entry : row1.entrySet()) {
				ArrayList<Object> list = new ArrayList<Object>();
				list.add(entry.getValue());
				join.put(entry.getKey(), list);
			}
		} else {
			Iterator<String> iter = table1Columns.iterator();
			while (iter.hasNext()) {
				String key = iter.next();
				ArrayList<Object> list = new ArrayList<Object>();
				list.add(row1.get(key));
				join.put(key, list);
			}
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
			Iterator<String> iter = table2Columns.iterator();
			while (iter.hasNext()) {
				String key = iter.next();
				ArrayList<Object> list;
				if (join.containsKey(key)) {
					list = join.remove(key);
				} else {
				  list = new ArrayList<Object>();
				}
				list.add(row2.get(key));
				join.put(key, list);
			}
		}
		
		// emit row  
		outport.emit(join);
  }
}
