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
 *  This operator provides sql group by query semantic on live data stream. <br>
 *  Stream rows satisfying given select condition are processed by group by column names
 *  and aggregate  column function. <br>
 *  HashMap of column name(s) and aggregate alias is emitted on output port. <br>
 *  <br>
 *  <b>StateFull : NO,</b> all row data is processed in current time window. <br>
 *  <b>Partitions : No, </b> will yield wrong result(s). <br>
 *  <br>
 * <b>Ports</b>:<br>
 * <b> inport : </b> Input hash map(row) port, expects HashMap&lt;String,Object&gt;<<br>
 * <b> outport : </b> Output hash map(row) port, emits  HashMap&lt;String,Object&gt;<br>
 * <br>
 * <b> Properties : <b> <br>
 * <b> condition : </b> Select condition for deleting rows. <br>
 * <b> groupNames : </b> Group by names list. <br>
 * <b> indexes : </b> Select column indexes. <br>
 * <br>
 *
 * @author Amol Kekre <amol@datatorrent.com>
 * @since 0.3.3
 */
public class GroupByOperator extends SqlOperator
{
  /**
   * select indexes.
   */
	private ArrayList<SelectAggregateIndex> indexes = new  ArrayList<SelectAggregateIndex> ();

	/**
	 *  Group by names
	 */
	private HashMap<String, String> groupNames = new HashMap<String, String>();

	/**
	 * where condition.
	 */
	private SelectCondition condition;

	@Override
  public ArrayList<HashMap<String, Object>> processRows(
      ArrayList<HashMap<String, Object>> rows)
  {
		// filter rows by select condition
		if (condition != null) {
			rows = condition.filetrValidRows(rows);
		}

		// aggregate rows
		rows = aggregateRows(rows);
	  return rows;
  }

	/**
	 * add select index
	 * @throws Exception
	 */
	public void addSelectIndex(SelectAggregateIndex index) {
		if (index == null) return;
		indexes.add(index);
	}

	/**
   * @param set condition
   */
  public void setCondition(SelectCondition condition)
  {
	  this.condition = condition;
  }

  /**
   * Add group name.
   */
  public void addGroupByName(String name, String alias) {
  	groupNames.put(name, alias);
  }

  /**
   * multi key compare class.
   */
  @SuppressWarnings("rawtypes")
  private class MultiKeyCompare implements Comparable
  {
    /**
     * compare keys.
     */
  	ArrayList<Object> compareKeys = new ArrayList<Object>();

  	@Override
  	public boolean equals(Object other) {
  		if (other instanceof MultiKeyCompare)
  		if (compareKeys.size() != ((MultiKeyCompare)other).compareKeys.size()) {
  			return false;
  		}
  		for (int i=0; i < compareKeys.size(); i++) {
  			if (!(compareKeys.get(i).equals(((MultiKeyCompare)other).compareKeys.get(i)))) {
  				return false;
  			}
  		}
  		return true;
  	}

  	@Override
  	public int hashCode() {
  	   int hashCode = 0;
  	   for (int i=0; i < compareKeys.size(); i++) {
  	  	 hashCode += compareKeys.get(i).hashCode();
  	   }
  	   return hashCode;
  	}

		@Override
    public int compareTo(Object other)
    {
      if (this.equals(other)) return 0;
      return -1;
    }

		/**
		 * Add compare key.
		 */
		public void addCompareKey(Object value){
			compareKeys.add(value);
		}

		/**
		 * Get ith key value
		 */
		public Object getValue(int i) {
			return compareKeys.get(i);
		}
  }

  /**
   * Aggregate function.
   */
	private ArrayList<HashMap<String, Object>> aggregateRows(
      ArrayList<HashMap<String, Object>> rows)
  {
		// no group by names
		if (groupNames.size() == 0) return new ArrayList<HashMap<String, Object>>();

		// aggregate rows by group
		HashMap<MultiKeyCompare, ArrayList<HashMap<String, Object>>> aggregate =
				new HashMap<MultiKeyCompare, ArrayList<HashMap<String, Object>>>();
		for (int i=0; i < rows.size(); i++) {
			HashMap<String, Object> row = rows.get(i);
			MultiKeyCompare key = new MultiKeyCompare();
			for (Map.Entry<String, String> entry : groupNames.entrySet()) {
				key.addCompareKey(row.get(entry.getKey()));
			}
			ArrayList<HashMap<String, Object>> list;
			if (aggregate.containsKey(key)) {
				list = aggregate.get(key);
			} else {
				list = new ArrayList<HashMap<String, Object>>();
				aggregate.put(key,  list);
			}
			list.add(row);
		}

		// create result groups
		ArrayList<HashMap<String, Object>> result = new ArrayList<HashMap<String, Object>>();
		for (Map.Entry<MultiKeyCompare, ArrayList<HashMap<String, Object>>> entry : aggregate.entrySet()) {

			// put group names first
			MultiKeyCompare key = entry.getKey();
			HashMap<String, Object> outrow = new HashMap<String, Object>();
			int index = 0;
			for (Map.Entry<String, String> entry1 : groupNames.entrySet()) {
				String name = entry1.getValue();
				if (name == null) name = entry1.getKey();
				outrow.put(name, key.getValue(index++));
			}

			// put aggregate functions now
			ArrayList<HashMap<String, Object>> group = entry.getValue();
			for (int i=0; i < indexes.size(); i++) {
				ArrayList<HashMap<String, Object>> out = indexes.get(i).process(group);
				for (Map.Entry<String, Object> entry1 : out.get(0).entrySet()) {
					outrow.put(entry1.getKey(), entry1.getValue());
				}
			}
			result.add(outrow);
 		}
		return result;
  }
}
