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

import com.datatorrent.api.Operator.Unifier;

/**
 * <p>
 * This operator provides sql oredr by operator semantic over luve stream data. <br>
 * Input data rows are ordered by order rules, ordered result is emitted on output port. <br>
 * <br>
 *  *  <br>
 *  <b>StateFull : NO,</b> all row data is processed in current time window. <br>
 *  <b>Partitions : Yes, </b> This operator is also unifier on output port. <br>
 *  <br>
 * <b>Ports</b>:<br>
 * <b> inport : </b> Input hash map(row) port, expects HashMap&lt;String,Object&gt;<<br>
 * <b> outport : </b> Output hash map(row) port, emits  HashMap&lt;String,Object&gt;<br>
 * <br>
 * <b> Properties : <b> <br>
 */
public class OrderByOperator extends SqlOperator implements Unifier<HashMap<String, Object>>
{
	/**
	 * Order by rules.
	 */
	ArrayList<OrderByRule<?>>	oredrByRules	= new ArrayList<OrderByRule<?>>();

	/**
	 * Descending flag.
	 */
	private boolean isDescending;
	
	
	/**
	 * Process rows operator.
	 */
	@Override
	public ArrayList<HashMap<String, Object>> processRows(
	    ArrayList<HashMap<String, Object>> rows)
	{
		for (int i=(oredrByRules.size()-1); i >= 0; i--) {
			rows = oredrByRules.get(i).sort(rows);
		}
		if (isDescending) {
			ArrayList<HashMap<String, Object>> tmp = new ArrayList<HashMap<String, Object>>();
			for (int i=rows.size()-1; i >= 0; i--) {
				tmp.add(rows.get(i));
			}
			rows = tmp;
		}
		return rows;
	}

	/**
	 * Add order by rule.
	 */
	public void addOrderByRule(OrderByRule<?> rule)
	{
		oredrByRules.add(rule);
	}

	/**
   * @return isDescending
   */
  public boolean isDescending()
  {
	  return isDescending;
  }

	/**
   * @param set isDescending
   */
  public void setDescending(boolean isDescending)
  {
	  this.isDescending = isDescending;
  }

	@Override
  public void process(HashMap<String, Object> tuple)
  {
	  rows.add(tuple);
  }
	
	/**
	 *  Get unifier for output port.
	 */
	@Override
	public Unifier<HashMap<String, Object>> getUnifier() {
		OrderByOperator unifier = new OrderByOperator();
		for (int i=0; i < oredrByRules.size(); i++) {
			unifier.addOrderByRule(oredrByRules.get(i));
		}
		unifier.setDescending(isDescending);
		return unifier;
	}
}
