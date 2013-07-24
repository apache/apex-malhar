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
 *  This operator provides sql delete query semantic on live data stream. <br>
 *  Stream rows passing condition are removed from stream. <br>
 *  <br>
 *  <b>StateFull : NO,</b> all row data is processed in current time window. <br>
 *  <b>Partitions : Yes, </b> No Input dependency among input rows. <br>
 *  <br>
 * <b>Ports</b>:<br>
 * <b> inport : </b> Input hash map(row) port, expects HashMap&lt;String,Object&gt;<<br>
 * <b> outport : </b> Output hash map(row) port, emits  HashMap&lt;String,Object&gt;<br>
 * <br>
 * <b> Properties : <b> <br>
 * <b> condition : </b> Select condition for deleting rows. <br>
 * <br>
 */
public class DeleteOperator extends SqlOperator
{
	/**
	 * Where condition for deleting row.
	 */
	private SelectCondition condition;
	
	/**
	 * Process rows function.
	 */
	@Override
  public ArrayList<HashMap<String, Object>> processRows(
      ArrayList<HashMap<String, Object>> rows)
  {
		for (int i=0; i < rows.size(); i++) {
			HashMap<String, Object> row = rows.get(i);
			boolean isValid = (condition == null) ? true : condition.isValidRow(row);
			if (!isValid) outport.emit(row);
		}
	  return null;
  }

	/**
   * @return the condition
   */
  public SelectCondition getCondition()
  {
	  return condition;
  }

	/**
   * @param set condition
   */
  public void setCondition(SelectCondition condition)
  {
	  this.condition = condition;
  }
}
