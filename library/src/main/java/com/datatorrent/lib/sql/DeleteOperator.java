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
 *  This operator provides sql delete query semantic on live data stream. <br>
 *  Stream rows passing condition are removed from stream. <br>
 *  
 *  This operators read data rows from input port. <br>
 *  If row meets given select condition, it is not emitted on outport, <br>
 *  else it is emitted on output.  <br>
 *  
 * <b>Ports</b>:<br>
 * <b> inport : </b> Input hash map(row) port. <br>
 * <b> outport : </b> Output hash map(row) port. <br>
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
		ArrayList<HashMap<String, Object>> result = new ArrayList<HashMap<String, Object>>();
		for (int i=0; i < rows.size(); i++) {
			HashMap<String, Object> row = rows.get(i);
			boolean isValid = (condition == null) ? true : condition.isValidRow(row);
			if (!isValid) result.add(row);
		}
	  return result;
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
