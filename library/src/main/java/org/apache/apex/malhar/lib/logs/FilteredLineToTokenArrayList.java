/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.logs;

import java.util.HashMap;

import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;

/**
 * <p>
 * Splits String objects into tokens, and emits filtered keys as and ArrayList.
 * <p>
 * An ArrayList of all tokens that pass the filter are emitted.
 * </p>
 * <p>
 * This module is a pass through<br>
 * <br>
 * <b>StateFull : No, </b> tokens are processed in current window. <br>
 * <b>Partitions : Yes, </b> No state dependency in output tokens. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Input port, expects String<br>
 * <b>tokens</b>: Output port, emits ArrayList<Object><br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val pair. If not specified the value is set to null. Default is ",", i.e. tokens are split<br>
 * <b>filterby</b>: Only emit the keys (comma separated_that are in filterby<br>
 * <br>
 * <br>
 * <br>
 * </p>
 * @displayName Filtered Line To Token ArrayList
 * @category Tuple Converters
 * @tags filter, arraylist, string
 *
 * @since 0.3.2
 */
@Stateless
@OperatorAnnotation(partitionable = true)
public class FilteredLineToTokenArrayList extends LineToTokenArrayList
{
  HashMap<String, Object> filterBy = new HashMap<String, Object>();

  /**
   * setter function for filter
   *
   * @param list list of keys for subtoken filters
   */
  public void setFilterBy(String[] list)
  {
    if (list != null) {
      for (String s: list) {
        filterBy.put(s, null);
      }
    }
  }

  /**
   * If the key is in the filter, returns true
   *
   * @param subtok
   * @return true if super.validToken (!isEmpty()) and filter has they token
   */
  @Override
  public boolean validSubTokenKey(String subtok)
  {
    return super.validToken(subtok) && filterBy.containsKey(subtok);
  }
}
