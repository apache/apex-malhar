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
 * Splits the String tuples into tokens and emits filtered keys as a HashMap.
 * <p>
 * A HashMap of all filtered tokens are emitted on output port "tokens". <br>
 * </p>
 * <p>
 * First token in line is treated as key and rest are put into values array list. <br>
 * HashMap of token and array values are emitted on output port.
 * This module is a pass through<br>
 * <br>
 * <b>StateFull : No, </b> tokens are processed in current window. <br>
 * <b>Partitions : Yes, </b> No state dependency in output tokens. <br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects String<br>
 * <b>tokens</b>: Output port, emits HashMap<String, Object><br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val pair.
 * Default is "", i.e. tokens are not split, and key is set to token, and val is
 * null<br>
 * <b>filterby</b>: Only emit the keys (comma separated) that are in filterby<br>
 * <br>
 * </p>
 * @displayName Filtered Line To Token HashMap
 * @category Tuple Converters
 * @tags filter, hashmap, string
 *
 * @since 0.3.3
 */
@Stateless
@OperatorAnnotation(partitionable = true)
public class FilteredLineToTokenHashMap extends LineToTokenHashMap
{
  HashMap<String, Object> filterBy = new HashMap<String, Object>(4);

  /**
   * setter function for filterBy
   *
   * @param list
   *          list of keys for subtoken filters
   */
  public void setFilterBy(String[] list)
  {
    if (list != null) {
      for (String s : list) {
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
