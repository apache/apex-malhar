/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.pigquery.generate;

import java.util.Map;


/**
 * Interface to declare Foreach Generate indexes.
 * <p>
 * @displayName Generate
 * @category Pig Query
 * @tags map, string
 * @since 0.3.4
 */
@Deprecated
public interface Generate
{
  /**
   * This function is called on foreach input tuple, output filed is stored in collect map.
   * @param  tuple Current tuple in foreach loop. 
   * @param  collect  Map to collect output filed/value.
   */
  public void evaluate(Map<String, Object> tuple, Map<String, Object> collect);
  
  /**
   * Get output alias name.
   * @return output alias name.
   */
  public String getOutputAliasName();
}
