/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.pigquery.generate;

import java.util.Map;


/**
 * <p>An implementation of Unary Generate that generates a name and provides implementation of evaluate method.</p>
 *
 * @displayName: Generate Name
 * @category: pigquery.generate
 * @tag: map, string, unary
 * @since 0.3.4
 */
public class GenerateName extends UnaryGenerate
{

  /**
   * @param fieldName field name for value to output.
   * @param aliasName Alias name for output value. 
   */
  public GenerateName(String fieldName, String aliasName)
  {
    super(fieldName, aliasName);
  }

  /**
   * @see com.datatorrent.lib.pigquery.generate.Generate#evaluate(java.util.Map, java.util.Map)
   */
  @Override
  public void evaluate(Map<String, Object> tuple, Map<String, Object> collect)
  {
    collect.put(getOutputAliasName(), tuple.get(fieldName));
  }
}
