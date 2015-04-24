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
 * An implementation of Binary Generate that implements sum generation index expression.
 * <p>
 * @displayName Generate Sum
 * @category Pig Query
 * @tags map, string, sum
 * @since 0.3.4
 */
@Deprecated
public class SumGenerate  extends BinaryGenerate
{
   
  /**
   * @param leftField left field name.
   * @param rightField right field name.
   * @param aliasName output alias name.
   */
  public SumGenerate(String leftField, String rightField, String aliasName)
  {
    super(leftField, rightField, aliasName, "SUM");
  }

  /**
   * @see com.datatorrent.lib.pigquery.generate.Generate#evaluate(java.util.Map, java.util.Map)
   */
  @Override
  public void evaluate(Map<String, Object> tuple, Map<String, Object> collect)
  {
    collect.put(getOutputAliasName(), ((Number)tuple.get(leftField)).doubleValue() + ((Number)tuple.get(rightField)).doubleValue());
  }
}
