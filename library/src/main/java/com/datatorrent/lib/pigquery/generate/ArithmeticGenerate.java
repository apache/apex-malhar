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

import javax.validation.constraints.NotNull;


/**
 * Implements Pig arithmetic operator generate semantic.   <br>
 * <p>
 * Valid opName : '+', '-', '*', '/', '%'. <br>
 * see {@link com.datatorrent.lib.pigquery.generate.BinaryGenerate}.
 * 
 * @displayName: Arithmetic Generate
 * @category: pigquery.generate
 * @tag: arithmetic operation, map, string, addition, subtraction, multiplication, division, percentage 
 * @since 0.3.4
 */
public class ArithmeticGenerate  extends BinaryGenerate
{

  /**
   * @param leftField  left argument field name from pig RDD.
   * @param rightField right argument field name from pig RDD.
   * @param opName  Arithmetic operator name, must be "+", "-", "*", "/", "%".
   */
  public ArithmeticGenerate(@NotNull String leftField, @NotNull String rightField, String aliasName,@NotNull String opName)
  {
    super(leftField, rightField, aliasName, opName);
    assert(opName.equals("+") || opName.equals("-") || opName.equals("*") || opName.equals("/") || opName.equals("%"));
  }

  /**
   * @see com.datatorrent.lib.pigquery.generate.Generate#evaluate(java.util.Map, java.util.Map)
   */
  @Override
  public void evaluate(Map<String, Object> tuple, Map<String, Object> collect)
  {
    if (!tuple.containsKey(leftField) || !tuple.containsKey(rightField)) return;
    double leftVal = ((Number)tuple.get(leftField)).doubleValue();
    double rightVal = ((Number)tuple.get(rightField)).doubleValue();
    double result = 0.0;
    if (opName.equals("+")) result = leftVal + rightVal;
    if (opName.equals("-")) result = leftVal - rightVal;
    if (opName.equals("*")) result = leftVal * rightVal;
    if (opName.equals("/")) result = leftVal / rightVal;
    if (opName.equals("%")) result = leftVal % rightVal;
    collect.put(getOutputAliasName(), result);
  }
}
