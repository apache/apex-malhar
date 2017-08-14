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
package org.apache.apex.malhar.sql.operators;

import org.apache.apex.malhar.lib.expression.Expression;
import org.apache.apex.malhar.lib.transform.TransformOperator;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Context;

/**
 * This is an extension of {@link TransformOperator} which also takes care of filtering tuples.
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public class FilterTransformOperator extends TransformOperator
{
  private String condition;
  private transient Expression conditionExpression;

  @Override
  public void activate(Context context)
  {
    super.activate(context);
    if (condition != null) {
      conditionExpression = PojoUtils.createExpression(inputClass, condition, Boolean.class,
        expressionFunctions.toArray(new String[expressionFunctions.size()]));
    }
  }

  @Override
  protected void processTuple(Object in)
  {
    if ((conditionExpression != null) && (conditionExpression.execute(in) == Boolean.FALSE)) {
      return;
    }

    super.processTuple(in);
  }

  public String getCondition()
  {
    return condition;
  }

  /**
   * Set quasi-Java expression which acts as filtering logic for given tuple
   *
   * @param condition Expression which can be evaluated for filtering
   */
  public void setCondition(String condition)
  {
    this.condition = condition;
  }
}
