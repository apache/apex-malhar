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

package com.datatorrent.lib.filter;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.expression.Expression;
import com.datatorrent.lib.util.PojoUtils;

/**
 * <b>FilterOperator</b>
 * Filter Operator filter out tuples based on defined condition
 *
 * <b>Parameters</b>
 * - condition: condition based on expression language
 *
 * <b>Input Port</b> takes POJOs as an input
 *
 * <b>Output Ports</b>
 * - truePort emits POJOs meeting the given condition
 * - falsePort emits POJOs not meeting the given condition
 * - error port emits any error situation while evaluating expression
 * 
 */
@InterfaceStability.Evolving
public class FilterOperator extends BaseOperator implements Operator.ActivationListener
{
  private String condition;
  private List<String> expressionFunctions = new LinkedList<>();

  private transient Class<?> inClazz = null;
  private transient Expression<Boolean> expr = null;

  @AutoMetric
  private long trueTuples;

  @AutoMetric
  private long falseTuples;

  @AutoMetric
  private long errorTuples;

  public final transient DefaultOutputPort<Object> truePort = new DefaultOutputPort<Object>();

  public final transient DefaultOutputPort<Object> falsePort = new DefaultOutputPort<Object>();

  public final transient DefaultOutputPort<Object> error = new DefaultOutputPort<Object>();

  public FilterOperator()
  {
    expressionFunctions.add("java.lang.Math.*");
    expressionFunctions.add("org.apache.commons.lang3.StringUtils.*");
    expressionFunctions.add("org.apache.commons.lang3.StringEscapeUtils.*");
    expressionFunctions.add("org.apache.commons.lang3.time.DurationFormatUtils.*");
    expressionFunctions.add("org.apache.commons.lang3.time.DateFormatUtils.*");
  }

  @InputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    public void setup(PortContext context)
    {
      inClazz = context.getValue(Context.PortContext.TUPLE_CLASS);
    }

    @Override
    public void process(Object t)
    {
      processTuple(t);
    }
  };

  @Override
  public void activate(Context context)
  {
    createExpression();
  }

  @Override
  public void deactivate()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    errorTuples = trueTuples = falseTuples = 0;
  }

  /**
   * createExpression: create an expression from condition of POJO fields
   * Override this function for custom field expressions
   */
  protected void createExpression()
  {
    logger.info("Creating an expression for condition {}", condition);
    expr = PojoUtils.createExpression(inClazz, condition, Boolean.class,
        expressionFunctions.toArray(new String[expressionFunctions.size()]));
  }

  /**
   * evalExpression: Evaluate condition/expression
   * Override this function for custom condition evaluation
   */
  protected Boolean evalExpression(Object t)
  {
    return expr.execute(t);
  }

  /**
   * handleFilter: emit POJO meeting condition on truePort
   * and if it did not meet condition then on falsePort
   */
  private void processTuple(Object t)
  {
    try {
      if (evalExpression(t)) {
        truePort.emit(t);
        trueTuples++;
      } else {
        falsePort.emit(t);
        falseTuples++;
      }
    } catch (Exception ex) {
      logger.error("Error in expression eval: {}", ex.getMessage());
      logger.debug("Exception: ", ex);
      error.emit(t);
      errorTuples++;
    }
  }

  /**
   * Returns condition/expression with which Filtering is done
   *
   * @return condition parameter of Filter Operator
   */
  public String getCondition()
  {
    return condition;
  }

  /**
   * Set condition/expression with which Filtering operation would be applied
   *
   * @param condition parameter of Filter Operator
   */
  public void setCondition(String condition)
  {
    logger.info("Changing condition from {} to {}", this.condition, condition);
    this.condition = condition;
  }

  /**
   * Returns the list of expression function which would be made available to
   * expression to use.
   *
   * @return List of functions available in expression.
   */
  public List<String> getExpressionFunctions()
  {
    return expressionFunctions;
  }

  /**
   * Set list of import classes/method should should be made statically available
   * to expression to use.
   * For ex. org.apache.apex.test1.Test would mean that "Test" method will be
   * available in the expression to be used directly.
   * This is an optional property. See constructor to see defaults that are included.
   *
   * @param expressionFunctions List of qualified class/method that needs to be
   *                            imported to expression.
   */
  public void setExpressionFunctions(List<String> expressionFunctions)
  {
    this.expressionFunctions = expressionFunctions;
  }

  private static final Logger logger = LoggerFactory.getLogger(FilterOperator.class);
}
