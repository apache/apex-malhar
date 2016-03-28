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
package com.datatorrent.lib.expressions;

/**
 * This interface defines:
 * <ul>
 *   <li>How Expression Evaluator should be defined.</li>
 *   <li>What is the output interface after evaluating the expression i.e. Expression interface.</li>
 *   <li>How to parse Expression i.e ExpressionParser interface</li>
 * </ul>
 */
public interface ExpressionEvaluator
{
  /**
   * Method will create create a compiled & executable expression which is go as-is as method's content.
   * <b>
   *   NOTE: The expression provided here is expected to contain a return statement.
   *         If expression does not contain return statement, the method will throw RuntimeException.
   * </b>
   *
   * <p>
   *  Example is as follows:
   *  Let the expression be <func_expression>
   *  After compiling and resolving the placeholder, it become <compiled_func_expression>
   *
   *  The execute method of Expression interface should look like this:
   *  <return_type> execute(Object... obj)
   *  {
   *    <compiled_func_expression>
   *  }
   * </p>
   *
   * @param expression  String expression which has return statement in it.
   * @param returnType  Type of class which will be returned by execute method of Expression interface.
   * @param <O>         Parameterized return class type of expression after evaluation.
   * @return            Object of type Expression interface having compiled and executable expression.
   */
  <O> ExpressionEvaluator.Expression<O> createExecutableFunction(String expression, Class<?> returnType);

  /**
   * Method will create a compiled & executable expression to which return statement is added.
   * <b>NOTE: The expression provided here is not expected to contain a return statement. </b>
   *
   * <p>
   *  Example is as follows:
   *  Let the expression be <expression>
   *  After compiling and resolving the placeholder, it becomes <compiled_expression>
   *
   *  The execute method of Expression interface would look like this:
   *  <return_type> execute(Object... obj)
   *  {
   *    return ((<return_type>)(<compiled_expression>))
   *  }
   * </p>
   *
   * @param expression  String expression which has return statement in it.
   * @param returnType  Type of class which will be returned by execute method of Expression interface.
   * @param <O>         Parameterized return class type of expression after evaluation.
   * @return            Object of type Expression interface having compiled and executable expression.
   */
  <O> ExpressionEvaluator.Expression<O> createExecutableExpression(String expression, Class<?> returnType);

  /**
   * Sets parser that should be used to convert the expression to compilable code.
   *
   * @param parser Object of {@link ExpressionParser}
   */
  void setExpressionParser(ExpressionParser parser);

  /**
   * This is the Expression interface, object of which will be returned after evaluating expression.
   * The object parameters sent to this method should be in the same order as the one configured in
   * ExpressionParser.setInputObjectPlaceholders.
   *
   * @param <O> This is generic class return type of execute method of expression.
   */
  interface Expression<O>
  {
    /**
     * Method which contains the compiled and executable code corresponding to provided expression.
     *
     * @param obj Variable parameter list of object that are operands for expression.
     * @return Returns result of expression of type O.
     */
    O execute(Object... obj);
  }

  /**
   * Defined an interface for how an expression should be parsed.
   * ExpressionParser is expected to be plugged into {@link ExpressionEvaluator} via ExpressionEvaluator.setExpressionParser
   * method.
   */
  interface ExpressionParser
  {
    /**
     * The method should convert given expression to a code which can be compiled for execution.
     *
     * @param expression Original expression which needs to be converted to compilable code.
     * @param returnType Return type of given expression.
     * @param isCompleteMethod Whether expression contains a "return" statement.
     * @return
     */
    String convertToCompilableExpression(String expression, Class<?> returnType, boolean isCompleteMethod);

    /**
     * This method provides order and mapping is object placeholder vs their class types.
     * The order in which the inputObjectPlaceholders are added, the same order needs to be followed while passing
     * parameters to execute method as well.
     *
     * inputObjectPlaceholder are the string placeholder which can be used in expression to reference the object
     * that is passed to execute method of {@link Expression} class.
     *
     * <b>NOTE: This registers the order in which parameters are passed to execute method. </b>
     *
     * @param inputObjectPlaceholders Array of string placeholder that can be used in expression to reference the object.
     * @param classTypes              Array of Class type of respective string placeholders in expression.
     */
    void setInputObjectPlaceholders(String[] inputObjectPlaceholders, Class[] classTypes);

    /**
     * This method allows one to set the placeholder expected in String expression.
     *
     * @param variablePlaceholderPattern Variable placeholder pattern
     */
    void setVariableRegexPattern(String variablePlaceholderPattern);
  }
}
