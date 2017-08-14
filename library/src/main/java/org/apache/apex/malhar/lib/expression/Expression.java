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
package org.apache.apex.malhar.lib.expression;

/**
 * This is the Expression interface, object of which will be returned after evaluating expression.
 * The interface can be used to execute given expression.
 *
 * @param <O> This is generic class return type of execute method of expression.
 *
 * @since 3.4.0
 */
public interface Expression<O>
{
  /**
   * Method which contains the compiled and executable code corresponding to provided expression.
   *
   * @param obj Object on which expression will be executed.
   * @return Returns result of expression of type O.
   */
  O execute(Object obj);

  /**
   * Defines an interface for how an expression should be parsed.
   */
  interface ExpressionParser
  {
    /**
     * The method should convert given expression to a code which can be compiled for execution.
     *
     * @param expression Original expression which needs to be converted to compilable code.
     * @param objectType Type of the object which will be used in expression.
     * @param returnType Return type of given expression.
     * @return Returns a java compilable code of given expression.
     */
    String convertToCompilableExpression(String expression, Class<?> objectType, Class<?> returnType);

    /**
     * This method provides way in which the object in action is placed in expression.
     * The method also defined how should the object placeholder get converted in compilable code.
     *
     * @param exprObjPlaceholder String placeholder that can be used in expression to reference the object.
     * @param codeObjPlaceholder String placeholder that will be used in code to replace exprObjectPlaceholder.
     */
    void setInputObjectPlaceholder(String exprObjPlaceholder, String codeObjPlaceholder);
  }
}
