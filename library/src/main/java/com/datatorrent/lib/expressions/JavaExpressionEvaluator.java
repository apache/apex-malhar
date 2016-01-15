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

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This class is used for evaluating expressions which takes multiple parameter object and the result is returned for that expression.
 *
 * The variable will be resolved to its accessible variable or getter method in order. After this the variable can be used as if its a Java variable.
 *
 * JavaExpressionEvaluator also allows you to set extra imports that needs to be added over default is java.lang.*
 *
 * JavaExpressionEvaluator needs to be configured with following configurations as minimal configuration:
 * 1. Mapping of input object place holders to it corresponding types.
 * This can be done with setInputObjectPlaceholders method.
 * 2. Return type of of expression evaluation.
 * 3. Expression to be evaluated. This is a standard java expression except for referencing the variable inside object JEL syntax needs to be used i.e. ${objectPlaceHolder.varName}
 *
 * Expression can be defined as per JSL 7 syntax:
 * http://docs.oracle.com/javase/specs/jls/se7/html/jls-14.html
 */
public class JavaExpressionEvaluator implements ExpressionEvaluator
{
  public static String EXECUTE_INPUT_PARAM_NAME = "paramObject";

  private final Set<String> imports = new HashSet<>();

  private ExpressionParser expressionParser;

  /**
   * Constructor for JavaExpressionEvaluator.
   * This constructor will add some default java imports for expressions to be compiled.
   */
  public JavaExpressionEvaluator()
  {
    addDefaultImports();
  }

  /**
   * {@inheritDoc}
   */
  @Override public <O> ExpressionEvaluator.Expression<O> createExecutableFunction(String expression, Class<?> returnType)
  {
    return createExecutableCode(expression, returnType, true);
  }


  /**
   * Expression should be a single line expression as per JLS 7 as given in:
   * <a>https://docs.oracle.com/javase/specs/jls/se7/html/jls-15.html</a>
   *
   * {@inheritDoc}
   */
  @Override public <O> ExpressionEvaluator.Expression<O> createExecutableExpression(String expression, Class<?> returnType)
  {
    return createExecutableCode(expression, returnType, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override public void setExpressionParser(ExpressionParser parser)
  {
    this.expressionParser = parser;
  }

  /**
   * This is a private which will be called by public method createExecutableExpression & createExecutableFunction.
   * This method is the main method which converts the given expression into java expression format and compiles it
   * using Janino.
   *
   * The method works in following steps:
   * <ol>
   *   <li>Resolve object/variable placeholder to respective types and replace them in expression</li>
   *   <li>Generate final code to go into execute method of Expression class. Here the final code is generated based
   *       on whether the isCompleteMethod is true OR false. If its true, the code is added as it is
   *       If false, it'll added along with return statement.</li>
   *   <li>Compile the expression using Janino library</li>
   *   <li>Return an object of type Expression which contains compiled expression.</li>
   * </ol>
   *
   * @param expression              String expression that needs to be compiled
   * @param returnType              Return type of the expression.
   * @param isCompleteMethod Whether the expression contains return statement OR not.
   * @param <O>                     Parameterized return class type of expression after evaluation.
   * @return                        Object of type Expression interface having compiled and executable expression.
   */
  @SuppressWarnings({"unchecked"})
  private <O> ExpressionEvaluator.Expression<O> createExecutableCode(String expression, Class<?> returnType,
      boolean isCompleteMethod)
  {
    String code = expressionParser.convertToCompilableExpression(expression, returnType, isCompleteMethod);

    try {
      IScriptEvaluator se = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
      se.setDefaultImports(this.imports.toArray(new String[imports.size()]));
      return (ExpressionEvaluator.Expression)se.createFastEvaluator(code, ExpressionEvaluator.Expression.class, new String[]{EXECUTE_INPUT_PARAM_NAME});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This private method add default imports for compiling the expression.
   * The import that are added here can be directly used inside Expression.
   */
  private void addDefaultImports()
  {
    imports.add("java.util.*");
    imports.add("java.math.*");
    imports.add("java.text.*");

    // Pre-existing
    imports.add("static java.lang.Math.*");
    imports.add("static org.apache.commons.lang3.StringUtils.*");
    imports.add("static org.apache.commons.lang3.StringEscapeUtils.*");
    imports.add("static org.apache.commons.lang3.time.DurationFormatUtils.*");
    imports.add("static org.apache.commons.lang3.time.DateFormatUtils.*");
    imports.add("static org.apache.commons.lang3.time.DateUtils.*");

    // Custom ones
    imports.add("static com.datatorrent.lib.expressions.ConversionUtils.*");
    imports.add("static com.datatorrent.lib.expressions.StringUtils.*");
    imports.add("static com.datatorrent.lib.expressions.DateTimeUtils.*");
  }

  /**
   * This method adds any given package to the default list of imports.
   * The import that are added here can be directly used inside Expression.
   * @param importPackage
   */
  public void addImports(String importPackage)
  {
    imports.add(importPackage);
  }

  /**
   * This method overrides the list of imports currently set for expression evaluation.
   * Except java.lang will remain there for technical reason.
   * After call to this method, following list of imports will be added to the file:
   * <ul>
   *   <li>java.lang</li>
   *   <li>One present in importPackages</li>
   * </ul>
   *
   * @param importPackages List of import packages that needs to be set for expression evaluation.
   */
  public void overrideImports(String[] importPackages)
  {
    imports.clear();
    imports.addAll(Arrays.asList(importPackages));
  }

  /**
   * This method can add a public static custom function to be made available while expression evaluation.
   * The parameter passed to the function should be qualified i.e. it should be prepended with classname as well.
   * For eg.
   * <p>
   *   Lets say there is a class which has public static function:
   *   package com.package.name.something;
   *
   *   public class Example
   *   {
   *     public static compute(<params>)
   *     {
   *       <operations>
   *     }
   *   }
   *
   *   Calling registerCustomMethod("com.package.name.something.Example.compute") will add compute method to be directly
   *   available in Expression Language.
   * </p>
   * @param qualifiedFunc
   */
  public void registerCustomMethod(String qualifiedFunc)
  {
    addImports("static " + qualifiedFunc);
  }
}
