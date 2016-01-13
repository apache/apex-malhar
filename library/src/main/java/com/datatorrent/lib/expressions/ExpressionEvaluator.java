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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is used for evaluating expressions which takes multiple parameter object and the result is returned for that expression.
 *
 * The way to reference a variable in an object is ${placeholder.varname}.
 * The variable will be resolved to its accessible variable or getter method in order. After this the variable can be used as if its a Java variable.
 *
 * ExpressionEvaluator also allows you to set extra imports that needs to be added over default is java.lang.*
 *
 * ExpressionEvaluator needs to be configured with following configurations as minimal configuration:
 * 1. Mapping of input object place holders to it corresponding types.
 * This can be done with setInputObjectPlaceholders method.
 * 2. Return type of of expression evaluation.
 * 3. Expression to be evaluated. This is a standard java expression except for referencing the variable inside object JEL syntax needs to be used i.e. ${objectPlaceHolder.varName}
 *
 * Expression cab be defined as per JSL 7 syntax:
 * http://docs.oracle.com/javase/specs/jls/se7/html/jls-14.html
 */
public class ExpressionEvaluator
{
  private static final Logger logger = LoggerFactory.getLogger(ExpressionEvaluator.class);

  private static final String INP_OBJECT_FUNC_VAR = "obj";

  private static final String GET = "get";
  private static final String IS = "is";

  private final Map<String, Class> placeholderClassMapping = new HashMap<>();
  private final Map<String, Integer> placeholderIndexMapping = new HashMap<>();
  private final Set<String> imports = new HashSet<>();
  private String variablePlaceholderPattern = "\\$\\{(.*?)\\}";


  /**
   * Constructor for ExpressionEvaluator.
   * This constructor will add some default java imports for expressions to be compiled.
   */
  public ExpressionEvaluator()
  {
    addDefaultImports();
  }

  /**
   * This is the Expression interface, object of which will be returned to the evaluated expression.
   * The person who want to execute the expression is expected to run the execute method with parameters in the same order
   * as set in setInputObjectPlaceholders method.
   *
   * @param <O> This is generic class return type of execute method of expression.
   */
  public interface Expression<O>
  {
    O execute(Object... obj);
  }

  /**
   * This method of ExpressionEvaluator will create a compiled & executable expression which is go as-is as method's content.
   * The expression provided here is expected to contain a return statement.
   * If expression does not contain return statement, the method will throw RuntimeException.
   *
   * <p>
   *  Example is as follows:
   *  Let the expression be <func_expression>
   *  After compiling and resolving the placeholder, it becomes <compiled_func_expression>
   *
   *  The execute method of Expression interface would look like this:
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
  public <O> Expression<O> createExecutableFunction(String expression, Class<?> returnType)
  {
    return createExecutableCode(expression, returnType, true);
  }


  /**
   * This method of ExpressionEvaluator will create a compiled & executable expression to which return statement is added.
   * The expression provided here is not expected to contain a return statement.
   * Expression should be a single line expression as per JLS 7 as given in:
   * <a>https://docs.oracle.com/javase/specs/jls/se7/html/jls-15.html</a>
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
  public <O> Expression<O> createExecutableExpression(String expression, Class<?> returnType)
  {
    return createExecutableCode(expression, returnType, false);
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
   *       on whether the containsReturnStatement is true OR false. If its true, the code is added as it is
   *       If false, it'll added along with return statement.</li>
   *   <li>Compile the expression using Janino library</li>
   *   <li>Return an object of type Expression which contains compiled expression.</li>
   * </ol>
   *
   * @param expression              String expression that needs to be compiled
   * @param returnType              Return type of the expression.
   * @param containsReturnStatement Whether the expression contains return statement OR not.
   * @param <O>                     Parameterized return class type of expression after evaluation.
   * @return                        Object of type Expression interface having compiled and executable expression.
   */
  @SuppressWarnings({"unchecked"})
  private <O> Expression<O> createExecutableCode(String expression, Class<?> returnType,
      boolean containsReturnStatement)
  {
    if (this.placeholderClassMapping.size() <= 0) {
      throw new RuntimeException("Mapping needs to be provided.");
    }

    Pattern entry = Pattern.compile(variablePlaceholderPattern);
    Matcher matcher = entry.matcher(expression);
    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      if (matcher.groupCount() == 1) {
        matcher.appendReplacement(sb, getObjectJavaExpression(matcher.group(1)));
      } else {
        throw new RuntimeException("Invalid expression: " + matcher.group());
      }
    }

    matcher.appendTail(sb);

    String code = getFinalMethodCode(sb.toString(), returnType, containsReturnStatement);
    try {
      IScriptEvaluator se = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
      se.setDefaultImports(this.imports.toArray(new String[imports.size()]));
      return (Expression)se.createFastEvaluator(code, Expression.class, new String[]{INP_OBJECT_FUNC_VAR});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String getFinalMethodCode(String code, Class<?> returnType, boolean containsReturnStatement)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("if (")
      .append(INP_OBJECT_FUNC_VAR)
      .append(".length != ")
      .append(placeholderClassMapping.size())
      .append(") {")
      .append("throw new RuntimeException(\"Incorrect number of parameters passed to Expression.\");")
      .append("}\n");

    if (containsReturnStatement) {
      sb.append(code);
    } else {
      sb.append("return (")
        .append(returnType.getName())
        .append(")(")
        .append(code)
        .append(");");
    }

    return sb.toString();
  }

  private String getObjectJavaExpression(String exp)
  {
    String[] split = exp.split("\\.");
    if (split.length == 1) {
      // Can be objectPlaceholder OR fieldExpression
      return getSingleOperandReplacement(split[0]);
    } else if (split.length > 0) {
      try {
        return getMultiOperandReplacement(split);
      } catch (NoSuchFieldException e) {
        throw new RuntimeException("Cannot find matching fields from given expression. " + exp);
      }
    } else {
      throw new RuntimeException("No operand find. " + exp);
    }
  }

  private String getMultiOperandReplacement(String[] split) throws NoSuchFieldException
  {
    StringBuilder sb = new StringBuilder();
    if (this.placeholderClassMapping.containsKey(split[0])) {
      Class classType = this.placeholderClassMapping.get(split[0]);
      sb.append(getSingleOperandReplacement(split[0]));

      for (int i = 1; i < split.length; i++) {
        sb.append(".")
          .append(getGetterForVariable(split[i], classType));
        classType = classType.getDeclaredField(split[i]).getType();
      }
    } else {
      throw new RuntimeException("First index in operand should be objectPlaceholder" + split.toString());
    }

    return sb.toString();
  }

  private String getSingleOperandReplacement(String operand)
  {
    StringBuilder sb = new StringBuilder();
    String objectPlaceholder;
    Class classType;
    if (this.placeholderClassMapping.containsKey(operand)) {
      // This is object placeholder.
      objectPlaceholder = operand;
      classType = this.placeholderClassMapping.get(operand);
    } else {
      // This operand is a field variable. Find objectPlaceholder & its class for this.
      // Pick first of the mapping. Give warning if more than one placeholders are registered.
      Map.Entry<String, Class> mapping = this.placeholderClassMapping.entrySet().iterator().next();
      if (this.placeholderClassMapping.size() > 1) {
        logger.warn("More than one placeholder registered. Picking first one.");
      }
      objectPlaceholder = mapping.getKey();
      classType = mapping.getValue();
    }

    String replacement = INP_OBJECT_FUNC_VAR + "[" + this.placeholderIndexMapping.get(objectPlaceholder) + "]";
    sb.append("(")
      .append("(")
      .append(classType.getName().replace("$", "\\$"))
      .append(")")
      .append("(")
      .append(replacement)
      .append(")")
      .append(")");

    if (!this.placeholderClassMapping.containsKey(operand)) {
      // This is fieldPlaceholder. Append getter to expression.
      sb.append(".")
        .append(getGetterForVariable(operand, classType));
    }

    return sb.toString();
  }

  private String getGetterForVariable(String var, Class inputClassType)
  {
    try {
      final Field field = inputClassType.getField(var);
      if (Modifier.isPublic(field.getModifiers())) {
        return var;
      }
      logger.debug("Field {} is not publicly accessible. Proceeding to locate a getter method.", var);
    } catch (NoSuchFieldException | SecurityException ex) {
      logger.debug("{} does not have field {}. Proceeding to locate a getter method.", inputClassType.getName(), var);
    }

    String methodName = GET + var.substring(0, 1).toUpperCase() + var.substring(1);
    try {
      Method method = inputClassType.getMethod(methodName);
      if (Modifier.isPublic(method.getModifiers())) {
        return methodName + "()";
      }
      logger.debug("Method {} of {} is not accessible. Proceeding to locate another getter method.", methodName, inputClassType.getName());
    } catch (NoSuchMethodException | SecurityException ex) {
      logger.debug("{} does not have method {}. Proceeding to locate another getter method", inputClassType.getName(), methodName);
    }

    methodName = IS + var.substring(0, 1).toUpperCase() + var.substring(1);
    try {
      Method method = inputClassType.getMethod(methodName);
      if (Modifier.isPublic(method.getModifiers())) {
        return methodName + "()";
      }
      logger.debug("Method {} of {} is not accessible. Proceeding to locate another getter method.", methodName, inputClassType.getName());
    } catch (NoSuchMethodException | SecurityException ex) {
      logger.debug("{} does not have method {}. Proceeding to locate another getter method", inputClassType.getName(), methodName);
    }

    throw new IllegalArgumentException("Field '" + var + "' is not found in given class " + inputClassType.getName());
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

  /**
   * This method provides order and mapping is object placeholder vs their class types.
   * The order in which the inputObjectPlaceholders are added, the same order needs to be followed while passing
   * parameters to execute method as well.
   *
   * inputObjectPlaceholder are the string placeholder which can be used in expression to reference the object
   * that is passed to execute method of Expression class.
   *
   * <b>NOTE: This registers the order in which parameters are passed to execute method. </b>
   *
   * @param inputObjectPlaceholders Array of string placeholder that can be used in expression to reference the object.
   * @param classTypes              Class type of respective string placeholders in expression.
   */
  public void setInputObjectPlaceholders(String[] inputObjectPlaceholders, Class[] classTypes)
  {
    this.placeholderClassMapping.clear();
    this.placeholderIndexMapping.clear();
    for (int i = 0; i < inputObjectPlaceholders.length; i++) {
      this.placeholderClassMapping.put(inputObjectPlaceholders[i], classTypes[i]);
      this.placeholderIndexMapping.put(inputObjectPlaceholders[i], i);
    }
  }

  /**
   * This setter allows one to override the placeholder expected in String expression.
   * The default value is ${variablePlaceholderPattern}.
   *
   * @param variablePlaceholderPattern Variable placeholder pattern
   */
  public void setVariablePlaceholderPattern(String variablePlaceholderPattern)
  {
    this.variablePlaceholderPattern = variablePlaceholderPattern;
  }
}
