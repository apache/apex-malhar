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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Defines how quasi-Java Expression should be parsed.
 *
 * Instance of this class will go as a plugin to {@link JavaExpressionEvaluator}
 */
public class JavaExpressionParser implements ExpressionEvaluator.ExpressionParser
{
  private static final Logger logger = LoggerFactory.getLogger(JavaExpressionParser.class);

  private static final String GET = "get";
  private static final String IS = "is";

  private final Map<String, Class> placeholderClassMapping = new HashMap<>();
  private final Map<String, Integer> placeholderIndexMapping = new HashMap<>();
  private String variablePlaceholderPattern = "\\$\\{(.*?)\\}";

  /**
   * Constructed added for Kryo Serialization.
   */
  public JavaExpressionParser()
  {
    // for kryo serialization.
  }

  /**
   * {@inheritDoc}
   */
  @Override public String convertToCompilableExpression(String expression, Class<?> returnType,
      boolean isCompleteMethod)
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

    return getFinalMethodCode(sb.toString(), returnType, isCompleteMethod);
  }

  /**
   * {@inheritDoc}
   */
  @Override public void setInputObjectPlaceholders(String[] inputObjectPlaceholders, Class[] classTypes)
  {
    for (int i = 0; i < inputObjectPlaceholders.length; i++) {
      this.placeholderClassMapping.put(inputObjectPlaceholders[i], classTypes[i]);
      this.placeholderIndexMapping.put(inputObjectPlaceholders[i], i);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override public void setVariableRegexPattern(String variablePlaceholderPattern)
  {
    this.variablePlaceholderPattern = variablePlaceholderPattern;
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
        sb.append(".").append(getGetterForVariable(split[i], classType));
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

    String replacement =
        JavaExpressionEvaluator.EXECUTE_INPUT_PARAM_NAME + "[" + this.placeholderIndexMapping.get(objectPlaceholder)
            + "]";
    sb.append("(").append("(").append(classType.getName().replace("$", "\\$")).append(")").append("(")
        .append(replacement).append(")").append(")");

    if (!this.placeholderClassMapping.containsKey(operand)) {
      // This is fieldPlaceholder. Append getter to expression.
      sb.append(".").append(getGetterForVariable(operand, classType));
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
      logger.debug("Method {} of {} is not accessible. Proceeding to locate another getter method.", methodName,
          inputClassType.getName());
    } catch (NoSuchMethodException | SecurityException ex) {
      logger.debug("{} does not have method {}. Proceeding to locate another getter method", inputClassType.getName(),
          methodName);
    }

    methodName = IS + var.substring(0, 1).toUpperCase() + var.substring(1);
    try {
      Method method = inputClassType.getMethod(methodName);
      if (Modifier.isPublic(method.getModifiers())) {
        return methodName + "()";
      }
      logger.debug("Method {} of {} is not accessible. Proceeding to locate another getter method.", methodName,
          inputClassType.getName());
    } catch (NoSuchMethodException | SecurityException ex) {
      logger.debug("{} does not have method {}. Proceeding to locate another getter method", inputClassType.getName(),
          methodName);
    }

    throw new IllegalArgumentException("Field '" + var + "' is not found in given class " + inputClassType.getName());
  }

  private String getFinalMethodCode(String code, Class<?> returnType, boolean containsReturnStatement)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("if (").append(JavaExpressionEvaluator.EXECUTE_INPUT_PARAM_NAME).append(".length != ")
        .append(placeholderClassMapping.size()).append(") {")
        .append("throw new RuntimeException(\"Incorrect number of parameters passed to Expression.\");").append("}\n");

    if (containsReturnStatement) {
      sb.append(code);
    } else {
      sb.append("return (").append(returnType.getName()).append(")(").append(code).append(");");
    }

    return sb.toString();
  }

}
