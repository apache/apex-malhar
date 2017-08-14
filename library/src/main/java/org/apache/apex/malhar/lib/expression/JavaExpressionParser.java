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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.KeyValPair;

/**
 * Defines how quasi-Java Expression should be parsed.
 *
 * @since 3.4.0
 */
public class JavaExpressionParser implements Expression.ExpressionParser
{
  private static final Logger logger = LoggerFactory.getLogger(JavaExpressionParser.class);

  private static final String GET = "get";
  private static final String IS = "is";

  private final String variablePlaceholderPattern = "\\{(.*?)\\}";

  private String exprObjPlaceholder;
  private String codeObjPlaceholder;

  /**
   * {@inheritDoc}
   */
  @Override
  public String convertToCompilableExpression(String expression, Class<?> objectType, Class<?> returnType)
  {
    if (expression.startsWith(".")) {
      expression = expression.substring(1);
    }

    if (expression.isEmpty()) {
      throw new IllegalArgumentException("The getter expression: \"" + expression + "\" is invalid.");
    }

    Pattern entry = Pattern.compile(variablePlaceholderPattern);
    Matcher matcher = entry.matcher(expression);
    StringBuffer sb = new StringBuffer();

    while (matcher.find()) {
      if (matcher.groupCount() == 1) {
        matcher.appendReplacement(sb, getObjectJavaExpression(matcher.group(1), objectType));
      } else {
        throw new RuntimeException("Invalid expression: " + matcher.group());
      }
    }

    matcher.appendTail(sb);

    if (sb.toString().equals(expression)) {
      // This is a simple expression. No object placeholder. create proper expression.
      if (!expression.startsWith(this.exprObjPlaceholder + ".")) {
        expression = this.exprObjPlaceholder + "." + expression;
      }
      String tempExpr = getObjectJavaExpression(expression, objectType);
      sb.setLength(0);
      sb.append(tempExpr.replace("\\$", "$"));
    }

    return "return ((" + returnType.getName().replace("$", "\\$") + ")(" + sb.toString() + "));";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setInputObjectPlaceholder(String exprObjPlaceholder, String codeObjPlaceholder)
  {
    this.exprObjPlaceholder = exprObjPlaceholder;
    this.codeObjPlaceholder = codeObjPlaceholder;
  }

  private String getObjectJavaExpression(String exp, Class objectType)
  {
    StringBuilder sb = new StringBuilder();

    String[] split = exp.split("\\.");
    Class<?> currentClassType = objectType;

    boolean first = true;
    for (String field : split) {
      if (first) {
        first = false;
      } else {
        sb.append(".");
      }
      if (field.equals(exprObjPlaceholder)) {
        // Replace with object type
        sb.append("((").append(objectType.getName().replace("$", "\\$")).append(")").append("(")
            .append(codeObjPlaceholder).append("))");
        currentClassType = objectType;
      } else {
        KeyValPair<String, ? extends Class<?>> getter = getGetterForVariable(field, currentClassType);
        sb.append(getter.getKey());
        currentClassType = getter.getValue();
      }
    }

    return sb.toString();
  }

  private KeyValPair<String, ? extends Class<?>> getGetterForVariable(String var, Class<?> inputClassType)
  {
    try {
      final Field field = inputClassType.getField(var);
      if (Modifier.isPublic(field.getModifiers())) {
        return new KeyValPair<>(var, field.getType());
      }
      logger.debug("Field {} is not publicly accessible. Proceeding to locate a getter method.", var);
    } catch (NoSuchFieldException ex) {
      logger.debug("{} does not have field {}. Proceeding to locate a getter method.", inputClassType.getName(), var);
    }

    String[] methodAccessors = new String[]{GET, IS};

    for (String m : methodAccessors) {
      String methodName = m + var.substring(0, 1).toUpperCase() + var.substring(1);
      try {
        Method method = inputClassType.getMethod(methodName);
        if (Modifier.isPublic(method.getModifiers())) {
          return new KeyValPair<>(methodName + "()", method.getReturnType());
        }
        logger.debug("Method {} of {} is not accessible. Proceeding to locate another getter method.", methodName,
            inputClassType.getName());
      } catch (NoSuchMethodException ex) {
        logger.debug("{} does not have method {}. Proceeding to locate another getter method", inputClassType.getName(),
            methodName);
      }
    }

    return new KeyValPair<>(var, inputClassType);
  }

}
