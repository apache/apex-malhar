/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.util;

import com.google.common.base.Preconditions;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

public class ConvertUtils
{
  private static final Logger logger = LoggerFactory.getLogger(ConvertUtils.class);

  public static final String JAVA_DOT = ".";
  public static final String DEFAULT_POJO_NAME = "pojo";

  public static final String GET = "get";
  public static final String IS = "is";

  private ConvertUtils()
  {
  }

  public interface GetterBoolean
  {
    public boolean get(Object obj);
  }

  public interface GetterByte
  {
    public byte get(Object obj);
  }

  public interface GetterChar
  {
    public char get(Object obj);
  }

  public interface GetterDouble
  {
    public double get(Object obj);
  }

  public interface GetterFloat
  {
    public float get(Object obj);
  }

  public interface GetterInt
  {
    public int get(Object obj);
  }

  public interface GetterLong
  {
    public long get(Object obj);
  }

  public interface GetterObject
  {
    public Object get(Object obj);
  }

  public interface GetterShort
  {
    public short get(Object obj);
  }

  public interface GetterString
  {
    public String get(Object obj);
  }

  public static String upperCaseWord(String field)
  {
    Preconditions.checkArgument(!field.isEmpty(), field);
    return field.substring(0, 1).toUpperCase() + field.substring(1);
  }

  /**
   * Return the getter expression for the given field.
   * <p>
   * If the field is a public member, the field name is used else the getter function. If not matching field or getter
   * method is found, the expression is returned unmodified.
   *
   * @param clazz
   * @param fieldExpression
   * @return
   */
  public static String getSingleFieldExpression(Class<?> clazz, String fieldExpression)
  {
    try {
      Field f = clazz.getField(fieldExpression);
      return f.getName();
    } catch (NoSuchFieldException ex) {
      try {
        Method m = clazz.getMethod(GET + upperCaseWord(fieldExpression));
        return m.getName().concat("()");
      } catch (NoSuchMethodException nsm) {
        try {
          Method m = clazz.getMethod(IS + upperCaseWord(fieldExpression));
          return m.getName().concat("()");
        } catch (NoSuchMethodException nsm2) {
        }
      }
      return fieldExpression;
    }
  }

  public static String fieldListToGetExpression(Class<?> clazz, List<String> fields)
  {
    StringBuilder sb = new StringBuilder();

    for (int index = 0; index < fields.size() - 1; index++) {
      String field = fields.get(index);
      sb.append(sb).append(getSingleFieldExpression(clazz, field)).append(JAVA_DOT);
    }

    sb.append(getSingleFieldExpression(clazz, fields.get(fields.size() - 1)));

    return sb.toString();
  }

  public static Object createExpressionGetter(String fqClassName, String getterExpr, Class<?> castClass, Class<?> getterClass)
  {
    logger.debug("{} {} {} {}", fqClassName, getterExpr, castClass, getterClass);

    if (getterExpr.startsWith(".")) {
      getterExpr = getterExpr.substring(1);
    }

    if (getterExpr.isEmpty()) {
      throw new IllegalArgumentException("The getter string: " + getterExpr + "\nis invalid.");
    }

    IScriptEvaluator se = null;

    try {
      se = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    try {
      String code = "return (" + castClass.getName() + ") (((" + fqClassName + ")" + ConvertUtils.DEFAULT_POJO_NAME + ")." + getterExpr + ");";
      logger.debug("{}", code);

      return se.createFastEvaluator(code, getterClass, new String[] { ConvertUtils.DEFAULT_POJO_NAME });
    } catch (CompileException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static GetterBoolean createExpressionGetterBoolean(String fqClassName, String getterExpr)
  {
    return (GetterBoolean) createExpressionGetter(fqClassName, getterExpr, boolean.class, GetterBoolean.class);
  }

  public static GetterByte createExpressionGetterByte(String fqClassName, String getterExpr)
  {
    return (GetterByte) createExpressionGetter(fqClassName, getterExpr, byte.class, GetterByte.class);
  }

  public static GetterChar createExpressionGetterChar(String fqClassName, String getterExpr)
  {
    return (GetterChar) createExpressionGetter(fqClassName, getterExpr, char.class, GetterChar.class);
  }

  public static GetterDouble createExpressionGetterDouble(String fqClassName, String getterExpr)
  {
    return (GetterDouble) createExpressionGetter(fqClassName, getterExpr, double.class, GetterDouble.class);
  }

  public static GetterFloat createExpressionGetterFloat(String fqClassName, String getterExpr)
  {
    return (GetterFloat) createExpressionGetter(fqClassName, getterExpr, float.class, GetterFloat.class);
  }

  public static GetterInt createExpressionGetterInt(String fqClassName, String getterExpr)
  {
    return (GetterInt) createExpressionGetter(fqClassName, getterExpr, int.class, GetterInt.class);
  }

  public static GetterLong createExpressionGetterLong(String fqClassName, String getterExpr)
  {
    return (GetterLong) createExpressionGetter(fqClassName, getterExpr, long.class, GetterLong.class);
  }

  public static GetterShort createExpressionGetterShort(String fqClassName, String getterExpr)
  {
    return (GetterShort) createExpressionGetter(fqClassName, getterExpr, short.class, GetterShort.class);
  }

  public static GetterString createExpressionGetterString(String fqClassName, String getterExpr)
  {
    return (GetterString) createExpressionGetter(fqClassName, getterExpr, String.class, GetterString.class);
  }

  public static GetterObject createExpressionGetterObject(String fqClassName, String getterExpr)
  {
    return (GetterObject) createExpressionGetter(fqClassName, getterExpr, Object.class, GetterObject.class);
  }

  public static Object createExtractionGetter(String extractionString, Class<?> getterClass)
  {
    IScriptEvaluator se = null;

    try {
      se = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    try {
      return se.createFastEvaluator(extractionString, getterClass, new String[] { ConvertUtils.DEFAULT_POJO_NAME });
    } catch (CompileException ex) {
      throw new RuntimeException(ex);
    }
  }

}
