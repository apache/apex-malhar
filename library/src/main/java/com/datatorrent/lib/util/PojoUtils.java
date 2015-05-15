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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ClassUtils;

public class PojoUtils
{
  private static final Logger logger = LoggerFactory.getLogger(PojoUtils.class);

  private static final String JAVA_DOT = ".";
  private static final String DEFAULT_POJO_NAME = "pojo";
  private static final String DEFAULT_SETTER_ARG = "val";

  private static final String GET = "get";
  private static final String IS = "is";
  private static final String SET = "set";

  private PojoUtils()
  {
  }

  public interface GetterBoolean
  {
    public boolean get(final Object obj);
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

  /**
   * Setter interface for <tt>boolean</tt> primitives
   * @param <T> class of objects that the setter applies to
   */
  public interface SetterBoolean<T>
  {
    void set(T obj, final boolean booleanVal);
  }

  /**
   * Setter interface for <tt>byte</tt> primitives
   * @param <T> class of objects that the setter applies to
   */
  public interface SetterByte<T>
  {
    void set(T obj, final byte byteVal);
  }

  /**
   * Setter interface for <tt>char</tt> primitives
   * @param <T> class of objects that the setter applies to
   */
  public interface SetterChar<T>
  {
    void set(T obj, final char charVal);
  }

  /**
   * Setter interface for <tt>double</tt> primitives
   * @param <T> class of objects that the setter applies to
   */
  public interface SetterDouble<T>
  {
    void set(T obj, final double doubleVal);
  }

  /**
   * Setter interface for <tt>float</tt> primitives
   * @param <T> class of objects that the setter applies to
   */
  public interface SetterFloat<T>
  {
    void set(T obj, final float floatVal);
  }

  /**
   * Setter interface for <tt>int</tt> primitives
   * @param <T> class of objects that the setter applies to
   */
  public interface SetterInt<T>
  {
    void set(T obj, final int intVal);
  }

  /**
   * Setter interface for <tt>long</tt> primitives
   * @param <T> class of objects that the setter applies to
   */
  public interface SetterLong<T>
  {
    void set(T obj, final long longVal);
  }

  /**
   * Setter interface for <tt>short</tt> primitives
   * @param <T> class of objects that the setter applies to
   */
  public interface SetterShort<T>
  {
    void set(T obj, final short shortVal);
  }

  /**
   * Setter interface for arbitrary object
   * @param <T> class of objects that the setter applies to
   * @param <V> class of the rhs expression
   */
  public interface Setter<T, V>
  {
    void set(T obj, final V value);
  }

  public static String upperCaseWord(String field)
  {
    Preconditions.checkArgument(!field.isEmpty(), field);
    return field.substring(0, 1).toUpperCase() + field.substring(1);
  }

  /**
   * Return the getter expression for the given field.
   * <p>
   * If the field is a public member, the field name is used else the getter function. If no matching field or getter
   * method is found, the expression is returned unmodified.
   *
   * @param clazz
   * @param fieldExpression
   * @return
   */
  public static String getSingleFieldGetterExpression(Class<?> clazz, final String fieldExpression)
  {
    try {
      final Field f = clazz.getField(fieldExpression);
      return f.getName();
    } catch (NoSuchFieldException ex) {
      logger.debug("Class " + clazz.getName() + " does not have field " + fieldExpression + ". Will try to find getter.", ex);
    }

    try {
      Method m = clazz.getMethod(GET + upperCaseWord(fieldExpression));
      return m.getName().concat("()");
    } catch (NoSuchMethodException ex) {
      logger.debug("Class " + clazz.getName() + " does not have method " + GET + upperCaseWord(fieldExpression) +
              ". Will try to find another getter.", ex);
    }

    try {
      Method m = clazz.getMethod(IS + upperCaseWord(fieldExpression));
      return m.getName().concat("()");
    } catch (NoSuchMethodException ex) {
      logger.debug("Class " + clazz.getName() + " does not have method " + IS + upperCaseWord(fieldExpression) +
              ". Returning original expression " + fieldExpression, ex);
    }

    return fieldExpression;
  }

  public static String fieldListToGetExpression(Class<?> clazz, List<String> fields)
  {
    StringBuilder sb = new StringBuilder();

    for (int index = 0; index < fields.size() - 1; index++) {
      String field = fields.get(index);
      sb.append(sb).append(getSingleFieldGetterExpression(clazz, field)).append(JAVA_DOT);
    }

    sb.append(getSingleFieldGetterExpression(clazz, fields.get(fields.size() - 1)));

    return sb.toString();
  }

  public static Object createGetter(Class<?> pojoClass, String getterExpr, Class<?> castClass, Class<?> getterClass)
  {
    logger.debug("{} {} {} {}", pojoClass, getterExpr, castClass, getterClass);

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
      String code = "return (" + castClass.getName() + ") (((" + pojoClass.getName() + ")" + PojoUtils.DEFAULT_POJO_NAME + ")." + getterExpr + ");";
      logger.debug("{}", code);

      return se.createFastEvaluator(code, getterClass, new String[] { PojoUtils.DEFAULT_POJO_NAME });
    } catch (CompileException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static GetterBoolean createGetterBoolean(Class<?> pojoClass, String getterExpr)
  {
    return (GetterBoolean) createGetter(pojoClass, getterExpr, boolean.class, GetterBoolean.class);
  }

  public static GetterByte createGetterByte(Class<?> pojoClass, String getterExpr)
  {
    return (GetterByte) createGetter(pojoClass, getterExpr, byte.class, GetterByte.class);
  }

  public static GetterChar createGetterChar(Class<?> pojoClass, String getterExpr)
  {
    return (GetterChar) createGetter(pojoClass, getterExpr, char.class, GetterChar.class);
  }

  public static GetterDouble createGetterDouble(Class<?> pojoClass, String getterExpr)
  {
    return (GetterDouble) createGetter(pojoClass, getterExpr, double.class, GetterDouble.class);
  }

  public static GetterFloat createGetterFloat(Class<?> pojoClass, String getterExpr)
  {
    return (GetterFloat) createGetter(pojoClass, getterExpr, float.class, GetterFloat.class);
  }

  public static GetterInt createGetterInt(Class<?> pojoClass, String getterExpr)
  {
    return (GetterInt) createGetter(pojoClass, getterExpr, int.class, GetterInt.class);
  }

  public static GetterLong createGetterLong(Class<?> pojoClass, String getterExpr)
  {
    return (GetterLong) createGetter(pojoClass, getterExpr, long.class, GetterLong.class);
  }

  public static GetterShort createGetterShort(Class<?> pojoClass, String getterExpr)
  {
    return (GetterShort) createGetter(pojoClass, getterExpr, short.class, GetterShort.class);
  }

  public static GetterString createGetterString(Class<?> pojoClass, String getterExpr)
  {
    return (GetterString) createGetter(pojoClass, getterExpr, String.class, GetterString.class);
  }

  public static GetterObject createGetterObject(Class<?> pojoClass, String getterExpr)
  {
    return (GetterObject) createGetter(pojoClass, getterExpr, Object.class, GetterObject.class);
  }

  private static String getSingleFieldSetterExpression(final Class<?> pojoClass, final String fieldExpression, final Class<?> exprClass)
  {
    StringBuilder code = new StringBuilder(pojoClass.getName().length() + exprClass.getName().length() + 16);
    /* Construct ((<pojo class name>)pojo). */
    code.append("((").append(pojoClass.getName()).append(")").append(PojoUtils.DEFAULT_POJO_NAME).append(").");
    try {
      final Field classField = pojoClass.getField(fieldExpression);
      Class<?> type = classField.getType();
      if (ClassUtils.isAssignable(exprClass, type)) {
        /* there is public field on the class, use direct assignment. */
        /* append <field name> = (<field type>)val; */
        code.append(classField.getName()).append(" = (").append(exprClass.getName()).append(")").append(PojoUtils.DEFAULT_SETTER_ARG).append(";");
        return code.toString();
      }
      else if (logger.isDebugEnabled()) {
        logger.debug("{}", "Class" + pojoClass.getName() + " does not have field that " + exprClass.getName() + " may be assigned to." +
          "Will try to find setter");
      }
    } catch (NoSuchFieldException ex) {
      logger.debug("Class " + pojoClass.getName() + " does not have field " + fieldExpression + ". Will try to find setter.", ex);
    }

    final String setMethodName = SET + upperCaseWord(fieldExpression);
    Method bestMatchMethod = null;
    List<Method> candidates = new ArrayList<Method>();
    for (Method method : pojoClass.getMethods()) {
      if (setMethodName.equals(method.getName())) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length == 1) {
          if (exprClass == parameterTypes[0]) {
            bestMatchMethod = method;
            break;
          }
          else if (org.apache.commons.lang.ClassUtils.isAssignable(exprClass, parameterTypes[0])) {
            candidates.add(method);
          }
        }
      }
    }

    if (bestMatchMethod == null) { // We did not find the exact match, use candidates to find the match
      if (candidates.size() == 0) {
        if (logger.isDebugEnabled()) {
          logger.debug("Class " + pojoClass.getName() + " does not have method " + SET + upperCaseWord(fieldExpression) +
                  "(" + exprClass.getName() + "). Returning original expression " + fieldExpression);
        }
        /* We did not find any match at all, use original expression */
        /* append = (<expr type>)val;*/
        code.append(fieldExpression).append(" = (").append(exprClass.getName()).append(")").append(PojoUtils.DEFAULT_SETTER_ARG).append(";");
        return code.toString();
      } else {
        // TODO: see if we can find a better match
        bestMatchMethod = candidates.get(0);
      }
    }

    /* We found a method that we may use for setter */
    /* append <method name>((<expr class)val); */
    code.append(bestMatchMethod.getName()).append("((").append(exprClass.getName()).append(")").append(PojoUtils.DEFAULT_SETTER_ARG).append(");");
    return code.toString();
  }

  /**
   *
   * @param pojoClass Class object that the setter applies to
   * @param setterExpr expression to use for setter
   * @param exprClass Class that setter will accept as parameter
   * @param setterClass setter interface to implement
   * @return instance of a class that implements requested Setter interface
   */
  public static Object createSetter(Class<?> pojoClass, String setterExpr, Class<?> exprClass, Class<?> setterClass)
  {
    logger.debug("{} {} {} {}", pojoClass, setterExpr, exprClass, setterClass);

    if (setterExpr.startsWith(".")) {
      setterExpr = setterExpr.substring(1);
    }

    if (setterExpr.isEmpty()) {
      throw new IllegalArgumentException("The setter string: " + setterExpr + "\nis invalid.");
    }

    IScriptEvaluator se;

    try {
      se = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    try {

      String code = getSingleFieldSetterExpression(pojoClass, setterExpr, exprClass);

      logger.debug("{}", code);

      return se.createFastEvaluator(code, setterClass, new String[] { PojoUtils.DEFAULT_POJO_NAME, PojoUtils.DEFAULT_SETTER_ARG });
    } catch (CompileException ex) {
      throw new RuntimeException(ex);
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> SetterBoolean<T> createSetterBoolean(Class<? extends T> pojoClass, String setterExpr)
  {
    return (SetterBoolean<T>) createSetter(pojoClass, setterExpr, boolean.class, SetterBoolean.class);
  }

  @SuppressWarnings("unchecked")
  public static <T> SetterByte<T> createSetterByte(Class<? extends T> pojoClass, String setterExpr)
  {
    return (SetterByte<T>) createSetter(pojoClass, setterExpr, byte.class, SetterByte.class);
  }

  @SuppressWarnings("unchecked")
  public static <T> SetterChar<T> createSetterChar(Class<? extends T> pojoClass, String setterExpr)
  {
    return (SetterChar<T>) createSetter(pojoClass, setterExpr, char.class, SetterChar.class);
  }

  @SuppressWarnings("unchecked")
  public static <T> SetterDouble<T> createSetterDouble(Class<? extends T> pojoClass, String setterExpr)
  {
    return (SetterDouble<T>) createSetter(pojoClass, setterExpr, double.class, SetterDouble.class);
  }

  @SuppressWarnings("unchecked")
  public static <T> SetterFloat<T> createSetterFloat(Class<? extends T> pojoClass, String setterExpr)
  {
    return (SetterFloat<T>) createSetter(pojoClass, setterExpr, float.class, SetterFloat.class);
  }

  @SuppressWarnings("unchecked")
  public static <T> SetterInt<T> createSetterInt(Class<? extends T> pojoClass, String setterExpr)
  {
    return (SetterInt<T>) createSetter(pojoClass, setterExpr, int.class, SetterInt.class);
  }

  @SuppressWarnings("unchecked")
  public static <T> SetterLong<T> createSetterLong(Class<? extends T> pojoClass, String setterExpr)
  {
    return (SetterLong<T>) createSetter(pojoClass, setterExpr, long.class, SetterLong.class);
  }

  @SuppressWarnings("unchecked")
  public static <T> SetterShort<T> createSetterShort(Class<? extends T> pojoClass, String setterExpr)
  {
    return (SetterShort<T>) createSetter(pojoClass, setterExpr, short.class, SetterShort.class);
  }

  @SuppressWarnings("unchecked")
  public static <T, V> Setter<T, V> createSetter(Class<? extends T>pojoClass, String setterExpr, Class<? extends V> exprClass)
  {
    return (Setter<T, V>) createSetter(pojoClass, setterExpr, exprClass, Setter.class);
  }

}
