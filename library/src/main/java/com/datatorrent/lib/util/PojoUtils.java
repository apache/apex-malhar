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
import org.apache.commons.lang3.StringUtils;

/**
 * @since 2.1.0
 */
public class PojoUtils
{
  private static final Logger logger = LoggerFactory.getLogger(PojoUtils.class);

  public static final String DEFAULT_EXP_OBJECT_PLACEHOLDER = "{$}";
  public static final String DEFAULT_EXP_VAL_PLACEHOLDER = "{#}";

  private static final String OBJECT = "object";
  private static final String VAL = "val";

  private static final String GET = "get";
  private static final String IS = "is";
  private static final String SET = "set";

  private PojoUtils()
  {
  }

  public interface GetterBoolean<T>
  {
    boolean get(T obj);
  }

  public interface GetterByte<T>
  {
    byte get(T obj);
  }

  public interface GetterChar<T>
  {
    char get(T obj);
  }

  public interface GetterShort<T>
  {
    short get(T obj);
  }

  public interface GetterInt<T>
  {
    int get(T obj);
  }

  public interface GetterLong<T>
  {
    long get(T obj);
  }

  public interface GetterFloat<T>
  {
    float get(T obj);
  }

  public interface GetterDouble<T>
  {
    double get(T obj);
  }

  public interface Getter<T, V>
  {
    V get(T obj);
  }

  public static <T> GetterBoolean<T> createGetterBoolean(Class<? extends T> pojoClass, String getterExpr)
  {
    return createGetterBoolean(pojoClass, getterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER);
  }

  @SuppressWarnings("unchecked")
  public static <T> GetterBoolean<T> createGetterBoolean(Class<? extends T> pojoClass, String getterExpr, String exprObjectPlaceholder)
  {
    return (GetterBoolean<T>) createGetter(pojoClass, getterExpr, exprObjectPlaceholder, boolean.class, GetterBoolean.class);
  }

  public static <T> GetterByte<T> createGetterByte(Class<? extends T> pojoClass, String getterExpr)
  {
    return createGetterByte(pojoClass, getterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER);
  }

  @SuppressWarnings("unchecked")
  public static <T> GetterByte<T> createGetterByte(Class<? extends T> pojoClass, String getterExpr, String exprObjectPlaceholder)
  {
    return (GetterByte<T>) createGetter(pojoClass, getterExpr, exprObjectPlaceholder, byte.class, GetterByte.class);
  }

  public static <T> GetterChar<T> createGetterChar(Class<? extends T> pojoClass, String getterExpr)
  {
    return createGetterChar(pojoClass, getterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER);
  }

  @SuppressWarnings({ "unchecked"})
  public static <T> GetterChar<T> createGetterChar(Class<? extends T> pojoClass, String getterExpr, String exprObjectPlaceholder)
  {
    return (GetterChar<T>) createGetter(pojoClass, getterExpr, exprObjectPlaceholder, char.class, GetterChar.class);
  }

  public static <T> GetterShort<T> createGetterShort(Class<? extends T> pojoClass, String getterExpr)
  {
    return createGetterShort(pojoClass, getterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER);
  }

  @SuppressWarnings("unchecked")
  public static <T> GetterShort<T> createGetterShort(Class<? extends T> pojoClass, String getterExpr, String exprObjectPlaceholder)
  {
    return (GetterShort<T>) createGetter(pojoClass, getterExpr, exprObjectPlaceholder, short.class, GetterShort.class);
  }

  public static <T> GetterInt<T> createGetterInt(Class<? extends T> pojoClass, String getterExpr)
  {
    return createGetterInt(pojoClass, getterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER);
  }

  @SuppressWarnings("unchecked")
  public static <T> GetterInt<T> createGetterInt(Class<? extends T> pojoClass, String getterExpr, String exprObjectPlaceholder)
  {
    return (GetterInt<T>) createGetter(pojoClass, getterExpr, exprObjectPlaceholder, int.class, GetterInt.class);
  }

  public static <T> GetterLong<T> createGetterLong(Class<? extends T> pojoClass, String getterExpr)
  {
    return createGetterLong(pojoClass, getterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER);
  }

  @SuppressWarnings("unchecked")
  public static <T> GetterLong<T> createGetterLong(Class<? extends T> pojoClass, String getterExpr, String exprObjectPlaceholder)
  {
    return (GetterLong<T>) createGetter(pojoClass, getterExpr, exprObjectPlaceholder, long.class, GetterLong.class);
  }

  public static <T> GetterFloat<T> createGetterFloat(Class<? extends T> pojoClass, String getterExpr)
  {
    return createGetterFloat(pojoClass, getterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER);
  }

  @SuppressWarnings("unchecked")
  public static <T> GetterFloat<T> createGetterFloat(Class<? extends T> pojoClass, String getterExpr, String exprObjectPlaceholder)
  {
    return (GetterFloat<T>) createGetter(pojoClass, getterExpr, exprObjectPlaceholder, float.class, GetterFloat.class);
  }

  public static <T> GetterDouble<T> createGetterDouble(Class<? extends T> pojoClass, String getterExpr)
  {
    return createGetterDouble(pojoClass, getterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER);
  }

  @SuppressWarnings("unchecked")
  public static <T> GetterDouble<T> createGetterDouble(Class<? extends T> pojoClass, String getterExpr, String exprObjectPlaceholder)
  {
    return (GetterDouble<T>) createGetter(pojoClass, getterExpr, exprObjectPlaceholder, double.class, GetterDouble.class);
  }

  public static <T, V> Getter<T, V> createGetter(Class<? extends T> pojoClass, String getterExpr, Class<? extends V> exprClass)
  {
    return createGetter(pojoClass, getterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER, exprClass);
  }

  @SuppressWarnings("unchecked")
  public static <T, V> Getter<T, V> createGetter(Class<? extends T> pojoClass, String getterExpr, String exprObjectPlaceholder, Class<? extends V> exprClass)
  {
    return (Getter<T, V>) createGetter(pojoClass, getterExpr, exprObjectPlaceholder, exprClass, Getter.class);
  }

  /**
   * Setter interface for <tt>boolean</tt> primitives
   * @param <T> class of objects that the setter applies to
   */
  public interface SetterBoolean<T>
  {
    void set(T obj, boolean booleanVal);
  }

  /**
   * Setter interface for <tt>byte</tt> primitives
   * @param <T> class of objects that the setter applies to
   */
  public interface SetterByte<T>
  {
    void set(T obj, byte byteVal);
  }

  /**
   * Setter interface for <tt>char</tt> primitives
   * @param <T> class of objects that the setter applies to
   */
  public interface SetterChar<T>
  {
    void set(T obj, char charVal);
  }

  /**
   * Setter interface for <tt>short</tt> primitives
   * @param <T> class of objects that the setter applies to
   */
  public interface SetterShort<T>
  {
    void set(T obj, short shortVal);
  }

  /**
   * Setter interface for <tt>int</tt> primitives
   * @param <T> class of objects that the setter applies to
   */
  public interface SetterInt<T>
  {
    void set(T obj, int intVal);
  }

  /**
   * Setter interface for <tt>long</tt> primitives
   * @param <T> class of objects that the setter applies to
   */
  public interface SetterLong<T>
  {
    void set(T obj, long longVal);
  }

  /**
   * Setter interface for <tt>float</tt> primitives
   * @param <T> class of objects that the setter applies to
   */
  public interface SetterFloat<T>
  {
    void set(T obj, float floatVal);
  }

  /**
   * Setter interface for <tt>double</tt> primitives
   * @param <T> class of objects that the setter applies to
   */
  public interface SetterDouble<T>
  {
    void set(T obj, double doubleVal);
  }

  /**
   * Setter interface for arbitrary object
   * @param <T> class of objects that the setter applies to
   * @param <V> class of the rhs expression
   */
  public interface Setter<T, V>
  {
    void set(T obj, V value);
  }

  public static <T> SetterBoolean<T> createSetterBoolean(Class<? extends T> pojoClass, String setterExpr)
  {
    return createSetterBoolean(pojoClass, setterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER, DEFAULT_EXP_VAL_PLACEHOLDER);
  }

  @SuppressWarnings("unchecked")
  public static <T> SetterBoolean<T> createSetterBoolean(Class<? extends T> pojoClass, String setterExpr, String exprObjectPlaceholder, String exprValuePlaceholder)
  {
    return (SetterBoolean<T>) createSetter(pojoClass, setterExpr, exprObjectPlaceholder, exprValuePlaceholder, boolean.class, SetterBoolean.class);
  }

  public static <T> SetterByte<T> createSetterByte(Class<? extends T> pojoClass, String setterExpr)
  {
    return createSetterByte(pojoClass, setterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER, DEFAULT_EXP_VAL_PLACEHOLDER);
  }

  @SuppressWarnings("unchecked")
  public static <T> SetterByte<T> createSetterByte(Class<? extends T> pojoClass, String setterExpr, String exprObjectPlaceholder, String exprValuePlaceholder)
  {
    return (SetterByte<T>) createSetter(pojoClass, setterExpr, exprObjectPlaceholder, exprValuePlaceholder, byte.class, SetterByte.class);
  }

  public static <T> SetterChar<T> createSetterChar(Class<? extends T> pojoClass, String setterExpr)
  {
    return createSetterChar(pojoClass, setterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER, DEFAULT_EXP_VAL_PLACEHOLDER);
  }

  @SuppressWarnings("unchecked")
  public static <T> SetterChar<T> createSetterChar(Class<? extends T> pojoClass, String setterExpr, String exprObjectPlaceholder, String exprValuePlaceholder)
  {
    return (SetterChar<T>) createSetter(pojoClass, setterExpr, exprObjectPlaceholder, exprValuePlaceholder, char.class, SetterChar.class);
  }

  public static <T> SetterShort<T> createSetterShort(Class<? extends T> pojoClass, String setterExpr)
  {
    return createSetterShort(pojoClass, setterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER, DEFAULT_EXP_VAL_PLACEHOLDER);
  }

  @SuppressWarnings("unchecked")
  public static <T> SetterShort<T> createSetterShort(Class<? extends T> pojoClass, String setterExpr, String exprObjectPlaceholder, String exprValuePlaceholder)
  {
    return (SetterShort<T>) createSetter(pojoClass, setterExpr, exprObjectPlaceholder, exprValuePlaceholder, short.class, SetterShort.class);
  }

  public static <T> SetterInt<T> createSetterInt(Class<? extends T> pojoClass, String setterExpr)
  {
    return createSetterInt(pojoClass, setterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER, DEFAULT_EXP_VAL_PLACEHOLDER);
  }

  @SuppressWarnings("unchecked")
  public static <T> SetterInt<T> createSetterInt(Class<? extends T> pojoClass, String setterExpr, String exprObjectPlaceholder, String exprValuePlaceholder)
  {
    return (SetterInt<T>) createSetter(pojoClass, setterExpr, exprObjectPlaceholder, exprValuePlaceholder, int.class, SetterInt.class);
  }

  public static <T> SetterLong<T> createSetterLong(Class<? extends T> pojoClass, String setterExpr)
  {
    return createSetterLong(pojoClass, setterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER, DEFAULT_EXP_VAL_PLACEHOLDER);
  }

  @SuppressWarnings("unchecked")
  public static <T> SetterLong<T> createSetterLong(Class<? extends T> pojoClass, String setterExpr, String exprObjectPlaceholder, String exprValuePlaceholder)
  {
    return (SetterLong<T>) createSetter(pojoClass, setterExpr, exprObjectPlaceholder, exprValuePlaceholder, long.class, SetterLong.class);
  }

  public static <T> SetterFloat<T> createSetterFloat(Class<? extends T> pojoClass, String setterExpr)
  {
    return createSetterFloat(pojoClass, setterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER, DEFAULT_EXP_VAL_PLACEHOLDER);
  }

  @SuppressWarnings("unchecked")
  public static <T> SetterFloat<T> createSetterFloat(Class<? extends T> pojoClass, String setterExpr, String exprObjectPlaceholder, String exprValuePlaceholder)
  {
    return (SetterFloat<T>) createSetter(pojoClass, setterExpr, exprObjectPlaceholder, exprValuePlaceholder, float.class, SetterFloat.class);
  }

  public static <T> SetterDouble<T> createSetterDouble(Class<? extends T> pojoClass, String setterExpr)
  {
    return createSetterDouble(pojoClass, setterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER, DEFAULT_EXP_VAL_PLACEHOLDER);
  }

  @SuppressWarnings("unchecked")
  public static <T> SetterDouble<T> createSetterDouble(Class<? extends T> pojoClass, String setterExpr, String exprObjectPlaceholder, String exprValuePlaceholder)
  {
    return (SetterDouble<T>) createSetter(pojoClass, setterExpr, exprObjectPlaceholder, exprValuePlaceholder, double.class, SetterDouble.class);
  }

  public static <T, V> Setter<T, V> createSetter(Class<? extends T>pojoClass, String setterExpr, Class<? extends V> exprClass)
  {
    return createSetter(pojoClass, setterExpr, DEFAULT_EXP_OBJECT_PLACEHOLDER, DEFAULT_EXP_VAL_PLACEHOLDER, exprClass);
  }

  @SuppressWarnings("unchecked")
  public static <T, V> Setter<T, V> createSetter(Class<? extends T>pojoClass, String setterExpr, String exprObjectPlaceholder, String exprValuePlaceholder, Class<? extends V> exprClass)
  {
    return (Setter<T, V>) createSetter(pojoClass, setterExpr, exprObjectPlaceholder, exprValuePlaceholder, exprClass, Setter.class);
  }

  private static class JavaStatement {
    private final StringBuilder javaStatement;
    private final int capacity;

    private JavaStatement() {
      javaStatement = new StringBuilder();
      capacity = javaStatement.capacity();
    }

    private JavaStatement(int length) {
      javaStatement = new StringBuilder(length);
      capacity = javaStatement.capacity();
    }

    @Override
    public String toString() {
      return javaStatement.toString();
    }

    protected JavaStatement append(String string) {
      javaStatement.append(string);
      return this;
    }

    private JavaStatement appendCastToTypeExpr(Class<?> type, String expr) {
      return append("((").append(type.getName()).append(")(").append(expr).append("))");
    }

    protected String getStatement() {
      if (capacity < javaStatement.length() + 1) {
        logger.debug("Java statement capacity {} was not sufficient for the statement length {}. Actual statement {}", capacity, javaStatement.length(), javaStatement);
      }
      return javaStatement.append(";").toString();
    }
  }

  private static class JavaReturnStatement extends JavaStatement {
    private JavaReturnStatement(Class<?> returnType) {
      super();
      append("return (").append(returnType.getName()).append(")");
    }

    private JavaReturnStatement(int length, Class<?> returnType) {
      super(length);
      append("return ((").append(returnType.getName()).append(")");
    }

    @Override
    protected String getStatement() {
      append(")");
      return super.getStatement();
    }
  }

  private static String upperCaseWord(String field)
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
   * @param pojoClass
   * @param fieldExpression
   * @param castClass
   * @return
   */
  private static String getSingleFieldGetterExpression(final Class<?> pojoClass, final String fieldExpression, final Class<?> castClass)
  {
    JavaStatement code = new JavaReturnStatement(pojoClass.getName().length() + fieldExpression.length() + castClass.getName().length() + 32, castClass);
    code.appendCastToTypeExpr(pojoClass, OBJECT).append(".");
    try {
      final Field field = pojoClass.getField(fieldExpression);
      if (ClassUtils.isAssignable(field.getType(), castClass)) {
        return code.append(field.getName()).getStatement();
      }
      logger.debug("Field {} can not be assigned to an {}. Proceeding to locate a getter method.", field, castClass);
    } catch (NoSuchFieldException ex) {
      logger.debug("{} does not have field {}. Proceeding to locate a getter method.", pojoClass, fieldExpression);
    } catch (SecurityException ex) {
      logger.debug("{} does not have field {}. Proceeding to locate a getter method.", pojoClass, fieldExpression);
    }

    String methodName = GET + upperCaseWord(fieldExpression);
    try {
      Method method = pojoClass.getMethod(methodName);
      if (ClassUtils.isAssignable(method.getReturnType(), castClass)) {
        return code.append(methodName).append("()").getStatement();
      }
      logger.debug("method {} of the {} returns {} that can not be assigned to an {}. Proceeding to locate another getter method.",
              pojoClass, methodName, method.getReturnType(), castClass);
    } catch (NoSuchMethodException ex) {
      logger.debug("{} does not have method {}. Proceeding to locate another getter method.",
              pojoClass, methodName);
    } catch (SecurityException ex) {
      logger.debug("{} does not have method {}. Proceeding to locate another getter method.",
              pojoClass, methodName);
    }

    methodName = IS + upperCaseWord(fieldExpression);
    try {
      Method method = pojoClass.getMethod(methodName);
      if (ClassUtils.isAssignable(method.getReturnType(), castClass)) {
        return code.append(methodName).append("()").getStatement();
      }
      logger.debug("method {} of the {} returns {} that can not be assigned to an {}. Proceeding with the original expression {}.",
              pojoClass, methodName, method.getReturnType(), castClass, fieldExpression);
    } catch (NoSuchMethodException ex) {
      logger.debug("{} does not have method {}. Proceeding with the original expression {}.",
              pojoClass, methodName, fieldExpression);
    } catch (SecurityException ex) {
      logger.debug("{} does not have method {}. Proceeding with the original expression {}.",
              pojoClass, methodName, fieldExpression);
    }

    return code.append(fieldExpression).getStatement();
  }

  @SuppressWarnings("StringEquality")
  private static <T> Object createGetter(Class<?> pojoClass, String getterExpr, String exprObjectPlaceholder, Class<?> castClass, Class<T> getterClass)
  {
    logger.debug("{} {} {} {}", pojoClass, getterExpr, castClass, getterClass);

    if (getterExpr.startsWith(".")) {
      getterExpr = getterExpr.substring(1);
    }

    if (getterExpr.isEmpty()) {
      throw new IllegalArgumentException("The getter expression: \"" + getterExpr + "\" is invalid.");
    }

    IScriptEvaluator se;

    try {
      se = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    String code = StringUtils.replaceEach(getterExpr, new String[]{exprObjectPlaceholder},
            new String[]{new JavaStatement(pojoClass.getName().length() + OBJECT.length() + 4).appendCastToTypeExpr(pojoClass, OBJECT).toString()});
    if (code != getterExpr) {
      code = new JavaReturnStatement(castClass.getName().length() + code.length() + 12, castClass).append(code).getStatement();
      logger.debug("Original expression {} is a complex expression. Replacing it with {}.", getterExpr, code);
    }
    else {
      code = getSingleFieldGetterExpression(pojoClass, getterExpr, castClass);
    }

    logger.debug("code: {}", code);

    try {
      return se.createFastEvaluator(code, getterClass, new String[] {PojoUtils.OBJECT});
    } catch (CompileException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static String getSingleFieldSetterExpression(final Class<?> pojoClass, final String fieldExpression, final Class<?> exprClass)
  {
    JavaStatement code = new JavaStatement(pojoClass.getName().length() + fieldExpression.length() + exprClass.getName().length() + 16);
    /* Construct ((<pojo class name>)pojo). */
    code.appendCastToTypeExpr(pojoClass, OBJECT).append(".");
    try {
      final Field field = pojoClass.getField(fieldExpression);
      if (ClassUtils.isAssignable(exprClass, field.getType())) {
        /* there is public field on the class, use direct assignment. */
        /* append <field name> = (<field type>)val; */
        return code.append(field.getName()).append(" = ").appendCastToTypeExpr(exprClass, VAL).getStatement();
      }
      logger.debug("{} can not be assigned to {}. Proceeding to locate a setter method.", exprClass, field);
    } catch (NoSuchFieldException ex) {
      logger.debug("{} does not have field {}. Proceeding to locate a setter method.", pojoClass, fieldExpression);
    } catch (SecurityException ex) {
      logger.debug("{} does not have field {}. Proceeding to locate a setter method.", pojoClass, fieldExpression);
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
        logger.debug("{} does not have suitable setter method {}. Returning original expression {}.",
                pojoClass, setMethodName, fieldExpression);
        /* We did not find any match at all, use original expression */
        /* append = (<expr type>)val;*/
        return code.append(fieldExpression).append(" = ").appendCastToTypeExpr(exprClass, VAL).getStatement();
      } else {
        // TODO: see if we can find a better match
        bestMatchMethod = candidates.get(0);
      }
    }

    /* We found a method that we may use for setter */
    /* append <method name>((<expr class)val); */
    return code.append(bestMatchMethod.getName()).append("(").appendCastToTypeExpr(exprClass, VAL).append(")").getStatement();
  }

  /**
   *
   * @param pojoClass Class object that the setter applies to
   * @param setterExpr expression to use for setter
   * @param exprClass Class that setter will accept as parameter
   * @param setterClass setter interface to implement
   * @return instance of a class that implements requested Setter interface
   */
  @SuppressWarnings("StringEquality")
  private static <T> Object createSetter(Class<?> pojoClass, String setterExpr, String exprObjectPlaceholder, String exprValPlaceholder, Class<?> exprClass, Class<T> setterClass)
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


    String code = StringUtils.replaceEach(setterExpr, new String[]{exprObjectPlaceholder, exprValPlaceholder},
            new String[]{new JavaStatement().appendCastToTypeExpr(pojoClass, OBJECT).toString(), new JavaStatement().appendCastToTypeExpr(exprClass, VAL).toString()});
    if (code != setterExpr) {
      code = new JavaStatement().append(code).getStatement();
      logger.debug("Original expression {} is a complex expression. Replacing it with {}.", setterExpr, code);
    }
    else {
      code = getSingleFieldSetterExpression(pojoClass, setterExpr, exprClass);
    }

    try {

      logger.debug("code: {}", code);

      return se.createFastEvaluator(code, setterClass, new String[] { PojoUtils.OBJECT, PojoUtils.VAL});
    } catch (CompileException ex) {
      throw new RuntimeException(ex);
    }
  }

}

