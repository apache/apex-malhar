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

package com.datatorrent.lib.projection;

import java.lang.reflect.Field;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.ClassUtils;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.common.util.BaseOperator;

import com.datatorrent.lib.util.PojoUtils;

/**
 * <b>ProjectionOperator</b>
 * Projection Operator projects defined set of fields from given selectFields/dropFields
 *
 * <b>Parameters</b>
 * - selectFields: comma separated list of fields to be selected from input tuples
 * - dropFields: comma separated list of fields to be dropped from input tuples
 * selectFields and dropFields are optional and either of them shall be specified
 * When both are not specified, all fields shall be projected to downstream operator
 * When both are specified, selectFields shall be given the preference
 *
 * <b>Input Port</b> takes POJOs as an input
 *
 * <b>Output Ports</b>
 * - projected port emits POJOs with projected fields from input POJOs
 * - remainder port, if connected, emits POJOs with remainder fields from input POJOs
 * - error port emits input POJOs as is upon error situations
 * 
 * <b>Examples</b>
 * For {a, b, c} type of input tuples
 *  - when selectFields = "" and dropFields = "", projected port shall emit {a, b, c}
 *  - when selectFields = "a" and dropFields = "b", projected port shall emit {a}, remainder {b, c}
 *  - when selectFields = "b", projected port shall emit {b} and remainder port shall emit {a, c}
 *  - when dropFields = "b", projected port shall emit {a, c} and remainder port shall emit {b}
 *
 *
 * @since 3.4.0
 */
public class ProjectionOperator extends BaseOperator implements Operator.ActivationListener<Context>
{
  protected String selectFields;
  protected String dropFields;
  protected String condition;

  static class TypeInfo
  {
    String name;
    Class type;
    PojoUtils.Setter setter;
    PojoUtils.Getter getter;

    public TypeInfo(String name, Class<?> type)
    {
      this.name = name;
      this.type = type;
    }

    public String toString()
    {
      String s = new String("'name': " + name + " 'type': " + type);
      return s;
    }
  }

  private transient List<TypeInfo> projectedFields = new ArrayList<>();
  private transient List<TypeInfo> remainderFields = new ArrayList<>();

  @VisibleForTesting
  List<TypeInfo> getProjectedFields()
  {
    return projectedFields;
  }

  @VisibleForTesting
  List<TypeInfo> getRemainderFields()
  {
    return remainderFields;
  }

  @AutoMetric
  protected long projectedTuples;

  @AutoMetric
  protected long remainderTuples;

  @AutoMetric
  protected long errorTuples;

  protected Class<?> inClazz = null;
  protected Class<?> projectedClazz = null;
  protected Class<?> remainderClazz = null;

  @InputPortFieldAnnotation(schemaRequired = true)
  public transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    public void setup(PortContext context)
    {
      inClazz = context.getValue(Context.PortContext.TUPLE_CLASS);
    }

    @Override
    public void process(Object t)
    {
      handleProjection(t);
    }
  };

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> projected = new DefaultOutputPort<Object>()
  {
    public void setup(PortContext context)
    {
      projectedClazz = context.getValue(Context.PortContext.TUPLE_CLASS);
    }
  };

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> remainder = new DefaultOutputPort<Object>()
  {
    public void setup(PortContext context)
    {
      remainderClazz = context.getValue(Context.PortContext.TUPLE_CLASS);
    }
  };


  public final transient DefaultOutputPort<Object> error = new DefaultOutputPort<Object>();

  /**
   * addProjectedField: Add field details (name, type, getter and setter) for field with given name
   * in projectedFields list
   */
  protected void addProjectedField(String s)
  {
    try {
      Field f = inClazz.getDeclaredField(s);
      TypeInfo t = new TypeInfo(f.getName(), ClassUtils.primitiveToWrapper(f.getType()));
      t.getter = PojoUtils.createGetter(inClazz, t.name, t.type);
      t.setter = PojoUtils.createSetter(projectedClazz, t.name, t.type);
      projectedFields.add(t);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("Field " + s + " not found in class " + inClazz, e);
    }
  }

  /**
   * addRemainderField: Add field details (name, type, getter and setter) for field with given name
   * in remainderFields list
   */
  protected void addRemainderField(String s)
  {
    try {
      Field f = inClazz.getDeclaredField(s);
      TypeInfo t = new TypeInfo(f.getName(), ClassUtils.primitiveToWrapper(f.getType()));
      t.getter = PojoUtils.createGetter(inClazz, t.name, t.type);
      t.setter = PojoUtils.createSetter(remainderClazz, t.name, t.type);
      remainderFields.add(t);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("Field " + s + " not found in class " + inClazz, e);
    }
  }

  @Override
  public void activate(Context context)
  {
    final Field[] allFields = inClazz.getDeclaredFields();

    if (selectFields != null && !selectFields.isEmpty()) {
      List<String> sFields = Arrays.asList(selectFields.split(","));
      for (String s : sFields) {
        addProjectedField(s);
      }

      if (remainderClazz != null) {
        for (Field f : allFields) {
          if (!sFields.contains(f.getName())) {
            addRemainderField(f.getName());
          }
        }
      } else {
        logger.info("Remainder Port does not have Schema class defined");
      }
    } else {
      List<String> dFields = new ArrayList<>();
      if (dropFields != null && !dropFields.isEmpty()) {
        dFields = Arrays.asList(dropFields.split(","));
        if (remainderClazz != null) {
          for (String s : dFields) {
            addRemainderField(s);
          }
        } else {
          logger.info("Remainder Port does not have Schema class defined");
        }
      }

      for (Field f : allFields) {
        if (!dFields.contains(f.getName())) {
          addProjectedField(f.getName());
        }
      }
    }

    logger.debug("projected fields: {}", projectedFields);
    logger.debug("remainder fields: {}", remainderFields);
  }

  @Override
  public void deactivate()
  {
    projectedFields.clear();
    remainderFields.clear();
  }

  @Override
  public void beginWindow(long windowId)
  {
    errorTuples = projectedTuples = remainderTuples = 0;
  }

  protected Object getProjectedObject(Object t) throws IllegalAccessException
  {
    try {
      Object p = projectedClazz.newInstance();
      for (TypeInfo ti: projectedFields) {
        ti.setter.set(p, ti.getter.get(t));
      }
      return p;
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw e;
    }
  }

  protected Object getRemainderObject(Object t) throws IllegalAccessException
  {
    try {
      Object r = remainderClazz.newInstance();
      for (TypeInfo ti: remainderFields) {
        ti.setter.set(r, ti.getter.get(t));
      }
      return r;
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw e;
    }
  }

  /**
   * handleProjection: emit projected object on projected port
   * and remainder object on remainder port if that is connected.
   */
  private void handleProjection(Object t)
  {
    try {
      Object p = getProjectedObject(t);

      if (remainder.isConnected()) {
        Object r = getRemainderObject(t);
        remainder.emit(r);
        remainderTuples++;
      }

      projected.emit(p);
      projectedTuples++;
    } catch (IllegalAccessException e) {
      error.emit(t);
      errorTuples++;
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(ProjectionOperator.class);
}
