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
package com.datatorrent.lib.transform;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.ClassUtils;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.expression.Expression;
import com.datatorrent.lib.util.PojoUtils;

/**
 * This operator can transform given POJO using provided expressions and
 * return a final POJO as a return of transformation process.
 *
 * Following are the mandatory fields that needs to be set for TransformOperator to work:
 * <ul>
 *   <li><b>expressionMap</b> : Set how the transformation should happen</li>
 *   <li><b>inputPort.attr.TUPLE_CLASS</b>: Set class type at input port</li>
 *   <li><b>outputPort.attr.TUPLE_CLASS</b> : Set class type at output port</li>
 * </ul>
 *
 * The operator uses interaction via {@link Expression} and {@link PojoUtils} to transform given POJO.
 */
public class TransformOperator extends BaseOperator implements Operator.ActivationListener
{
  @NotNull
  private Map<String, String> expressionMap = new HashMap<>();
  private List<String> expressionFuntions = new LinkedList<>();
  private boolean copyMatchingFields = true;

  private transient Map<PojoUtils.Setter, Expression> transformationMap = new HashMap<>();
  private Class<?> inputClass;
  private Class<?> outputClass;

  public TransformOperator()
  {
    expressionFuntions.add("java.lang.Math.*");
    expressionFuntions.add("org.apache.commons.lang3.StringUtils.*");
    expressionFuntions.add("org.apache.commons.lang3.StringEscapeUtils.*");
    expressionFuntions.add("org.apache.commons.lang3.time.DurationFormatUtils.*");
    expressionFuntions.add("org.apache.commons.lang3.time.DateFormatUtils.*");
  }

  @InputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      inputClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }

    @Override
    public void process(Object o)
    {
      processTuple(o);
    }
  };

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>()
  {
    @Override public void setup(Context.PortContext context)
    {
      outputClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }
  };

  protected void processTuple(Object in)
  {
    if (!inputClass.isAssignableFrom(in.getClass())) {
      throw new RuntimeException(
          "Unexpected tuple received. Received class: " + in.getClass() + ". Expected class: " + inputClass.getClass());
    }

    Object out;
    try {
      out = outputClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Failed to create new object", e);
    }

    for (Map.Entry<PojoUtils.Setter, Expression> entry : transformationMap.entrySet()) {
      PojoUtils.Setter set = entry.getKey();
      Expression expr = entry.getValue();
      set.set(out, expr.execute(in));
    }

    output.emit(out);
  }

  @Override
  public void activate(Context context)
  {
    if (copyMatchingFields) {
      Field[] declaredFields = outputClass.getDeclaredFields();
      for (Field outputField : declaredFields) {
        String outputFieldName = outputField.getName();
        if (!expressionMap.containsKey(outputFieldName)) {
          try {
            Field inputField = inputClass.getDeclaredField(outputFieldName);
            if (inputField.getType() == outputField.getType()) {
              expressionMap.put(outputFieldName, inputField.getName());
            }
          } catch (NoSuchFieldException e) {
            continue;
          }
        }
      }
    }

    for (Map.Entry<String, String> entry : expressionMap.entrySet()) {
      String field = entry.getKey();
      String expr = entry.getValue();

      // Generate output setter
      Field f;
      try {
        f = outputClass.getDeclaredField(field);
      } catch (NoSuchFieldException e) {
        throw new RuntimeException("Failed to get output field info", e);
      }

      Class c = ClassUtils.primitiveToWrapper(f.getType());
      PojoUtils.Setter setter = PojoUtils.createSetter(outputClass, field, c);

      // Generate evaluated expression

      Expression expression = PojoUtils
          .createExpression(inputClass, expr, c, expressionFuntions.toArray(new String[expressionFuntions.size()]));

      transformationMap.put(setter, expression);
    }
  }

  @Override
  public void deactivate()
  {
  }

  /**
   * Returns expression map which defines outputFieldName => Expression mapping.
   *
   * @return Map of outputFieldName => Expression
   */
  public Map<String, String> getExpressionMap()
  {
    return expressionMap;
  }

  /**
   * Set expression map (outputFieldName => Expression) which defines how output POJO should be generated.
   * This is a mandatory property.
   * @param expressionMap Map of String => String defining expression for output field.
   *
   * @description $(key) Output field for which expression should be evaluated
   * @description $(value) Expression to be evaluated for output field.
   * @useSchema $(key) input.fields[].name
   */
  public void setExpressionMap(Map<String, String> expressionMap)
  {
    this.expressionMap = expressionMap;
  }

  /**
   * Returns the list of expression function which would be made available to expression to use.
   *
   * @return List of function that are available in expression.
   */
  public List<String> getExpressionFuntions()
  {
    return expressionFuntions;
  }

  /**
   * Set list of import classes/method should should be made statically available to expression to use.
   * For ex. org.apache.apex.test1.Test would mean that "Test" method will be available in the expression to be
   * used directly.
   * This is an optional property. See constructor to see defaults that are included.
   *
   * @param expressionFuntions List of qualified class/method that needs to be imported to expression.
   */
  public void setExpressionFuntions(List<String> expressionFuntions)
  {
    this.expressionFuntions = expressionFuntions;
  }

  /**
   * Returns expression to be evaluated for given output field.
   * This method is meant to be used for properties.xml file where map should be set using BeanUtils constructs.
   *
   * @param field Field for which expression needs to be evaluated.
   * @return Expression that will be evaluated for given field.
   * @omitFromUI
   */
  public String getExpressionMap(String field)
  {
    return (this.expressionMap != null) ? this.expressionMap.get(field) : null;
  }

  /**
   * Set expression for given field.
   * This is a mandatory property.
   * This method is meant to be used for properties.xml file where map should be set using BeanUtils constructs.
   *
   * @param field Output field for which expression should be set.
   * @param expression Expression that should be evaluated for given output field.
   * @omitFromUI
   */
  public void setExpressionMap(String field, String expression)
  {
    if (this.expressionMap == null) {
      this.expressionMap = new HashMap<>();
    }
    this.expressionMap.put(field, expression);
  }

  /**
   * Return expression function for given index.
   * This method is meant to be used for setting indexed property using BeanUtils constructs.
   *
   * @param index Index at which expression function should be returned.
   * @return Qualified Expression function at given index.
   * @omitFromUI
   */
  public String getExpressionFunctions(int index)
  {
    return (index < this.expressionFuntions.size()) ? this.expressionFuntions.get(index) : null;
  }

  /**
   * Sets expression function are given index.
   * This method is meant to be used for setting indexed property using BeanUtils constructs.
   *
   * @param index Index at which expression should be set.
   * @param expressionFunction Qualified expression function that should be made available to expression.
   * @omitFromUI
   */
  public void setExpressionFunctions(int index, String expressionFunction)
  {
    for (int i = this.expressionFuntions.size(); i <= index; i++) {
      this.expressionFuntions.add(null);
    }
    this.expressionFuntions.set(index, expressionFunction);
  }

  /**
   * Tells whether the matching (by name and by type) fields between input and output should be copied as is.
   *
   * @return Tells whether the matching (by name and by type) fields between input and output should be copied as is.
   */
  public boolean isCopyMatchingFields()
  {
    return copyMatchingFields;
  }

  /**
   * Set whether the matching (by name and by type) fields between input and output should be copied as is.
   * This is an optional property, default is true.
   *
   * @param copyMatchingFields true/false
   */
  public void setCopyMatchingFields(boolean copyMatchingFields)
  {
    this.copyMatchingFields = copyMatchingFields;
  }
}
