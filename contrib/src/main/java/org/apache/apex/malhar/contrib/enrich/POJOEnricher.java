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
package org.apache.apex.malhar.contrib.enrich;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;


/**
 * This class takes a POJO as input and extracts the value of the lookupKey configured
 * for this operator. It perform a lookup using {@link org.apache.apex.malhar.lib.db.cache.CacheManager} to
 * find a matching entry and adds the result to the original tuple.
 *
 * <p>
 * Properties:<br>
 * <b>inputClass</b>: Class to be loaded for the incoming data type<br>
 * <b>outputClass</b>: Class to be loaded for the emitted data type<br>
 * <br>
 * <p>
 *
 * <p>
 * Example:
 * Lets say, input tuple is
 *    { amount=10.0, channelId=4, productId=3 }
 * The tuple is modified as below:
 *    { amount=10.0, channelId=4, productId=3, <b>productCategory=1 </b>}
 * </p>
 *
 * @displayName POJOEnricher
 * @category Database
 * @tags enrichment, enricher, pojo, schema, lookup
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public class POJOEnricher extends AbstractEnricher<Object, Object>
{
  private static final Logger logger = LoggerFactory.getLogger(POJOEnricher.class);

  /**
   * Helper fields
   */
  protected Class<?> inputClass;
  protected Class<?> outputClass;
  private transient Map<PojoUtils.Getter, PojoUtils.Setter> fieldMap = new HashMap<>();
  private transient List<PojoUtils.Setter> includeSetters = new ArrayList<>();
  private transient List<PojoUtils.Getter> lookupGetters = new ArrayList<>();

  /**
   * AutoMetrics
   */
  @AutoMetric
  private int enrichedTupleCount;
  @AutoMetric
  private int errorTupleCount;

  @InputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      inputClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }

    @Override
    public void process(Object object)
    {
      processTuple(object);
    }
  };

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      outputClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }
  };

  public final transient DefaultOutputPort<Object> error = new DefaultOutputPort<>();

  protected void processTuple(Object object)
  {
    enrichTuple(object);
  }

  @Override
  public void beginWindow(long windowId)
  {
    enrichedTupleCount = 0;
    errorTupleCount = 0;
  }

  @Override
  protected Object getKey(Object tuple)
  {
    ArrayList<Object> keyList = new ArrayList<>();
    for (PojoUtils.Getter lookupGetter : lookupGetters) {
      keyList.add(lookupGetter.get(tuple));
    }
    return keyList;
  }

  @Override
  protected Object convert(Object in, Object cached)
  {
    Object o;

    try {
      o = outputClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      logger.error("Failed to create new instance of output POJO", e);
      errorTupleCount++;
      error.emit(in);
      return null;
    }

    try {
      for (Map.Entry<PojoUtils.Getter, PojoUtils.Setter> entry : fieldMap.entrySet()) {
        entry.getValue().set(o, entry.getKey().get(in));
      }
    } catch (RuntimeException e) {
      logger.error("Failed to set the property. Continuing with default.", e);
      errorTupleCount++;
      error.emit(in);
      return null;
    }

    if (cached == null) {
      return o;
    }

    ArrayList<Object> includeObjects = (ArrayList<Object>)cached;
    for (int i = 0; i < includeSetters.size(); i++) {
      try {
        includeSetters.get(i).set(o, includeObjects.get(i));
      } catch (RuntimeException e) {
        logger.error("Failed to set the property. Continuing with default.", e);
        errorTupleCount++;
        error.emit(in);
        return null;
      }
    }

    return o;
  }

  @Override
  protected void emitEnrichedTuple(Object tuple)
  {
    output.emit(tuple);
    enrichedTupleCount++;
  }

  @Override
  protected Class<?> getIncludeFieldType(String fieldName)
  {
    try {
      return outputClass.getDeclaredField(fieldName).getType();
    } catch (NoSuchFieldException e) {
      logger.warn("Failed to find given fieldName, returning object type", e);
      return Object.class;
    }
  }

  @Override
  protected Class<?> getLookupFieldType(String fieldName)
  {
    try {
      return inputClass.getDeclaredField(fieldName).getType();
    } catch (NoSuchFieldException e) {
      logger.warn("Failed to find given fieldName, returning object type", e);
      return Object.class;
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private PojoUtils.Setter generateSettersForField(Class<?> klass, String outputFieldName)
    throws NoSuchFieldException, SecurityException
  {
    Field f = klass.getDeclaredField(outputFieldName);
    Class c = ClassUtils.primitiveToWrapper(f.getType());
    return PojoUtils.createSetter(klass, outputFieldName, c);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private PojoUtils.Getter generateGettersForField(Class<?> klass, String inputFieldName)
    throws NoSuchFieldException, SecurityException
  {
    Field f = klass.getDeclaredField(inputFieldName);
    Class c = ClassUtils.primitiveToWrapper(f.getType());
    return PojoUtils.createGetter(klass, inputFieldName, c);
  }

  @Override
  public void activate(Context context)
  {
    super.activate(context);

    for (Field field : inputClass.getDeclaredFields()) {
      try {
        fieldMap.put(generateGettersForField(inputClass, field.getName()),
            generateSettersForField(outputClass, field.getName()));
      } catch (NoSuchFieldException e) {
        throw new RuntimeException("Unable to find field with name " + field.getName() + ", ignoring that field.", e);
      }
    }

    for (FieldInfo fieldInfo : this.includeFieldInfo) {
      try {
        includeSetters.add(generateSettersForField(outputClass, fieldInfo.getColumnName()));
      } catch (NoSuchFieldException e) {
        throw new RuntimeException("Given field name is not present in output POJO", e);
      }
    }

    for (FieldInfo fieldInfo : this.lookupFieldInfo) {
      try {
        lookupGetters.add(generateGettersForField(inputClass, fieldInfo.getColumnName()));
      } catch (NoSuchFieldException e) {
        throw new RuntimeException("Given lookup field is not present in POJO", e);
      }
    }
  }

  /**
   * Set fields on which lookup against which lookup will be performed.
   * This is a mandatory parameter to set.
   *
   * @param lookupFields List of fields on which lookup happens.
   * @description $[] Field which become part of lookup key
   * @useSchema $[] input.fields[].name
   */
  @Override
  public void setLookupFields(List<String> lookupFields)
  {
    super.setLookupFields(lookupFields);
  }

  /**
   * Set fields which will enrich the POJO.
   * This is a mandatory parameter to set.
   *
   * @param includeFields List of fields.
   * @description $[] Field which are fetched from store
   * @useSchema $[] output.fields[].name
   */
  @Override
  public void setIncludeFields(List<String> includeFields)
  {
    super.setIncludeFields(includeFields);
  }
}
