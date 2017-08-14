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
package org.apache.apex.malhar.contrib.avro;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.apex.malhar.lib.util.FieldInfo.SupportType;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * <p>
 * AvroToPojo
 * </p>
 * A generic implementation for conversion from Avro to POJO. The POJO class
 * name & field mapping should be provided by the user.<br>
 * If this mapping is not provided then reflection is used to determine this
 * mapping.<br>
 * As of now only primitive types are supported.<br>
 * Error records are emitted on the errorPort if connected
 *
 * @displayName Avro To Pojo
 * @category Converter
 * @tags avro
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public class AvroToPojo extends BaseOperator
{

  private transient Class<?> pojoClass;

  private static final String FIELD_SEPARATOR = ":";
  private static final String RECORD_SEPARATOR = ",";

  private String genericRecordToPOJOFieldsMapping = null;

  private transient List<FieldInfo> fieldInfos;

  private transient List<ActiveFieldInfo> columnFieldSetters;

  @AutoMetric
  @VisibleForTesting
  int recordCount = 0;

  @AutoMetric
  @VisibleForTesting
  int errorCount = 0;

  @AutoMetric
  @VisibleForTesting
  int fieldErrorCount = 0;

  public final transient DefaultOutputPort<GenericRecord> errorPort = new DefaultOutputPort<GenericRecord>();

  /**
   * Returns a string representing mapping from generic record to POJO fields
   */
  public String getGenericRecordToPOJOFieldsMapping()
  {
    return genericRecordToPOJOFieldsMapping;
  }

  /**
   * Comma separated list mapping a field in Avro schema to POJO field eg :
   * orderId:orderId:INTEGER,total:total:DOUBLE
   */
  public void setGenericRecordToPOJOFieldsMapping(String genericRecordToPOJOFieldsMapping)
  {
    this.genericRecordToPOJOFieldsMapping = genericRecordToPOJOFieldsMapping;
  }

  public final transient DefaultInputPort<GenericRecord> data = new DefaultInputPort<GenericRecord>()
  {
    @Override
    public void process(GenericRecord tuple)
    {
      processTuple(tuple);
    }
  };

  /**
   * Converts given Generic Record and to a POJO and emits it
   */
  protected void processTuple(GenericRecord tuple)
  {
    try {
      Object obj = getPOJOFromGenericRecord(tuple);

      if (obj != null) {
        output.emit(obj);
        recordCount++;
      } else if (errorPort.isConnected()) {
        errorPort.emit(tuple);
        errorCount++;
      }

    } catch (InstantiationException | IllegalAccessException e) {
      LOG.error("Could not initialize object of class -" + getClass().getName(), e);
      errorCount++;
    }
  }

  /**
   * Returns a POJO from a Generic Record
   *
   * @return Object
   */
  @SuppressWarnings("unchecked")
  private Object getPOJOFromGenericRecord(GenericRecord tuple) throws InstantiationException, IllegalAccessException
  {
    Object newObj = getPojoClass().newInstance();

    try {
      for (int i = 0; i < columnFieldSetters.size(); i++) {

        AvroToPojo.ActiveFieldInfo afi = columnFieldSetters.get(i);
        SupportType st = afi.fieldInfo.getType();
        Object val = null;

        try {
          val = tuple.get(afi.fieldInfo.getColumnName());
        } catch (Exception e) {
          LOG.error("Could not find field -" + afi.fieldInfo.getColumnName() + "- in the generic record", e);
          val = null;
          fieldErrorCount++;
        }

        if (val == null) {
          continue;
        }

        try {
          switch (st) {
            case BOOLEAN:
              ((PojoUtils.SetterBoolean<Object>)afi.setterOrGetter).set(newObj,
                  (boolean)tuple.get(afi.fieldInfo.getColumnName()));
              break;

            case DOUBLE:
              ((PojoUtils.SetterDouble<Object>)afi.setterOrGetter).set(newObj,
                  (double)tuple.get(afi.fieldInfo.getColumnName()));
              break;

            case FLOAT:
              ((PojoUtils.SetterFloat<Object>)afi.setterOrGetter).set(newObj,
                  (float)tuple.get(afi.fieldInfo.getColumnName()));
              break;

            case INTEGER:
              ((PojoUtils.SetterInt<Object>)afi.setterOrGetter).set(newObj,
                  (int)tuple.get(afi.fieldInfo.getColumnName()));
              break;

            case STRING:
              ((PojoUtils.Setter<Object, String>)afi.setterOrGetter).set(newObj,
                  new String(tuple.get(afi.fieldInfo.getColumnName()).toString()));
              break;

            case LONG:
              ((PojoUtils.SetterLong<Object>)afi.setterOrGetter).set(newObj,
                  (long)tuple.get(afi.fieldInfo.getColumnName()));
              break;

            default:
              throw new AvroRuntimeException("Invalid Support Type");

          }
        } catch (AvroRuntimeException e) {
          LOG.error("Exception in setting value", e);
          fieldErrorCount++;
        }

      }
    } catch (Exception ex) {
      LOG.error("Generic Exception in setting value" + ex.getMessage());
      errorCount++;
      newObj = null;
    }
    return newObj;
  }

  /**
   * Use reflection to generate field info values if the user has not provided
   * the inputs mapping
   *
   * @return String representing the POJO field to Avro field mapping
   */
  private String generateFieldInfoInputs(Class<?> cls)
  {
    java.lang.reflect.Field[] fields = cls.getDeclaredFields();
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < fields.length; i++) {
      java.lang.reflect.Field f = fields[i];
      Class<?> c = ClassUtils.primitiveToWrapper(f.getType());
      sb.append(f.getName()).append(FIELD_SEPARATOR).append(f.getName()).append(FIELD_SEPARATOR)
          .append(c.getSimpleName().toUpperCase()).append(RECORD_SEPARATOR);
    }
    return sb.substring(0, sb.length() - 1);
  }

  /**
   * Creates a map representing fieldName in POJO:field in Generic Record:Data
   * type
   *
   * @return List of FieldInfo
   */
  private List<FieldInfo> createFieldInfoMap(String str)
  {
    fieldInfos = new ArrayList<FieldInfo>();
    StringTokenizer strtok = new StringTokenizer(str, RECORD_SEPARATOR);

    while (strtok.hasMoreTokens()) {
      String[] token = strtok.nextToken().split(FIELD_SEPARATOR);
      try {
        fieldInfos.add(new FieldInfo(token[0], token[1], SupportType.valueOf(token[2])));
      } catch (Exception e) {
        LOG.error("Invalid support type", e);
      }
    }
    return fieldInfos;
  }

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>()
  {
    public void setup(PortContext context)
    {
      setPojoClass(context.getValue(Context.PortContext.TUPLE_CLASS));

      columnFieldSetters = Lists.newArrayList();

      /**
       * Check if the mapping of Generic record fields to POJO is given, else
       * use reflection
       */
      if (getGenericRecordToPOJOFieldsMapping() == null) {
        setFieldInfos(createFieldInfoMap(generateFieldInfoInputs(getPojoClass())));
      } else {
        setFieldInfos(createFieldInfoMap(getGenericRecordToPOJOFieldsMapping()));
      }

      initColumnFieldSetters(getFieldInfos());
      initializeActiveFieldSetters();
    }
  };

  @Override
  public void endWindow()
  {
    errorCount = 0;
    fieldErrorCount = 0;
    recordCount = 0;

  }

  private Class<?> getPojoClass()
  {
    return pojoClass;
  }

  public void setPojoClass(Class<?> pojoClass)
  {
    this.pojoClass = pojoClass;
  }

  /**
   * Class that maps fieldInfo to its getters or setters
   */
  protected static class ActiveFieldInfo
  {
    final FieldInfo fieldInfo;
    Object setterOrGetter;

    ActiveFieldInfo(FieldInfo fieldInfo)
    {
      this.fieldInfo = fieldInfo;
    }

  }

  /**
   * A list of {@link FieldInfo}s where each item maps a column name to a pojo
   * field name.
   */
  private List<FieldInfo> getFieldInfos()
  {
    return fieldInfos;
  }

  /**
   * Add the Active Fields to the columnFieldSetters {@link ActiveFieldInfo}s
   */
  private void initColumnFieldSetters(List<FieldInfo> fieldInfos)
  {
    for (FieldInfo fi : fieldInfos) {
      if (columnFieldSetters == null) {
        columnFieldSetters = Lists.newArrayList();
      }
      columnFieldSetters.add(new AvroToPojo.ActiveFieldInfo(fi));
    }
  }

  /**
   * Sets the {@link FieldInfo}s. A {@link FieldInfo} maps a store column to a
   * pojo field name.<br/>
   * The value from fieldInfo.column is assigned to
   * fieldInfo.pojoFieldExpression.
   *
   * @description $[].columnName name of the Output Field in POJO
   * @description $[].pojoFieldExpression expression to get the respective field
   *              from generic record
   * @useSchema $[].pojoFieldExpression outputPort.fields[].name
   */
  private void setFieldInfos(List<FieldInfo> fieldInfos)
  {
    this.fieldInfos = fieldInfos;
  }

  /**
   * Initialize the setters for generating the POJO
   */
  private void initializeActiveFieldSetters()
  {
    for (int i = 0; i < columnFieldSetters.size(); i++) {
      ActiveFieldInfo activeFieldInfo = columnFieldSetters.get(i);

      SupportType st = activeFieldInfo.fieldInfo.getType();

      switch (st) {

        case BOOLEAN:

          activeFieldInfo.setterOrGetter = PojoUtils.createSetterBoolean(getPojoClass(),
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case DOUBLE:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetterDouble(getPojoClass(),
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case FLOAT:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetterFloat(getPojoClass(),
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case INTEGER:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetterInt(getPojoClass(),
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case STRING:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetter(getPojoClass(),
              activeFieldInfo.fieldInfo.getPojoFieldExpression(), activeFieldInfo.fieldInfo.getType().getJavaType());
          break;

        case LONG:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetterLong(getPojoClass(),
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        default:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetter(getPojoClass(),
              activeFieldInfo.fieldInfo.getPojoFieldExpression(), Byte.class);
          break;
      }

      columnFieldSetters.get(i).setterOrGetter = activeFieldInfo.setterOrGetter;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(AvroToPojo.class);

}
