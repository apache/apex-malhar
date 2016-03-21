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
package com.datatorrent.contrib.parquet;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.ClassUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.FieldInfo.SupportType;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Setter;

import parquet.example.data.Group;
import parquet.io.InvalidRecordException;
import parquet.io.ParquetEncodingException;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * <p>
 * ParquetFilePOJOReader
 * </p>
 * ParquetFilePOJOReader operator is a concrete implementation of
 * AbstractParquetFileReader to read Parquet files and emit records as POJOs.The
 * POJO class name & field mapping should be provided by the user. If this
 * mapping is not provided then reflection is used to determine this mapping. As
 * of now only primitive types ( INT32, INT64, BOOLEAN, FLOAT, DOUBLE, BINARY )
 * are supported.
 *
 * Parquet primitive types maps to POJO datatypes as follows : INT32 -> int
 * INT64 -> long BOOLEAN -> boolean FLOAT -> float DOUBLE -> double BINARY ->
 * String
 *
 * @displayName ParquetFilePOJOReader
 * @tags parquet,input adapter
 * @since 3.3.0
 */
public class ParquetFilePOJOReader extends AbstractParquetFileReader<Object>
{

  /**
   * POJO class
   */
  protected transient Class<?> pojoClass;
  /**
   * Map containing setters for fields in POJO
   */
  protected transient Map<String, Setter> pojoSetters;
  /**
   * String representing Parquet TO POJO field mapping. If not provided, then
   * reflection is used to determine the mapping. Format :
   * PARQUET_FIELD_NAME:POJO_FIELD_NAME:TYPE
   * E.g.event_id:event_id_v2:INTEGER,org_id:org_id_v2:STRING,long_id:
   * long_id_v2:
   * LONG,css_file_loaded:css_file_loaded_v2:BOOLEAN,float_val:float_val_v2:
   * FLOAT,double_val:double_val_v2:DOUBLE
   */
  protected transient String groupToPOJOFieldsMapping = null;
  protected transient List<FieldInfo> fieldInfos;
  protected transient List<ActiveFieldInfo> columnFieldSetters = null;
  protected static final String FIELD_SEPARATOR = ":";
  protected static final String RECORD_SEPARATOR = ",";
  private static final Logger logger = LoggerFactory.getLogger(ParquetFilePOJOReader.class);

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>()
  {

    @Override
    public void setup(Context.PortContext context)
    {
      pojoClass = context.getValue(Context.PortContext.TUPLE_CLASS);
      pojoSetters = Maps.newHashMap();
      for (Field f : pojoClass.getDeclaredFields()) {
        try {
          pojoSetters.put(f.getName(), generateSettersForField(pojoClass, f.getName()));
        } catch (NoSuchFieldException | SecurityException e) {
          logger.error("Failed to generate setters. Exception {}", e);
          throw new RuntimeException(e);
        }
      }

      if (groupToPOJOFieldsMapping == null) {
        fieldInfos = createFieldInfoMap(generateFieldInfoInputs());
      } else {
        fieldInfos = createFieldInfoMap(groupToPOJOFieldsMapping);
      }
      initColumnFieldSetters();
    }

  };

  /**
   * Converts Group to POJO
   */
  @Override
  protected Object convertGroup(Group group)
  {
    Object obj;
    try {
      obj = pojoClass.newInstance();
    } catch (InstantiationException | IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }
    for (int i = 0; i < columnFieldSetters.size(); i++) {
      try {
        ParquetFilePOJOReader.ActiveFieldInfo afi = columnFieldSetters.get(i);
        int fieldIndex = schema.getFieldIndex(afi.fieldInfo.getColumnName());
        PrimitiveTypeName primitiveTypeName = schema.getType(fieldIndex).asPrimitiveType().getPrimitiveTypeName();

        switch (primitiveTypeName) {

          case BOOLEAN:
            Boolean booleanVal = Boolean.parseBoolean(group.getValueToString(fieldIndex, 0));
            pojoSetters.get(afi.fieldInfo.getPojoFieldExpression()).set(obj, booleanVal);
            break;

          case INT32:
            Integer intVal = Integer.parseInt(group.getValueToString(fieldIndex, 0));
            pojoSetters.get(afi.fieldInfo.getPojoFieldExpression()).set(obj, intVal);
            break;

          case INT64:
            Long longVal = Long.parseLong(group.getValueToString(fieldIndex, 0));
            pojoSetters.get(afi.fieldInfo.getPojoFieldExpression()).set(obj, longVal);
            break;

          case FLOAT:
            Float floatVal = Float.parseFloat(group.getValueToString(fieldIndex, 0));
            pojoSetters.get(afi.fieldInfo.getPojoFieldExpression()).set(obj, floatVal);
            break;

          case DOUBLE:
            Double doubleVal = Double.parseDouble(group.getValueToString(fieldIndex, 0));
            pojoSetters.get(afi.fieldInfo.getPojoFieldExpression()).set(obj, doubleVal);
            break;

          case BINARY:
            pojoSetters.get(afi.fieldInfo.getPojoFieldExpression()).set(obj, group.getValueToString(fieldIndex, 0));
            break;

          default:
            throw new ParquetEncodingException("Unsupported column type: " + primitiveTypeName);

        }
      } catch (InvalidRecordException e) {
        logger.error("Field not found in schema {} ", e);
      }
    }
    return obj;
  }

  @Override
  protected void emit(Object tuple)
  {
    output.emit(tuple);
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
  public void setFieldInfos(List<FieldInfo> fieldInfos)
  {
    this.fieldInfos = fieldInfos;
  }

  /**
   * Creates a map representing fieldName in POJO:field in Generic Record:Data
   * type
   * 
   * @return List of FieldInfo
   */
  public List<FieldInfo> createFieldInfoMap(String str)
  {
    fieldInfos = new ArrayList<FieldInfo>();

    StringTokenizer strtok = new StringTokenizer(str, RECORD_SEPARATOR);

    while (strtok.hasMoreTokens()) {
      String[] token = strtok.nextToken().split(FIELD_SEPARATOR);

      fieldInfos.add(new FieldInfo(token[0], token[1], SupportType.valueOf(token[2])));
    }

    return fieldInfos;
  }

  /**
   * Generates setter for given field and POJO class
   * 
   * @param klass
   * @param inputFieldName
   * @return Setter
   * @throws NoSuchFieldException
   * @throws SecurityException
   */
  private Setter generateSettersForField(Class<?> klass, String inputFieldName)
      throws NoSuchFieldException, SecurityException
  {
    Field f = klass.getDeclaredField(inputFieldName);
    Class c = ClassUtils.primitiveToWrapper(f.getType());
    Setter classSetter = PojoUtils.createSetter(klass, inputFieldName, c);
    return classSetter;
  }

  /**
   * Returns groupToPOJOFieldsMapping
   * 
   * @return groupToPOJOFieldsMapping
   */
  public String getGroupToPOJOFieldsMapping()
  {
    return groupToPOJOFieldsMapping;
  }

  /**
   * Sets groupToPOJOFieldsMapping
   * 
   * @param groupToPOJOFieldsMapping
   */
  public void setGroupToPOJOFieldsMapping(String groupToPOJOFieldsMapping)
  {
    this.groupToPOJOFieldsMapping = groupToPOJOFieldsMapping;
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
  public List<FieldInfo> getFieldInfos()
  {
    return fieldInfos;
  }

  /**
   * Add the Active Fields to the columnFieldSetters {@link ActiveFieldInfo}s
   */
  private void initColumnFieldSetters()
  {
    for (FieldInfo fi : fieldInfos) {
      if (columnFieldSetters == null) {
        columnFieldSetters = Lists.newArrayList();
      }
      columnFieldSetters.add(new ParquetFilePOJOReader.ActiveFieldInfo(fi));
    }
  }

  /**
   * Use reflection to generate field info values if the user has not provided
   * the inputs mapping.
   * 
   * @return String representing the POJO field to Parquet field mapping
   */
  private String generateFieldInfoInputs()
  {
    java.lang.reflect.Field[] fields = pojoClass.getDeclaredFields();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fields.length; i++) {
      java.lang.reflect.Field f = fields[i];
      Class<?> c = ClassUtils.primitiveToWrapper(f.getType());
      sb.append(f.getName() + FIELD_SEPARATOR + f.getName() + FIELD_SEPARATOR + c.getSimpleName().toUpperCase()
          + RECORD_SEPARATOR);
    }
    return sb.substring(0, sb.length() - 1);
  }

}
