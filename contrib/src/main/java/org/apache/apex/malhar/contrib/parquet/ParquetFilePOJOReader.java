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
package org.apache.apex.malhar.contrib.parquet;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.FieldInfo.SupportType;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

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
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public class ParquetFilePOJOReader extends AbstractParquetFileReader<Object>
{

  /**
   * POJO class
   */
  protected transient Class<?> pojoClass;
  /**
   * String representing Parquet field name TO POJO field name mapping. If not
   * provided, then reflection is used to determine the mapping. Format :
   * PARQUET_FIELD_NAME:POJO_FIELD_NAME:TYPE
   * E.g.event_id:event_id_v2:INTEGER,org_id:org_id_v2:STRING,long_id:
   * long_id_v2:
   * LONG,css_file_loaded:css_file_loaded_v2:BOOLEAN,float_val:float_val_v2:
   * FLOAT,double_val:double_val_v2:DOUBLE
   */
  protected transient String parquetToPOJOFieldsMapping = null;
  protected transient List<ActiveFieldInfo> activeFieldInfos = null;
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
      if (parquetToPOJOFieldsMapping == null) {
        initialiseActiveFieldInfo(generateFieldInfoInputs());
      } else {
        initialiseActiveFieldInfo(parquetToPOJOFieldsMapping);
      }
    }
  };

  /**
   * Converts a Parquet <b>Group</b>(parquet.example.data.Group) to a POJO.
   * Supported parquet primitive types are BOOLEAN, INT32, INT64, FLOAT, DOUBLE
   * and BINARY
   *
   * @throws ParquetEncodingException
   *           if group contains unsupported type
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

    for (int i = 0; i < activeFieldInfos.size(); i++) {
      try {
        ParquetFilePOJOReader.ActiveFieldInfo afi = activeFieldInfos.get(i);

        switch (afi.primitiveTypeName) {

          case BOOLEAN:
            Boolean booleanVal = Boolean.parseBoolean(group.getValueToString(afi.fieldIndex, 0));
            ((PojoUtils.SetterBoolean<Object>)afi.setter).set(obj, booleanVal);
            break;

          case INT32:
            Integer intVal = Integer.parseInt(group.getValueToString(afi.fieldIndex, 0));
            ((PojoUtils.SetterInt<Object>)afi.setter).set(obj, intVal);
            break;

          case INT64:
            Long longVal = Long.parseLong(group.getValueToString(afi.fieldIndex, 0));
            ((PojoUtils.SetterLong<Object>)afi.setter).set(obj, longVal);
            break;

          case FLOAT:
            Float floatVal = Float.parseFloat(group.getValueToString(afi.fieldIndex, 0));
            ((PojoUtils.SetterFloat<Object>)afi.setter).set(obj, floatVal);
            break;

          case DOUBLE:
            Double doubleVal = Double.parseDouble(group.getValueToString(afi.fieldIndex, 0));
            ((PojoUtils.SetterDouble<Object>)afi.setter).set(obj, doubleVal);
            break;

          case BINARY:
            ((PojoUtils.Setter<Object, String>)afi.setter).set(obj, group.getValueToString(afi.fieldIndex, 0));
            break;

          default:
            throw new ParquetEncodingException("Unsupported column type: " + afi.primitiveTypeName);

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
   * Initializes {@link #activeFieldInfos} by adding fields represented by
   * fieldMapping
   *
   * @param fieldMapping
   *          String representing Parquet field name TO POJO field field name
   *          mapping
   */
  private void initialiseActiveFieldInfo(String fieldMapping)
  {
    String[] fields = fieldMapping.split(RECORD_SEPARATOR);
    activeFieldInfos = new ArrayList<>(fields.length);
    for (String field : fields) {
      String[] token = field.split(FIELD_SEPARATOR);
      try {
        int fieldIndex = schema.getFieldIndex(token[0]);
        PrimitiveTypeName primitiveTypeName = schema.getType(fieldIndex).asPrimitiveType().getPrimitiveTypeName();
        activeFieldInfos.add(
            new ActiveFieldInfo(getSetter(token[1], SupportType.valueOf(token[2])), primitiveTypeName, fieldIndex));
      } catch (InvalidRecordException e) {
        logger.error("{} not present in schema ", token[0]);
      } catch (UnsupportedOperationException e) {
        logger.error("{} not yet supported ", e.getMessage());
      }
    }
  }

  private Object getSetter(String pojoFieldExpression, SupportType supportType) throws UnsupportedOperationException
  {
    switch (supportType) {
      case BOOLEAN:
        return PojoUtils.createSetterBoolean(pojoClass, pojoFieldExpression);
      case DOUBLE:
        return PojoUtils.createSetterDouble(pojoClass, pojoFieldExpression);
      case FLOAT:
        return PojoUtils.createSetterFloat(pojoClass, pojoFieldExpression);
      case INTEGER:
        return PojoUtils.createSetterInt(pojoClass, pojoFieldExpression);
      case LONG:
        return PojoUtils.createSetterLong(pojoClass, pojoFieldExpression);
      case STRING:
        return PojoUtils.createSetter(pojoClass, pojoFieldExpression, String.class);
      default:
        throw new UnsupportedOperationException("Unsupported data type" + supportType);
    }
  }

  /**
   * Returns String containing Parquet field name to POJO field name mapping
   *
   * @return parquetToPOJOFieldsMapping String representing Parquet field name
   *         TO POJO field name mapping
   */
  public String getParquetToPOJOFieldsMapping()
  {
    return parquetToPOJOFieldsMapping;
  }

  /**
   * Sets parquetToPOJOFieldsMapping, string representing Parquet field name TO
   * POJO field name mapping.<br>
   * <b>Format :</b> PARQUET_FIELD_NAME:POJO_FIELD_NAME:TYPE <br>
   * <b>E.g:</b>event_id:event_id_v2:INTEGER,org_id:org_id_v2:STRING,long_id:
   * long_id_v2:
   * LONG,css_file_loaded:css_file_loaded_v2:BOOLEAN,float_val:float_val_v2:
   * FLOAT,double_val:double_val_v2:DOUBLE
   *
   * @param parquetToPOJOFieldsMapping
   *          String representing Parquet field name TO POJO field name mapping
   */
  public void setParquetToPOJOFieldsMapping(String parquetToPOJOFieldsMapping)
  {
    this.parquetToPOJOFieldsMapping = parquetToPOJOFieldsMapping;
  }

  /**
   * Class representing Parquet field information. Each object contains setter
   * for the field, index of the field in Parquet schema and primitive type of
   * the field
   */
  protected static class ActiveFieldInfo
  {
    Object setter;
    PrimitiveTypeName primitiveTypeName;
    int fieldIndex;

    ActiveFieldInfo(Object setter, PrimitiveTypeName primitiveTypeName, int fieldIndex)
    {
      this.setter = setter;
      this.primitiveTypeName = primitiveTypeName;
      this.fieldIndex = fieldIndex;
    }
  }

  /**
   * Use reflection to generate field info values if the user has not provided
   * the inputs mapping.
   *
   * @return String representing the Parquet field name to POJO field name
   *         mapping
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
