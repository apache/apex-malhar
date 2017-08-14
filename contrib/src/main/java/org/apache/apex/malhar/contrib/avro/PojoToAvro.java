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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.util.PojoUtils.Getter;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * <p>
 * PojoToAvro
 * </p>
 * A generic implementation for POJO to Avro conversion. A POJO is converted to
 * a GenericRecord based on the schema provided.<br>
 * As of now only primitive types are supported.<br>
 * Error records are emitted on the errorPort if connected
 *
 * @displayName Pojo To Avro
 * @category Converter
 * @tags avro
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public class PojoToAvro extends BaseOperator
{
  private List<Field> columnNames;

  private Class<?> cls;

  private List<Getter> keyMethodMap;

  private transient String schemaString;

  private transient Schema schema;

  @AutoMetric
  @VisibleForTesting
  int recordCount = 0;

  @AutoMetric
  @VisibleForTesting
  int errorCount = 0;

  @AutoMetric
  @VisibleForTesting
  int fieldErrorCount = 0;

  public final transient DefaultOutputPort<GenericRecord> output = new DefaultOutputPort<>();

  public final transient DefaultOutputPort<Object> errorPort = new DefaultOutputPort<>();

  private void parseSchema() throws IOException
  {
    setSchema(new Schema.Parser().parse(getSchemaString()));
  }

  /**
   * Returns the schema string for Avro Generic Record
   *
   * @return schemaString
   */
  public String getSchemaString()
  {
    return schemaString;
  }

  /**
   * Sets the schema string
   */
  public void setSchemaString(String schemaString)
  {
    this.schemaString = schemaString;
  }

  /**
   * Returns the schema object
   *
   * @return schema
   */
  private Schema getSchema()
  {
    return schema;
  }

  /**
   * Sets the shcema object
   */
  private void setSchema(Schema schema)
  {
    this.schema = schema;
  }

  /**
   * Returns the list for field names from provided Avro schema
   *
   * @return List of Fields
   */
  private List<Field> getColumnNames()
  {
    return columnNames;
  }

  /**
   * Sets the list of column names representing the fields in Avro schema
   */
  private void setColumnNames(List<Field> columnNames)
  {
    this.columnNames = columnNames;
  }

  /**
   * This method generates the getters for provided field of a given class
   *
   * @return Getter
   */
  private Getter<?, ?> generateGettersForField(Class<?> cls, String inputFieldName)
    throws NoSuchFieldException, SecurityException
  {
    java.lang.reflect.Field f = cls.getDeclaredField(inputFieldName);
    Class<?> c = ClassUtils.primitiveToWrapper(f.getType());

    Getter<?, ?> classGetter = PojoUtils.createGetter(cls, inputFieldName, c);

    return classGetter;
  }

  /**
   * Initializes the list of columns in POJO based on the names from schema
   */
  private void initializeColumnMap(Schema schema)
  {
    setColumnNames(schema.getFields());

    keyMethodMap = new ArrayList<>();
    for (int i = 0; i < getColumnNames().size(); i++) {
      try {
        keyMethodMap.add(generateGettersForField(cls, getColumnNames().get(i).name()));
      } catch (NoSuchFieldException | SecurityException e) {
        throw new RuntimeException("Failed to initialize pojo class getters for field - ", e);
      }
    }
  }

  @InputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultInputPort<Object> data = new DefaultInputPort<Object>()
  {

    @Override
    public void setup(PortContext context)
    {
      cls = context.getValue(Context.PortContext.TUPLE_CLASS);

      try {
        parseSchema();
        initializeColumnMap(getSchema());
      } catch (IOException e) {
        LOG.error("Exception in parsing schema", e);
      }
    }

    @Override
    public void process(Object tuple)
    {
      processTuple(tuple);
    }

  };

  /**
   * Converts incoming tuples into Generic records
   */
  protected void processTuple(Object tuple)
  {
    GenericRecord record = null;

    try {
      record = getGenericRecord(tuple);
    } catch (Exception e) {
      LOG.error("Exception in creating record");
      errorCount++;
    }

    if (record != null) {
      output.emit(record);
      recordCount++;
    } else if (errorPort.isConnected()) {
      errorPort.emit(tuple);
      errorCount++;
    }
  }

  /**
   * Returns a generic record mapping the POJO fields to provided schema
   *
   * @return Generic Record
   */
  private GenericRecord getGenericRecord(Object tuple) throws Exception
  {
    int writeErrorCount = 0;
    GenericRecord rec = new GenericData.Record(getSchema());

    for (int i = 0; i < columnNames.size(); i++) {
      try {
        rec.put(columnNames.get(i).name(), AvroRecordHelper.convertValueStringToAvroKeyType(getSchema(),
            columnNames.get(i).name(), keyMethodMap.get(i).get(tuple).toString()));
      } catch (AvroRuntimeException e) {
        LOG.error("Could not set Field [" + columnNames.get(i).name() + "] in the generic record", e);
        fieldErrorCount++;
      } catch (Exception e) {
        LOG.error("Parse Exception", e);
        fieldErrorCount++;
        writeErrorCount++;
      }
    }

    if (columnNames.size() == writeErrorCount) {
      errorCount++;
      return null;
    } else {
      return rec;
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    recordCount = 0;
    errorCount = 0;
    fieldErrorCount = 0;
  }

  private static final Logger LOG = LoggerFactory.getLogger(PojoToAvro.class);

}
