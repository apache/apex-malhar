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
package com.datatorrent.contrib.formatter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.FmtDate;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.contrib.parser.DelimitedSchema;
import com.datatorrent.contrib.parser.DelimitedSchema.Field;
import com.datatorrent.contrib.parser.DelimitedSchema.FieldType;
import com.datatorrent.lib.formatter.Formatter;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Operator that converts POJO to CSV string <br>
 * Assumption is that each field in the delimited data should map to a simple
 * java type.<br>
 * <br>
 * <b>Properties</b> <br>
 * <b>schema</b>:schema as a string specified in a json format as per
 * {@link DelimitedSchema}. Currently only Date constraint (fmt) is supported
 * <br>
 * <b>clazz</b>:Pojo class <br>
 * <b>Ports</b> <br>
 * <b>in</b>:input tuple as a POJO. Each tuple represents a record<br>
 * <b>out</b>:tuples are are converted to string are emitted on this port<br>
 * <b>err</b>:tuples that could not be converted are emitted on this port<br>
 * 
 * @displayName CsvFormatter
 * @category Formatter
 * @tags pojo csv formatter
 * @since 3.2.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class CsvFormatter extends Formatter<String>
{

  /**
   * Names of all the fields in the same order that would appear in output
   * records
   */
  private transient String[] nameMapping;
  /**
   * Cell processors are an integral part of reading and writing with Super CSV
   * they automate the data type conversions, and enforce constraints.
   */
  private transient CellProcessor[] processors;
  /**
   * Writing preferences that are passed through schema
   */
  private transient CsvPreference preference;

  /**
   * Contents of the schema.Schema is specified in a json format as per
   * {@link DelimitedSchema}
   */
  @NotNull
  private String schema;
  /**
   * Schema is read into this object to access fields
   */
  private transient DelimitedSchema delimitedParserSchema;

  /**
   * metric to keep count of number of tuples emitted on error port port
   */
  @AutoMetric
  private long errorTupleCount;

  /**
   * metric to keep count of number of tuples emitted on out port
   */
  @AutoMetric
  private long emittedObjectCount;

  /**
   * metric to keep count of number of tuples emitted on input port
   */
  @AutoMetric
  private long incomingTuplesCount;

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    errorTupleCount = 0;
    emittedObjectCount = 0;
    incomingTuplesCount = 0;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    delimitedParserSchema = new DelimitedSchema(schema);
    preference = new CsvPreference.Builder(delimitedParserSchema.getQuoteChar(),
        delimitedParserSchema.getDelimiterChar(), delimitedParserSchema.getLineDelimiter()).build();
    nameMapping = delimitedParserSchema.getFieldNames()
        .toArray(new String[delimitedParserSchema.getFieldNames().size()]);
    processors = getProcessor(delimitedParserSchema.getFields());
  }

  /**
   * Returns array of cellprocessors, one for each field
   */
  private CellProcessor[] getProcessor(List<Field> fields)
  {
    CellProcessor[] processor = new CellProcessor[fields.size()];
    int fieldCount = 0;
    for (Field field : fields) {
      if (field.getType() == FieldType.DATE) {
        String format = field.getConstraints().get(DelimitedSchema.DATE_FORMAT) == null ? null
            : (String)field.getConstraints().get(DelimitedSchema.DATE_FORMAT);
        processor[fieldCount++] = new Optional(new FmtDate(format == null ? "dd/MM/yyyy" : format));
      } else {
        processor[fieldCount++] = new Optional();
      }
    }
    return processor;
  }

  @Override
  public String convert(Object tuple)
  {
    incomingTuplesCount++;
    if (tuple == null) {
      errorTupleCount++;
      logger.error(" Null tuple", tuple);
      return null;
    }
    try {
      StringWriter stringWriter = new StringWriter();
      ICsvBeanWriter beanWriter = new CsvBeanWriter(stringWriter, preference);
      beanWriter.write(tuple, nameMapping, processors);
      beanWriter.flush();
      beanWriter.close();
      emittedObjectCount++;
      return stringWriter.toString();
    } catch (SuperCsvException e) {
      logger.error("Error while converting tuple {} {}", tuple, e.getMessage());
      errorTupleCount++;
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
    return null;
  }

  /**
   * Get the schema
   * 
   * @return schema
   */
  public String getSchema()
  {
    return schema;
  }

  /**
   * Set the schema
   * 
   * @param schema
   */
  public void setSchema(String schema)
  {
    this.schema = schema;
  }

  @VisibleForTesting
  protected long getErrorTupleCount()
  {
    return errorTupleCount;
  }

  @VisibleForTesting
  protected long getEmittedObjectCount()
  {
    return emittedObjectCount;
  }

  @VisibleForTesting
  protected long getIncomingTuplesCount()
  {
    return incomingTuplesCount;
  }

  private static final Logger logger = LoggerFactory.getLogger(CsvFormatter.class);
}
