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
package org.apache.apex.malhar.contrib.parser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.CsvMapReader;
import org.supercsv.prefs.CsvPreference;

import org.apache.apex.malhar.contrib.parser.DelimitedSchema.Field;
import org.apache.apex.malhar.lib.parser.Parser;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.util.ReusableStringReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Operator that parses a delimited tuple against a specified schema <br>
 * Schema is specified in a json format as per {@link DelimitedSchema} that
 * contains field information and constraints for each field.<br>
 * Assumption is that each field in the delimited data should map to a simple
 * java type.<br>
 * <br>
 * <b>Properties</b> <br>
 * <b>schema</b>:schema as a string<br>
 * <b>clazz</b>:Pojo class <br>
 * <b>Ports</b> <br>
 * <b>in</b>:input tuple as a byte array. Each tuple represents a record<br>
 * <b>parsedOutput</b>:tuples that are validated against the schema are emitted
 * as Map<String,Object> on this port<br>
 * <b>out</b>:tuples that are validated against the schema are emitted as pojo
 * on this port<br>
 * <b>err</b>:tuples that do not confine to schema are emitted on this port as
 * KeyValPair<String,String><br>
 * Key being the tuple and Val being the reason.
 *
 * @displayName CsvParser
 * @category Parsers
 * @tags csv pojo parser
 * @since 3.2.0
 */
@InterfaceStability.Evolving
public class CsvParser extends Parser<byte[], KeyValPair<String, String>>
{
  /**
   * Map Reader to read delimited records
   */
  private transient CsvMapReader csvMapReader;
  /**
   * Bean Reader to read delimited records
   */
  private transient CsvBeanReader csvBeanReader;
  /**
   * Reader used by csvMapReader and csvBeanReader
   */
  private transient ReusableStringReader csvStringReader;
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
   * Cell processors are an integral part of reading and writing with Super CSV
   * they automate the data type conversions, and enforce constraints.
   */
  private transient CellProcessor[] processors;
  /**
   * Names of all the fields in the same order of incoming records
   */
  private transient String[] nameMapping;
  /**
   * header-this will be delimiter separated string of field names
   */
  private transient String header;
  /**
   * Reading preferences that are passed through schema
   */
  private transient CsvPreference preference;

  /**
   * metric to keep count of number of tuples emitted on {@link #parsedOutput}
   * port
   */
  @AutoMetric
  long parsedOutputCount;

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    parsedOutputCount = 0;
  }

  @Override
  public void setup(OperatorContext context)
  {
    delimitedParserSchema = new DelimitedSchema(schema);
    preference = new CsvPreference.Builder(delimitedParserSchema.getQuoteChar(),
        delimitedParserSchema.getDelimiterChar(), delimitedParserSchema.getLineDelimiter()).build();
    nameMapping = delimitedParserSchema.getFieldNames().toArray(
        new String[delimitedParserSchema.getFieldNames().size()]);
    header = StringUtils.join(nameMapping, (char)delimitedParserSchema.getDelimiterChar() + "");
    processors = getProcessor(delimitedParserSchema.getFields());
    csvStringReader = new ReusableStringReader();
    csvMapReader = new CsvMapReader(csvStringReader, preference);
    csvBeanReader = new CsvBeanReader(csvStringReader, preference);
  }

  @Override
  public Object convert(byte[] tuple)
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void processTuple(byte[] tuple)
  {
    if (tuple == null) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(null, "Blank/null tuple"));
      }
      errorTupleCount++;
      return;
    }
    String incomingString = new String(tuple);
    if (StringUtils.isBlank(incomingString) || StringUtils.equals(incomingString, header)) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(incomingString, "Blank/header tuple"));
      }
      errorTupleCount++;
      return;
    }
    try {
      if (parsedOutput.isConnected()) {
        csvStringReader.open(incomingString);
        Map<String, Object> map = csvMapReader.read(nameMapping, processors);
        parsedOutput.emit(map);
        parsedOutputCount++;
      }

      if (out.isConnected() && clazz != null) {
        csvStringReader.open(incomingString);
        Object obj = csvBeanReader.read(clazz, nameMapping, processors);
        out.emit(obj);
        emittedObjectCount++;
      }

    } catch (SuperCsvException | IOException | IllegalArgumentException e) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(incomingString, e.getMessage()));
      }
      errorTupleCount++;
      logger.error("Tuple could not be parsed. Reason {}", e.getMessage());
    }
  }

  @Override
  public KeyValPair<String, String> processErrorTuple(byte[] input)
  {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * Returns array of cellprocessors, one for each field
   */
  private CellProcessor[] getProcessor(List<Field> fields)
  {
    CellProcessor[] processor = new CellProcessor[fields.size()];
    int fieldCount = 0;
    for (Field field : fields) {
      processor[fieldCount++] = CellProcessorBuilder.getCellProcessor(field.getType(), field.getConstraints());
    }
    return processor;
  }

  @Override
  public void teardown()
  {
    try {
      csvMapReader.close();
    } catch (IOException e) {
      logger.error("Error while closing csv map reader {}", e.getMessage());
      DTThrowable.wrapIfChecked(e);
    }
    try {
      csvBeanReader.close();
    } catch (IOException e) {
      logger.error("Error while closing csv bean reader {}", e.getMessage());
      DTThrowable.wrapIfChecked(e);
    }
  }

  /**
   * Get the schema
   *
   * @return
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

  /**
   * Get errorTupleCount
   *
   * @return errorTupleCount
   */
  @VisibleForTesting
  public long getErrorTupleCount()
  {
    return errorTupleCount;
  }

  /**
   * Get emittedObjectCount
   *
   * @return emittedObjectCount
   */
  @VisibleForTesting
  public long getEmittedObjectCount()
  {
    return emittedObjectCount;
  }

  /**
   * Get incomingTuplesCount
   *
   * @return incomingTuplesCount
   */
  @VisibleForTesting
  public long getIncomingTuplesCount()
  {
    return incomingTuplesCount;
  }

  /**
   * output port to emit validate records as map
   */
  public final transient DefaultOutputPort<Map<String, Object>> parsedOutput = new DefaultOutputPort<Map<String, Object>>();
  private static final Logger logger = LoggerFactory.getLogger(CsvParser.class);

}
