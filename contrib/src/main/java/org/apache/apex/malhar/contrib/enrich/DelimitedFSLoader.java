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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapReader;
import org.supercsv.prefs.CsvPreference;

import org.apache.apex.malhar.contrib.parser.CellProcessorBuilder;
import org.apache.apex.malhar.contrib.parser.DelimitedSchema;
import org.apache.apex.malhar.contrib.parser.DelimitedSchema.Field;
import org.apache.apex.malhar.lib.util.ReusableStringReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This implementation of {@link FSLoader} is used to load data from delimited
 * file.User needs to provide a schema as a string specified in a json format as
 * per {@link DelimitedSchema} that contains information of name and type of
 * field
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class DelimitedFSLoader extends FSLoader
{

  /**
   * Map Reader to read delimited records
   */
  private transient CsvMapReader csvMapReader;
  /**
   * Reader used by csvMapReader
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

  private boolean initialized;

  private static final Logger logger = LoggerFactory.getLogger(DelimitedFSLoader.class);

  public DelimitedFSLoader()
  {
  }

  /**
   * Extracts the fields from a delimited record and returns a map containing
   * field names and values
   */
  @Override
  Map<String, Object> extractFields(String line)
  {
    if (!initialized) {
      init();
      initialized = true;
    }
    if (StringUtils.isBlank(line) || StringUtils.equals(line, header)) {
      return null;
    }
    try {
      csvStringReader.open(line);
      return csvMapReader.read(nameMapping, processors);
    } catch (IOException e) {
      logger.error("Error parsing line{} Exception {}", line, e.getMessage());
      return null;
    }
  }

  private void init()
  {

    delimitedParserSchema = new DelimitedSchema(schema);
    preference = new CsvPreference.Builder(delimitedParserSchema.getQuoteChar(),
        delimitedParserSchema.getDelimiterChar(), delimitedParserSchema.getLineDelimiter()).build();
    nameMapping = delimitedParserSchema.getFieldNames()
        .toArray(new String[delimitedParserSchema.getFieldNames().size()]);
    header = StringUtils.join(nameMapping, (char)delimitedParserSchema.getDelimiterChar() + "");
    processors = getProcessor(delimitedParserSchema.getFields());
    csvStringReader = new ReusableStringReader();
    csvMapReader = new CsvMapReader(csvStringReader, preference);
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

}
