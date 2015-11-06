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
package com.datatorrent.contrib.schema.parser;

import java.io.IOException;
import java.util.ArrayList;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseBool;
import org.supercsv.cellprocessor.ParseChar;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.prefs.CsvPreference;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.util.ReusableStringReader;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Operator that converts CSV string to Pojo <br>
 * Assumption is that each field in the delimited data should map to a simple
 * java type.<br>
 * <br>
 * <b>Properties</b> <br>
 * <b>fieldInfo</b>:User need to specify fields and their types as a comma
 * separated string having format &lt;NAME&gt;:&lt;TYPE&gt;|&lt;FORMAT&gt; in
 * the same order as incoming data. FORMAT refers to dates with dd/mm/yyyy as
 * default e.g name:string,dept:string,eid:integer,dateOfJoining:date|dd/mm/yyyy <br>
 * <b>fieldDelimiter</b>: Default is comma <br>
 * <b>lineDelimiter</b>: Default is '\r\n'
 * 
 * @displayName CsvParser
 * @category Parsers
 * @tags csv pojo parser
 * @since 3.2.0
 */
@InterfaceStability.Evolving
public class CsvParser extends Parser<String>
{

  private ArrayList<Field> fields;
  @NotNull
  protected int fieldDelimiter;
  protected String lineDelimiter;

  @NotNull
  protected String fieldInfo;

  protected transient String[] nameMapping;
  protected transient CellProcessor[] processors;
  private transient CsvBeanReader csvReader;

  public enum FIELD_TYPE
  {
    BOOLEAN, DOUBLE, INTEGER, FLOAT, LONG, SHORT, CHARACTER, STRING, DATE
  };

  @NotNull
  private transient ReusableStringReader csvStringReader = new ReusableStringReader();

  public CsvParser()
  {
    fields = new ArrayList<Field>();
    fieldDelimiter = ',';
    lineDelimiter = "\r\n";
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    logger.info("field info {}", fieldInfo);
    fields = new ArrayList<Field>();
    String[] fieldInfoTuple = fieldInfo.split(",");
    for (int i = 0; i < fieldInfoTuple.length; i++) {
      String[] fieldTuple = fieldInfoTuple[i].split(":");
      Field field = new Field();
      field.setName(fieldTuple[0]);
      String[] typeFormat = fieldTuple[1].split("\\|");
      field.setType(typeFormat[0].toUpperCase());
      if (typeFormat.length > 1) {
        field.setFormat(typeFormat[1]);
      }
      getFields().add(field);
    }

    CsvPreference preference = new CsvPreference.Builder('"', fieldDelimiter, lineDelimiter).build();
    csvReader = new CsvBeanReader(csvStringReader, preference);
    int countKeyValue = getFields().size();
    logger.info("countKeyValue {}", countKeyValue);
    nameMapping = new String[countKeyValue];
    processors = new CellProcessor[countKeyValue];
    initialise(nameMapping, processors);
  }

  private void initialise(String[] nameMapping, CellProcessor[] processors)
  {
    for (int i = 0; i < getFields().size(); i++) {
      FIELD_TYPE type = getFields().get(i).type;
      nameMapping[i] = getFields().get(i).name;
      if (type == FIELD_TYPE.DOUBLE) {
        processors[i] = new Optional(new ParseDouble());
      } else if (type == FIELD_TYPE.INTEGER) {
        processors[i] = new Optional(new ParseInt());
      } else if (type == FIELD_TYPE.FLOAT) {
        processors[i] = new Optional(new ParseDouble());
      } else if (type == FIELD_TYPE.LONG) {
        processors[i] = new Optional(new ParseLong());
      } else if (type == FIELD_TYPE.SHORT) {
        processors[i] = new Optional(new ParseInt());
      } else if (type == FIELD_TYPE.STRING) {
        processors[i] = new Optional();
      } else if (type == FIELD_TYPE.CHARACTER) {
        processors[i] = new Optional(new ParseChar());
      } else if (type == FIELD_TYPE.BOOLEAN) {
        processors[i] = new Optional(new ParseBool());
      } else if (type == FIELD_TYPE.DATE) {
        String dateFormat = getFields().get(i).format;
        processors[i] = new Optional(new ParseDate(dateFormat == null ? "dd/MM/yyyy" : dateFormat));
      }
    }
  }

  @Override
  public void activate(Context context)
  {

  }

  @Override
  public void deactivate()
  {

  }

  @Override
  public Object convert(String tuple)
  {
    try {
      csvStringReader.open(tuple);
      return csvReader.read(clazz, nameMapping, processors);
    } catch (IOException e) {
      logger.debug("Error while converting tuple {} {}",tuple,e.getMessage());
      return null;
    }
  }

  @Override
  public void teardown()
  {
    try {
      if (csvReader != null) {
        csvReader.close();
      }
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  public static class Field
  {
    String name;
    String format;
    FIELD_TYPE type;

    public String getName()
    {
      return name;
    }

    public void setName(String name)
    {
      this.name = name;
    }

    public FIELD_TYPE getType()
    {
      return type;
    }

    public void setType(String type)
    {
      this.type = FIELD_TYPE.valueOf(type);
    }

    public String getFormat()
    {
      return format;
    }

    public void setFormat(String format)
    {
      this.format = format;
    }

  }

  /**
   * Gets the array list of the fields, a field being a POJO containing the name
   * of the field and type of field.
   * 
   * @return An array list of Fields.
   */
  public ArrayList<Field> getFields()
  {
    return fields;
  }

  /**
   * Sets the array list of the fields, a field being a POJO containing the name
   * of the field and type of field.
   * 
   * @param fields
   *          An array list of Fields.
   */
  public void setFields(ArrayList<Field> fields)
  {
    this.fields = fields;
  }

  /**
   * Gets the delimiter which separates fields in incoming data.
   * 
   * @return fieldDelimiter
   */
  public int getFieldDelimiter()
  {
    return fieldDelimiter;
  }

  /**
   * Sets the delimiter which separates fields in incoming data.
   * 
   * @param fieldDelimiter
   */
  public void setFieldDelimiter(int fieldDelimiter)
  {
    this.fieldDelimiter = fieldDelimiter;
  }

  /**
   * Gets the delimiter which separates lines in incoming data.
   * 
   * @return lineDelimiter
   */
  public String getLineDelimiter()
  {
    return lineDelimiter;
  }

  /**
   * Sets the delimiter which separates line in incoming data.
   * 
   * @param lineDelimiter
   */
  public void setLineDelimiter(String lineDelimiter)
  {
    this.lineDelimiter = lineDelimiter;
  }

  /**
   * Gets the name of the fields with type and format ( for date ) as comma
   * separated string in same order as incoming data. e.g
   * name:string,dept:string,eid:integer,dateOfJoining:date|dd/mm/yyyy
   * 
   * @return fieldInfo
   */
  public String getFieldInfo()
  {
    return fieldInfo;
  }

  /**
   * Sets the name of the fields with type and format ( for date ) as comma
   * separated string in same order as incoming data. e.g
   * name:string,dept:string,eid:integer,dateOfJoining:date|dd/mm/yyyy
   * 
   * @param fieldInfo
   */
  public void setFieldInfo(String fieldInfo)
  {
    this.fieldInfo = fieldInfo;
  }

  private static final Logger logger = LoggerFactory.getLogger(CsvParser.class);

}
