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
import java.util.ArrayList;

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

import com.datatorrent.api.Context;
import com.datatorrent.lib.formatter.Formatter;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Operator that converts POJO to CSV string <br>
 * Assumption is that each field in the delimited data should map to a simple
 * java type.<br>
 * <br>
 * <b>Properties</b> <br>
 * <b>fieldInfo</b>:User need to specify fields and their types as a comma
 * separated string having format &lt;NAME&gt;:&lt;TYPE&gt;|&lt;FORMAT&gt; in
 * the same order as incoming data. FORMAT refers to dates with dd/mm/yyyy as
 * default e.g name:string,dept:string,eid:integer,dateOfJoining:date|dd/mm/yyyy
 * 
 * @displayName CsvFormatter
 * @category Formatter
 * @tags pojo csv formatter
 * @since 3.2.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class CsvFormatter extends Formatter<String>
{

  private ArrayList<Field> fields;
  @NotNull
  protected String classname;
  @NotNull
  protected int fieldDelimiter;
  protected String lineDelimiter;

  @NotNull
  protected String fieldInfo;

  public enum FIELD_TYPE
  {
    BOOLEAN, DOUBLE, INTEGER, FLOAT, LONG, SHORT, CHARACTER, STRING, DATE
  };

  protected transient String[] nameMapping;
  protected transient CellProcessor[] processors;
  protected transient CsvPreference preference;

  public CsvFormatter()
  {
    fields = new ArrayList<Field>();
    fieldDelimiter = ',';
    lineDelimiter = "\r\n";

  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);

    //fieldInfo information
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
    preference = new CsvPreference.Builder('"', fieldDelimiter, lineDelimiter).build();
    int countKeyValue = getFields().size();
    nameMapping = new String[countKeyValue];
    processors = new CellProcessor[countKeyValue];
    initialise(nameMapping, processors);

  }

  private void initialise(String[] nameMapping, CellProcessor[] processors)
  {
    for (int i = 0; i < getFields().size(); i++) {
      FIELD_TYPE type = getFields().get(i).type;
      nameMapping[i] = getFields().get(i).name;
      if (type == FIELD_TYPE.DATE) {
        String dateFormat = getFields().get(i).format;
        processors[i] = new Optional(new FmtDate(dateFormat == null ? "dd/MM/yyyy" : dateFormat));
      } else {
        processors[i] = new Optional();
      }
    }

  }

  @Override
  public String convert(Object tuple)
  {
    try {
      StringWriter stringWriter = new StringWriter();
      ICsvBeanWriter beanWriter = new CsvBeanWriter(stringWriter, preference);
      beanWriter.write(tuple, nameMapping, processors);
      beanWriter.flush();
      beanWriter.close();
      return stringWriter.toString();
    } catch (SuperCsvException e) {
      logger.debug("Error while converting tuple {} {}",tuple,e.getMessage());
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
    return null;
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
   * Gets the name of the fields with type and format in data as comma separated
   * string in same order as incoming data. e.g
   * name:string,dept:string,eid:integer,dateOfJoining:date|dd/mm/yyyy
   * 
   * @return fieldInfo
   */
  public String getFieldInfo()
  {
    return fieldInfo;
  }

  /**
   * Sets the name of the fields with type and format in data as comma separated
   * string in same order as incoming data. e.g
   * name:string,dept:string,eid:integer,dateOfJoining:date|dd/mm/yyyy
   * 
   * @param fieldInfo
   */
  public void setFieldInfo(String fieldInfo)
  {
    this.fieldInfo = fieldInfo;
  }

  private static final Logger logger = LoggerFactory.getLogger(CsvFormatter.class);
}
