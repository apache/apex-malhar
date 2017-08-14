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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseChar;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.ICsvReader;
import org.supercsv.prefs.CsvPreference;

import org.apache.apex.malhar.lib.util.ReusableStringReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;

/**
 *  This is a base implementation of Delimited data parser which can be extended to output
 *  field values in desired data structure.
 *  Assumption is that each field in the delimited data should map to a simple java type.
 *  Delimited records can be supplied in file or message based sources like kafka.
 *  User can specify the name of the field , data type of the field as list of key value pairs or in a hdfs file
 *  and delimiter as properties on the parsing operator.
 *  Other properties to be specified
 *        - Input Stream encoding - default value should be UTF-8
 *        - End of line character - default should be ‘\r\n’
 *
 * @param <T> This is the output tuple type.
 *
 * @since 2.1.0
 */
public abstract class AbstractCsvParser<T> extends BaseOperator
{
  // List of key value pairs which has name of the field as key , data type of the field as value.
  private ArrayList<Field> fields;

  protected String inputEncoding;
  @NotNull
  protected int fieldDelimiter;
  protected String lineDelimiter;
  //User gets an option to specify filename containing name of the field and data type of the field.
  protected String fieldmappingFile;
  //Field and its data type can be separated by a user defined delimiter in the file.
  protected String fieldmappingFileDelimiter;

  protected transient String[] properties;
  protected transient CellProcessor[] processors;
  protected boolean hasHeader;

  private transient ICsvReader csvReader;

  public enum FIELD_TYPE
  {
    BOOLEAN, DOUBLE, INTEGER, FLOAT, LONG, SHORT, CHARACTER, STRING, DATE
  }

  @NotNull
  private transient ReusableStringReader csvStringReader = new ReusableStringReader();

  public AbstractCsvParser()
  {
    fields = new ArrayList<Field>();
    fieldDelimiter = ',';
    fieldmappingFileDelimiter = ":";
    inputEncoding = "UTF8";
    lineDelimiter = "\r\n";
    hasHeader = false;
  }

  /**
   * Output port that emits value of the fields.
   * Output data type can be configured in the implementation of this operator.
   */
  public final transient DefaultOutputPort<T> output = new DefaultOutputPort<T>();

  /**
   * This input port receives byte array as tuple.
   */
  public final transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] tuple)
    {
      try {
        csvStringReader.open(new String(tuple, inputEncoding));
        if (hasHeader) {
          String[] header = csvReader.getHeader(true);
          int len = header.length;
          for (int i = 0; i < len; i++) {
            logger.debug("header is {}", header[i]);
            @SuppressWarnings("unchecked")
            T headerData = (T)header[i];
            output.emit(headerData);
          }
        }

        while (true) {
          T data = readData(properties, processors);
          if (data == null) {
            break;
          }
          logger.debug("data in loop is {}", data.toString());
          output.emit(data);
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }

    }

  };

  @Override
  public void setup(OperatorContext context)
  {
    if (fieldmappingFile != null) {
      Configuration conf = new Configuration();
      try {
        FileSystem fs = FileSystem.get(conf);
        Path filepath = new Path(fieldmappingFile);
        if (fs.exists(filepath)) {
          BufferedReader bfr = new BufferedReader(new InputStreamReader(fs.open(filepath)));
          String str;

          while ((str = bfr.readLine()) != null) {
            logger.debug("string is {}", str);
            String[] temp = str.split(fieldmappingFileDelimiter);
            Field field = new Field();
            field.setName(temp[0]);
            field.setType(temp[1]);
            getFields().add(field);
          }
        } else {
          logger.debug("File containing fields and their data types does not exist.Please specify the fields and data type through properties of this operator.");
        }
      } catch (IOException ex) {
        DTThrowable.rethrow(ex);
      }

    }

    int countKeyValue = getFields().size();
    properties = new String[countKeyValue];
    processors = new CellProcessor[countKeyValue];
    initialise(properties, processors);
    CsvPreference preference = new CsvPreference.Builder('"', fieldDelimiter, lineDelimiter).build();
    csvReader = getReader(csvStringReader, preference);

  }

  // Initialise the properties and processors.
  public void initialise(String[] properties, CellProcessor[] processors)
  {
    for (int i = 0; i < getFields().size(); i++) {
      FIELD_TYPE type = getFields().get(i).type;
      properties[i] = getFields().get(i).name;
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
        processors[i] = new Optional(new ParseChar());
      } else if (type == FIELD_TYPE.DATE) {
        processors[i] = new Optional(new ParseDate("dd/MM/yyyy"));
      }
    }

  }

  @Override
  public void teardown()
  {
    try {
      csvReader.close();
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  /**
   * Any concrete class derived from AbstractParser has to implement this method.
   * It returns an instance of specific CsvReader required to read field values into a specific data type.
   *
   * @param reader
   * @param preference
   * @return CsvReader
   */
  protected abstract ICsvReader getReader(ReusableStringReader reader, CsvPreference preference);

  /**
   * Any concrete class derived from AbstractParser has to implement this method.
   * It returns the specific data structure in which field values are being read to.
   *
   * @param properties
   * @param processors
   * @return Specific data structure in which field values are read to.
   */
  protected abstract T readData(String[] properties, CellProcessor[] processors);

  public static class Field
  {
    String name;
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
   * Sets the delimiter which separates lines in incoming data.
   *
   * @param lineDelimiter
   */
  public void setLineDelimiter(String lineDelimiter)
  {
    this.lineDelimiter = lineDelimiter;
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
   * Gets the option if incoming data has header or not.
   *
   * @return hasHeader
   */
  public boolean isHasHeader()
  {
    return hasHeader;
  }

  /**
   * Sets the option if incoming data has header or not.
   *
   * @param hasHeader
   */
  public void setHasHeader(boolean hasHeader)
  {
    this.hasHeader = hasHeader;
  }

  /**
   * Gets the arraylist of the fields, a field being a POJO containing
   * the name of the field and type of field.
   *
   * @return An arraylist of Fields.
   */
  public ArrayList<Field> getFields()
  {
    return fields;
  }

  /**
   * Sets the arraylist of the fields, a field being a POJO containing
   * the name of the field and type of field.
   *
   * @param fields An arraylist of Fields.
   */
  public void setFields(ArrayList<Field> fields)
  {
    this.fields = fields;
  }

  /**
   * Gets the path of the file which contains mapping of field names to data type.
   *
   * @return Path
   */
  public String getFieldmappingFile()
  {
    return fieldmappingFile;
  }

  /**
   * Sets the path of the file which contains mapping of field names to data type.
   *
   * @param fieldmappingFile The path where fieldmappingFile is created.
   */
  public void setFieldmappingFile(String fieldmappingFile)
  {
    this.fieldmappingFile = fieldmappingFile;
  }

  /**
   * Gets the delimiter which separates field name and data type in input file.
   *
   * @return fieldmappingFileDelimiter
   */
  public String getFieldmappingFileDelimiter()
  {
    return fieldmappingFileDelimiter;
  }

  /**
   * Sets the delimiter which separates field name and data type in input file.
   *
   * @param fieldmappingFileDelimiter
   */
  public void setFieldmappingFileDelimiter(String fieldmappingFileDelimiter)
  {
    this.fieldmappingFileDelimiter = fieldmappingFileDelimiter;
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractCsvParser.class);

}
