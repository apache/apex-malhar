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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.contrib.parser.AbstractCsvParser.FIELD_TYPE;
import org.apache.apex.malhar.contrib.parser.AbstractCsvParser.Field;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.univocity.parsers.fixed.FixedWidthFields;
import com.univocity.parsers.fixed.FixedWidthParser;
import com.univocity.parsers.fixed.FixedWidthParserSettings;

/**
 * This implementation of {@link FSLoader} is used to load data from fixed width
 * file.User needs to set {@link FixedWidthFSLoader#fieldDescription} to specify
 * field information.
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public class FixedWidthFSLoader extends FSLoader
{

  private transient List<FixedWidthField> fields;
  /**
   * Indicates whether first line of the file is a header. Default is false
   */
  private boolean hasHeader;

  /**
   * Specifies information related to fields in fixed-width file. Format is
   * [NAME]:[FIELD_TYPE]:[WIDTH]:[date format if FIELD_TYPE is DATE] FIELD_TYPE
   * can take on of the values of {@link FIELD_TYPE} i.e BOOLEAN, DOUBLE,
   * INTEGER, FLOAT, LONG, SHORT, CHARACTER, STRING, DATE e.g.
   * Year:INTEGER:4,Make:STRING:5,Model:STRING:40,Description:STRING:40,
   * Price:DOUBLE:8,Date:DATE:10:\"dd:mm:yyyy\". Date format needs to be within
   * quotes (" ")
   */
  @NotNull
  private String fieldDescription;

  /**
   * Array containing headers
   */
  private transient String[] header;
  /**
   * Padding character. Default is white space.
   */
  private char padding = ' ';
  private transient FixedWidthParser fixedWidthParser;
  private transient boolean initialized;

  private static final Logger logger = LoggerFactory.getLogger(FixedWidthFSLoader.class);

  /**
   * Gets the option if file has header or not.
   *
   * @return hasHeader,indicating whether first line of the file is a header.
   */
  public boolean isHasHeader()
  {
    return hasHeader;
  }

  /**
   * Set to true if file has header
   *
   * @param hasHeader
   *          Indicates whether first line of the file is a header. Default is
   *          false
   */
  public void setHasHeader(boolean hasHeader)
  {
    this.hasHeader = hasHeader;
  }

  /**
   * Gets the field description
   *
   * @return fieldDescription. String specifying information related to fields
   *         in fixed-width file.
   */
  public String getFieldDescription()
  {
    return fieldDescription;
  }

  /**
   * Sets fieldDescription
   *
   * @param fieldDescription
   *          a String specifying information related to fields in fixed-width
   *          file. Format is [NAME]:[FIELD_TYPE]:[WIDTH]:[date format if
   *          FIELD_TYPE is DATE] FIELD_TYPE can take on of the values of
   *          {@link FIELD_TYPE}
   *          e.g.Year:INTEGER:4,Make:STRING:5,Model:STRING:40,Description:
   *          STRING:40, Price:DOUBLE:8,Date:DATE:10:\"dd:mm:yyyy\" Date format
   *          needs to be within quotes (" ")
   */
  public void setFieldDescription(String fieldDescription)
  {
    this.fieldDescription = fieldDescription;
  }

  /**
   * Gets the character used for padding in the fixed-width file.Default is
   * white space (' ')
   *
   * @return Padding character. Default is white space.
   */
  public char getPadding()
  {
    return padding;
  }

  /**
   * Sets the character used for padding in fixed-width file.Default is white
   * space (' ')
   *
   * @param padding
   *          Padding character. Default is white space.
   */
  public void setPadding(char padding)
  {
    this.padding = padding;
  }

  public static class FixedWidthField extends Field
  {
    int width;
    String dateFormat;

    public int getWidth()
    {
      return width;
    }

    public void setWidth(int width)
    {
      this.width = width;
    }

    public String getDateFormat()
    {
      return dateFormat;
    }

    public void setDateFormat(String dateFormat)
    {
      this.dateFormat = dateFormat;
    }

  }

  /**
   * Extracts the fields from a fixed width record and returns a map containing
   * field names and values
   */
  @Override
  Map<String, Object> extractFields(String line)
  {
    if (!initialized) {
      init();
      initialized = true;
    }
    String[] values = fixedWidthParser.parseLine(line);
    if (hasHeader && Arrays.deepEquals(values, header)) {
      return null;
    }
    Map<String, Object> map = Maps.newHashMap();
    int i = 0;
    for (FixedWidthField field : fields) {
      map.put(field.getName(), getValue(field, values[i++]));
    }
    return map;
  }

  private void init()
  {
    fields = new ArrayList<FixedWidthField>();
    List<String> headers = new ArrayList<String>();
    List<Integer> fieldWidth = new ArrayList<Integer>();
    for (String tmp : fieldDescription.split(",")) {
      String[] fieldTuple = tmp.split(":(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
      FixedWidthField field = new FixedWidthField();
      field.setName(fieldTuple[0]);
      field.setType(fieldTuple[1]);
      field.setWidth(Integer.parseInt(fieldTuple[2]));
      headers.add(fieldTuple[0]);
      fieldWidth.add(Integer.parseInt(fieldTuple[2]));
      if (field.getType() == FIELD_TYPE.DATE) {
        if (fieldTuple.length > 3) {
          field.setDateFormat(fieldTuple[3].replace("\"", ""));
        } else {
          logger.error("Date format is missing for the field {}", field.getName());
          throw new RuntimeException("Missing date format");
        }
      }
      fields.add(field);
    }
    header = headers.toArray(new String[headers.size()]);
    int[] width = Ints.toArray(fieldWidth);
    FixedWidthFields lengths = new FixedWidthFields(header, width);
    FixedWidthParserSettings settings = new FixedWidthParserSettings(lengths);
    settings.getFormat().setPadding(this.padding);
    fixedWidthParser = new FixedWidthParser(settings);
  }

  private Object getValue(FixedWidthField field, String value)
  {
    if (StringUtils.isEmpty(value)) {
      return null;
    }
    switch (field.getType()) {
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      case DOUBLE:
        return Double.parseDouble(value);
      case INTEGER:
        return Integer.parseInt(value);
      case FLOAT:
        return Float.parseFloat(value);
      case LONG:
        return Long.parseLong(value);
      case SHORT:
        return Short.parseShort(value);
      case CHARACTER:
        return value.charAt(0);
      case DATE:
        try {
          return new SimpleDateFormat(field.getDateFormat()).parse(value);
        } catch (ParseException e) {
          logger.error("Error parsing date for format {} and value {}", field.getDateFormat(), value);
          throw new RuntimeException(e);
        }
      default:
        return value;
    }
  }
}
