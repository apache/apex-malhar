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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * This is schema that defines fields and their constraints for fixed width files
 * The operators use this information to validate the incoming tuples.
 * Information from JSON schema is saved in this object and is used by the
 * operators
 * <p>
 * <br>
 * <br>
 * Example schema <br>
 * <br>
 * {{ "padding": " ","alignment"="left","fields": [ {"name": "adId",
 * "type": "Integer","padding":"0", "length": 3} , { "name": "adName",
 * "type": "String", "alignment": "right","fieldLength": 20}, { "name": "bidPrice", "type":
 * "Double", "length": 5}, { "name": "startDate", "type": "Date", "length": 10,
 * "format":"dd/MM/yyyy" }, { "name": "securityCode", "type": "Long","length": 10 },
 * { "name": "active", "type":"Boolean","length": 2} ] }}
 *
 * @since 3.7.0
 */
public class FixedWidthSchema extends Schema
{
  /**
   * JSON key string for record length
   */
  public static final String FIELD_LENGTH = "length";
  /**
   * JSON key string for Padding Character
   */
  public static final String FIELD_PADDING_CHARACTER = "padding";
  /**
   * Default Padding Character
   */
  public static final char DEFAULT_PADDING_CHARACTER = ' ';
  /**
   * Default Alignment
   */
  public static final String DEFAULT_ALIGNMENT = "left";
  /**
   * JSON key string for Field Alignment
   */
  public static final String FIELD_ALIGNMENT = "alignment";

  public static final Logger logger = LoggerFactory.getLogger(FixedWidthSchema.class);
  /**
   * This holds list of {@link Field}
   */
  private List<Field> fields = new LinkedList<>();
  /**
   * This holds the padding character for the entire file
   */
  private char globalPadding;
  /**
   * This holds the global alignment
   */
  private String globalAlignment;

  /**
   * Constructor for FixedWidthSchema
   */
  public FixedWidthSchema(String json)
  {
    try {
      initialize(json);
    } catch (JSONException | IOException e) {
      logger.error("{}", e);
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Get the Padding character
   * @return the padding character for the entire file
   */
  public char getGlobalPadding()
  {
    return globalPadding;
  }

  /**
   * Set the padding character
   * @param padding the padding character for the entire file
   */
  public void setGlobalPadding(char padding)
  {
    this.globalPadding = padding;
  }

  /**
   * Get the global alignment
   * @return globalAlignment the global alignment for the entire file.
   */
  public String getGlobalAlignment()
  {
    return globalAlignment;
  }

  /**
   * Set the global alignment
   * @param globalAlignment the global alignment for the entire file
   */
  public void setGlobalAlignment(String globalAlignment)
  {
    this.globalAlignment = globalAlignment;
  }

  /**
   * For a given json string, this method sets the field members
   *
   * @param json schema as provided by the user.
   */
  private void initialize(String json) throws JSONException, IOException
  {

    JSONObject jo = new JSONObject(json);
    JSONArray fieldArray = jo.getJSONArray(FIELDS);
    if (jo.has(FIELD_PADDING_CHARACTER)) {
      globalPadding = jo.getString(FIELD_PADDING_CHARACTER).charAt(0);
    } else {
      globalPadding = DEFAULT_PADDING_CHARACTER;
    }
    if (jo.has(FIELD_ALIGNMENT)) {
      globalAlignment = jo.getString(FIELD_ALIGNMENT);
    } else {
      globalAlignment = DEFAULT_ALIGNMENT;
    }

    for (int i = 0; i < fieldArray.length(); i++) {
      JSONObject obj = fieldArray.getJSONObject(i);
      Field field = new Field(obj.getString(NAME),
          obj.getString(TYPE).toUpperCase(), obj.getInt(FIELD_LENGTH));
      if (obj.has(FIELD_PADDING_CHARACTER)) {
        field.setPadding(obj.getString(FIELD_PADDING_CHARACTER).charAt(0));
      } else {
        field.setPadding(globalPadding);
      }
      if (obj.has(FIELD_ALIGNMENT)) {
        field.setAlignment(obj.getString(FIELD_ALIGNMENT));
      } else {
        field.setAlignment(globalAlignment);
      }
      //Get the format if the given data type is Date
      if (field.getType() == FieldType.DATE) {
        if (obj.has(DATE_FORMAT)) {
          field.setDateFormat(obj.getString(DATE_FORMAT));
        } else {
          field.setDateFormat(DEFAULT_DATE_FORMAT);
        }

      }
      //Get the trueValue and falseValue if the data type is Boolean
      if (field.getType() == FieldType.BOOLEAN) {
        if (obj.has(TRUE_VALUE)) {
          field.setTrueValue(obj.getString(TRUE_VALUE));
        } else {
          field.setTrueValue(DEFAULT_TRUE_VALUE);
        }
        if (obj.has(FALSE_VALUE)) {
          field.setFalseValue(obj.getString(FALSE_VALUE));
        } else {
          field.setFalseValue(DEFAULT_FALSE_VALUE);
        }

      }
      fields.add(field);
      fieldNames.add(field.name);
    }
  }

  /**
   * Get the list of Fields.
   *
   * @return fields list of {@link Field}
   */
  public List<Field> getFields()
  {
    return Collections.unmodifiableList(fields);
  }

  /**
   * Objects of this class represents a particular field in the schema. Each
   * field has a name, type and a fieldLength.
   * In case of type Date we need a dateFormat.
   *
   */
  public class Field extends Schema.Field
  {
    /**
     * Length of the field
     */
    private int fieldLength;
    /**
     * Parameter to specify format of date
     */
    private String dateFormat;
    /**
     * Parameter to specify true value of Boolean
     */
    private String trueValue;
    /**
     * Parameter to specify false value of Boolean
     */
    private String falseValue;
    /**
     * Parameter to specify padding
     */
    private char padding;
    /**
     * Parameter to specify alignment
     */
    private String alignment;

    /**
     * Constructor for Field
     * @param name - name of the field.
     * @param type - type of the field.
     * @param fieldLength - length of the field.
     */
    public Field(String name, String type, Integer fieldLength)
    {
      super(name, type);
      this.fieldLength = fieldLength;
      this.dateFormat = DEFAULT_DATE_FORMAT;
      this.trueValue = DEFAULT_TRUE_VALUE;
      this.falseValue = DEFAULT_FALSE_VALUE;
      this.padding = ' ';
      this.alignment = DEFAULT_ALIGNMENT;
    }

    /**
     * Get the Length of the Field
     * @return fieldLength length of the field.
     */
    public int getFieldLength()
    {
      return fieldLength;
    }

    /**
     * Set the end pointer of the field
     *
     * @param fieldLength length of the field.
     */
    public void setFieldLength(Integer fieldLength)
    {
      this.fieldLength = fieldLength;
    }

    /**
     * Get the dateFormat of the field
     *
     * @return dateFormat format of date given.
     */
    public String getDateFormat()
    {
      return dateFormat;
    }

    /**
     * Set the the dateFormat of the field
     *
     * @param dateFormat sets the format of date.
     */
    public void setDateFormat(String dateFormat)
    {
      this.dateFormat = dateFormat;
    }

    /**
     * Get the trueValue of the Boolean field
     * @return trueValue gets the equivalent true value.
     */
    public String getTrueValue()
    {
      return trueValue;
    }

    /**
     * Set the trueValue of the Boolean field
     *
     * @param trueValue sets the equivalent true value.
     */
    public void setTrueValue(String trueValue)
    {
      this.trueValue = trueValue;
    }

    /**
     * Get the falseValue of the Boolean field
     * @return falseValue gets the equivalent false value.
     */
    public String getFalseValue()
    {
      return falseValue;
    }

    /**
     * Set the end pointer of the field
     *
     * @param falseValue sets the equivalent false value.
     */
    public void setFalseValue(String falseValue)
    {
      this.falseValue = falseValue;
    }

    /**
     * Get the field padding
     * @return padding gets the padding for the individual field.
     */
    public char getPadding()
    {
      return padding;
    }

    /**
     * Set the field padding
     * @param padding sets the padding for the individual field.
     */
    public void setPadding(char padding)
    {
      this.padding = padding;
    }

    /**
     * Get the field alignment
     * @return alignment gets the alignment for the individual field.
     */
    public String getAlignment()
    {
      return alignment;
    }

    /**
     * Set the field alignment
     * @param alignment sets the alignment for the individual field.
     */
    public void setAlignment(String alignment)
    {
      this.alignment = alignment;
    }

    @Override
    public String toString()
    {
      return "Fields [name=" + name + ", type=" + type + " fieldLength= " + fieldLength + "]";
    }
  }

}
