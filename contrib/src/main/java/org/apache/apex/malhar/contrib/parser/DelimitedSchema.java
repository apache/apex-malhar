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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * This is schema that defines fields and their constraints for delimited files
 * The operators use this information to validate the incoming tuples.
 * Information from JSON schema is saved in this object and is used by the
 * operators
 * <p>
 * <br>
 * <br>
 * Example schema <br>
 * <br>
 * {@code{ "separator": ",", "quoteChar":"\"", "fields": [ "name": "adId",
 * "type": "Integer", "constraints": "required": "true" } , { "name": "adName",
 * "type": "String", "constraints": { "required": "true", "pattern":
 * "[a-z].*[a-z]$", "maxLength": "20" } }, { "name": "bidPrice", "type":
 * "Double", "constraints": { "required": "true", "minValue": "0.1", "maxValue":
 * "3.2" } }, { "name": "startDate", "type": "Date", "constraints": { "format":
 * "dd/MM/yyyy" } }, { "name": "securityCode", "type": "Long", "constraints": {
 * "minValue": "10", "maxValue": "30" } }, { "name": "active", "type":
 * "Boolean", "constraints": { "required": "true" } } ] }}
 *
 * @since 3.7.0
 */
public class DelimitedSchema extends Schema
{
  /**
   * JSON key string for required constraint
   */
  public static final String REQUIRED = "required";
  /**
   * JSON key string for equals constraint
   */
  public static final String EQUALS = "equals";
  /**
   * JSON key string for length constraint
   */
  public static final String LENGTH = "length";
  /**
   * JSON key string for min length constraint
   */
  public static final String MIN_LENGTH = "minLength";
  /**
   * JSON key string for max length constraint
   */
  public static final String MAX_LENGTH = "maxLength";
  /**
   * JSON key string for min value constraint
   */
  public static final String MIN_VALUE = "minValue";
  /**
   * JSON key string for max value constraint
   */
  public static final String MAX_VALUE = "maxValue";
  /**
   * JSON key string for regex pattern constraint
   */
  public static final String REGEX_PATTERN = "pattern";
  /**
   * JSON key string for locale constraint
   */
  public static final String LOCALE = "locale";
  /**
   * JSON key string for separator
   */
  private static final String SEPARATOR = "separator";
  /**
   * JSON key string for quote character
   */
  private static final String QUOTE_CHAR = "quoteChar";
  /**
   * JSON key string for line delimiter
   */
  private static final String LINE_DELIMITER = "lineDelimiter";
  /**
   * JSON key string for constraints for each field
   */
  private static final String CONSTRAINTS = "constraints";
  private static final Logger logger = LoggerFactory.getLogger(DelimitedSchema.class);
  /**
   * delimiter character provided in schema. Default is ,
   */
  private int delimiterChar = ',';
  /**
   * quote character provided in schema. Default is "
   */
  private char quoteChar = '\"';
  /**
   * line delimiter character provided in schema. Default is new line character
   */
  private String lineDelimiter = "\r\n";
  /**
   * This holds list of {@link Field}
   */
  private List<Field> fields = new LinkedList<Field>();

  public DelimitedSchema(String json)
  {
    try {
      initialize(json);
    } catch (JSONException | IOException e) {
      logger.error("{}", e);
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * For a given json string, this method sets the field members
   *
   * @param json
   * @throws JSONException
   * @throws IOException
   */
  private void initialize(String json) throws JSONException, IOException
  {
    JSONObject jo = new JSONObject(json);
    if (jo.has(SEPARATOR)) {
      delimiterChar = ((String)jo.getString(SEPARATOR)).charAt(0);
    }
    if (jo.has(QUOTE_CHAR)) {
      quoteChar = ((String)jo.getString(QUOTE_CHAR)).charAt(0);
    }
    if (jo.has(LINE_DELIMITER)) {
      lineDelimiter = (String)jo.getString(LINE_DELIMITER);
    }

    JSONArray fieldArray = jo.getJSONArray(FIELDS);
    for (int i = 0; i < fieldArray.length(); i++) {
      JSONObject obj = fieldArray.getJSONObject(i);
      Field field = new Field(obj.getString(NAME), obj.getString(TYPE));
      fields.add(field);
      fieldNames.add(field.name);
      if (obj.has(CONSTRAINTS)) {
        JSONObject constraints = obj.getJSONObject(CONSTRAINTS);
        field.constraints = new ObjectMapper().readValue(constraints.toString(), HashMap.class);
      }
    }
  }

  /**
   * Get the delimiter character
   *
   * @return delimiterChar
   */
  public int getDelimiterChar()
  {
    return delimiterChar;
  }

  /**
   * Get the quoteChar
   *
   * @return quoteChar
   */
  public char getQuoteChar()
  {
    return quoteChar;
  }

  /**
   * Get the line delimiter
   *
   * @return lineDelimiter
   */
  public String getLineDelimiter()
  {
    return lineDelimiter;
  }

  @Override
  public String toString()
  {
    return "DelimitedSchema [delimiterChar=" + delimiterChar + ", quoteChar=" + quoteChar + ", lineDelimiter="
        + lineDelimiter + ", fieldNames=" + fieldNames + ", fields=" + fields + "]";
  }

  /**
   * Get the list of Fields.
   *
   * @return fields
   */
  public List<Field> getFields()
  {
    return Collections.unmodifiableList(fields);
  }

  /**
   * Objects of this class represents a particular field in the schema. Each
   * field has a name, type and a set of associated constraints.
   *
   */
  public class Field extends Schema.Field
  {
    /**
     * constraints associated with the field
     */
    Map<String, Object> constraints = new HashMap<String, Object>();

    public Field(String name, String type)
    {
      super(name, type);
    }

    /**
     * Get the map of constraints associated with the field
     *
     * @return constraints
     */
    public Map<String, Object> getConstraints()
    {
      return constraints;
    }

    /**
     * Sets the map of constraints associated with the field
     *
     * @param constraints
     */
    public void setConstraints(Map<String, Object> constraints)
    {
      this.constraints = constraints;
    }

    @Override
    public String toString()
    {
      return "Fields [name=" + name + ", type=" + type + ", constraints=" + constraints + "]";
    }
  }

}
