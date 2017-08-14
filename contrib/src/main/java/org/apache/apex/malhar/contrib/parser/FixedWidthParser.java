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

import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.parser.Parser;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ClassUtils;

import com.google.common.annotations.VisibleForTesting;
import com.univocity.parsers.fixed.FieldAlignment;
import com.univocity.parsers.fixed.FixedWidthFields;
import com.univocity.parsers.fixed.FixedWidthParserSettings;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 * Operator that parses a fixed width record against a specified schema <br>
 * Schema is specified in a json format as per {@link FixedWidthSchema} that
 * contains field information for each field.<br>
 * Assumption is that each field in the data should map to a simple
 * java type.<br>
 * <br>
 * <b>Properties</b> <br>
 * <b>schema</b>:schema as a string<br>
 * <b>clazz</b>:Pojo class <br>
 * <b>Ports</b> <br>
 * <b>in</b>:input tuple as a byte array. Each tuple represents a record<br>
 * <b>parsedOutput</b>:tuples that are validated against the schema are emitted
 * as HashMap<String,Object> on this port<br>
 * Key being the name of the field and Val being the value of the field.
 * <b>out</b>:tuples that are validated against the schema are emitted as pojo
 * on this port<br>
 * <b>err</b>:tuples that do not confine to schema are emitted on this port as
 * KeyValPair<String,String><br>
 * Key being the tuple and Val being the reason.
 *
 * @displayName FixedWidthParser
 * @category Parsers
 * @tags fixedwidth pojo parser
 *
 * @since 3.7.0
 */
public class FixedWidthParser extends Parser<byte[], KeyValPair<String, String>> implements Operator.ActivationListener<Context>
{
  private static final Logger logger = LoggerFactory.getLogger(FixedWidthParser.class);
  public final transient DefaultOutputPort<HashMap<String, Object>> parsedOutput = new DefaultOutputPort<HashMap<String, Object>>();
  /**
   * Metric to keep count of number of tuples emitted on {@link #parsedOutput}
   * port
   */
  @AutoMetric
  private long parsedOutputCount;
  /**
   * Contents of the schema.Schema is specified in a json format as per
   * {@link FixedWidthSchema}
   */
  @NotNull
  private String jsonSchema;
  /**
   * Total length of the record
   */
  private int recordLength;
  /**
   * Schema is read into this object to access fields
   */
  private transient FixedWidthSchema schema;
  /**
   * List of setters to set the value in POJO to be emitted
   */
  private transient List<FixedWidthParser.TypeInfo> setters;
  /**
   * header- This will be string of field names, padded with padding character (if required)
   */
  private transient String header;
  /**
   * Univocity Parser to parse the input tuples
   */
  private com.univocity.parsers.fixed.FixedWidthParser univocityParser;

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    parsedOutputCount = 0;
  }

  @Override
  public void processTuple(byte[] tuple)
  {
    if (tuple == null) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(null, "Blank/null tuple"));
        logger.error("Tuple could not be parsed. Reason Blank/null tuple");
      }
      errorTupleCount++;
      return;
    }
    String incomingString = new String(tuple);
    if (StringUtils.isBlank(incomingString) || StringUtils.equals(incomingString, getHeader())) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<>(incomingString, "Blank/header tuple"));
        logger.error("Tuple could not be parsed. Reason Blank/header tuple");
      }
      errorTupleCount++;
      return;
    }
    if (incomingString.length() < recordLength) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<>(incomingString, "Record length mis-match/shorter tuple"));
      }
      logger.error("Tuple could not be parsed. Reason Record length mis-match/shorter tuple. " +
          "Expected length " + recordLength + " Actual length " + incomingString.length());
      errorTupleCount++;
      return;
    }
    if (incomingString.length() > recordLength) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<>(incomingString, "Record length mis-match/longer tuple"));
      }
      logger.error("Tuple could not be parsed. Reason Record length mis-match/longer tuple. " +
          "Expected length " + recordLength + " Actual length " + incomingString.length());
      errorTupleCount++;
      return;
    }
    try {
      String[] values = univocityParser.parseLine(incomingString);
      HashMap<String, Object> toEmit = new HashMap();
      Object pojo = validateAndSet(values, toEmit);
      if (parsedOutput.isConnected()) {
        parsedOutput.emit(toEmit);
        parsedOutputCount++;
      }
      if (out.isConnected() && clazz != null) {
        out.emit(pojo);
        emittedObjectCount++;
      }
    } catch (Exception e) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<>(incomingString, e.getMessage()));
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

  @Override
  public Object convert(byte[] tuple)
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    try {
      schema = new FixedWidthSchema(jsonSchema);
      recordLength = 0;
      List<FixedWidthSchema.Field> fields = schema.getFields();
      for (int i = 0; i < fields.size(); i++) {
        recordLength += fields.get(i).getFieldLength();
      }
      createUnivocityParser();
    } catch (Exception e) {
      logger.error("Cannot setup Parser Reason {}", e.getMessage());
      throw e;
    }
  }

  /**
   * Activate the Parser
   */
  @Override
  public void activate(Context context)
  {
    try {
      if (clazz != null) {
        setters = new ArrayList<>();
        List<String> fieldNames = schema.getFieldNames();
        if (fieldNames != null) {
          for (String fieldName : fieldNames) {
            addSetter(fieldName);
          }
        }
      }
    } catch (Exception e) {
      logger.error("Cannot activate Parser Reason {}", e.getMessage());
      throw e;
    }
  }

  /**
   * Function to create a univocity Parser
   */
  private void createUnivocityParser()
  {
    List<FixedWidthSchema.Field> fields = schema.getFields();
    FixedWidthFields fieldWidthFields = new FixedWidthFields();

    for (int i = 0; i < fields.size(); i++) {
      FixedWidthSchema.Field currentField = fields.get(i);
      int fieldLength = currentField.getFieldLength();
      FieldAlignment currentFieldAlignment;

      if (currentField.getAlignment().equalsIgnoreCase("centre")) {
        currentFieldAlignment = FieldAlignment.CENTER;
      } else if (currentField.getAlignment().equalsIgnoreCase("left")) {
        currentFieldAlignment = FieldAlignment.LEFT;
      } else {
        currentFieldAlignment = FieldAlignment.RIGHT;
      }
      fieldWidthFields.addField(currentField.getName(), fieldLength, currentFieldAlignment, currentField.getPadding());
    }
    FixedWidthParserSettings settings = new FixedWidthParserSettings(fieldWidthFields);
    univocityParser = new com.univocity.parsers.fixed.FixedWidthParser(settings);
  }

  @Override
  public void deactivate()
  {

  }

  /**
   * Function to add a setter for a field and add it
   * to the List of setters
   *
   * @param fieldName name of the field for which setter is to be added
   */
  private void addSetter(String fieldName)
  {
    try {
      Field f = clazz.getDeclaredField(fieldName);
      FixedWidthParser.TypeInfo t = new FixedWidthParser.TypeInfo(f.getName(),
          ClassUtils.primitiveToWrapper(f.getType()));
      t.setter = PojoUtils.createSetter(clazz, t.name, t.type);
      setters.add(t);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("Field " + fieldName + " not found in class " + clazz, e);
    } catch (Exception e) {
      throw new RuntimeException("Exception while adding a setter" + e.getMessage(), e);
    }
  }

  /**
   * Function to validate individual parsed values and set the objects to be emitted
   * @param values array of String containing individual parsed values
   * @param toEmit the map to be emitted
   * @return POJO the object to be returned (if the tuple class is set)
   */
  private Object validateAndSet(String[] values, HashMap toEmit)
  {
    Object pojoObject = null;
    try {
      List<FixedWidthSchema.Field> fields = schema.getFields();
      try {
        if (clazz != null) {
          pojoObject = clazz.newInstance();
        }
      } catch (InstantiationException ie) {
        throw new RuntimeException("Exception in instantiating", ie);
      }
      for (int i = 0; i < fields.size(); i++) {
        FixedWidthSchema.Field currentField = fields.get(i);
        FixedWidthParser.TypeInfo typeInfo = setters.get(i);
        validateAndSetCurrentField(currentField,
            values[i], typeInfo, pojoObject, toEmit);
      }
    } catch (StringIndexOutOfBoundsException e) {
      throw new RuntimeException("Record length and tuple length mismatch ", e);
    } catch (IllegalAccessException ie) {
      throw new RuntimeException("Illegal Access ", ie);
    } catch (Exception e) {
      throw new RuntimeException("Exception in validation", e);
    }
    return pojoObject;
  }

  /**
   * Function to validate and set the current field.
   * @param currentField the field which is to be validated and set
   * @param value the parsed value of the field
   * @param typeInfo information about the field in POJO
   * @param pojoObject POJO which is to be set
   * @param toEmit the map to be emitted
   */
  private void validateAndSetCurrentField(FixedWidthSchema.Field currentField,
      String value, FixedWidthParser.TypeInfo typeInfo, Object pojoObject, HashMap toEmit)
  {
    try {
      String fieldName = currentField.getName();
      if (value != null && !value.isEmpty()) {
        Object result;
        switch (currentField.getType()) {
          case INTEGER:
            result = Integer.parseInt(value);
            break;
          case DOUBLE:
            result = Double.parseDouble(value);
            break;
          case STRING:
            result = value;
            break;
          case CHARACTER:
            result = value.charAt(0);
            break;
          case FLOAT:
            result = Float.parseFloat(value);
            break;
          case LONG:
            result = Long.parseLong(value);
            break;
          case SHORT:
            result = Short.parseShort(value);
            break;
          case BOOLEAN:
            if (value.compareToIgnoreCase(currentField.getTrueValue()) == 0) {
              result = Boolean.parseBoolean("true");
            } else if (value.compareToIgnoreCase(currentField.getFalseValue()) == 0) {
              result = Boolean.parseBoolean("false");
            } else {
              throw new NumberFormatException();
            }
            break;
          case DATE:
            DateFormat df = new SimpleDateFormat(currentField.getDateFormat());
            df.setLenient(false);
            result = df.parse(value);
            break;
          default:
            throw new RuntimeException("Invalid Type in Field", new Exception());
        }
        toEmit.put(fieldName,result);
        if (typeInfo != null && pojoObject != null) {
          typeInfo.setter.set(pojoObject, result);
        }
      } else {
        toEmit.put(fieldName,value);
      }
    } catch (NumberFormatException e) {
      throw new RuntimeException("Error parsing" + value + " to Integer type", e);
    } catch (ParseException e) {
      throw new RuntimeException("Error parsing" + value, e);
    } catch (Exception e) {
      throw new RuntimeException("Error setting " + value + " in the given class" + typeInfo.toString(), e);
    }
  }

  /**
   * Get the schema
   *
   * @return the Json schema
   */
  public String getJsonSchema()
  {
    return jsonSchema;
  }

  /**
   * Set the jsonSchema
   *
   * @param jsonSchema schema to be set.
   */
  public void setJsonSchema(String jsonSchema)
  {
    this.jsonSchema = jsonSchema;
  }

  /**
   * Get the header
   *
   * @return header- This will be string of field names, padded with padding character (if required)
   */
  public String getHeader()
  {
    return header;
  }

  /**
   * Set the header
   *
   * @param header- This will be string of field names, padded with padding character (if required)
   */
  public void setHeader(String header)
  {
    this.header = header;
  }

  /**
   * Get errorTupleCount
   *
   * @return errorTupleCount number of erroneous tuples.
   */
  @VisibleForTesting
  public long getErrorTupleCount()
  {
    return errorTupleCount;
  }

  /**
   * Get emittedObjectCount
   *
   * @return emittedObjectCount count of objects emitted.
   */
  @VisibleForTesting
  public long getEmittedObjectCount()
  {
    return emittedObjectCount;
  }

  /**
   * Get incomingTuplesCount
   *
   * @return incomingTuplesCount number of incoming tuples.
   */
  @VisibleForTesting
  public long getIncomingTuplesCount()
  {
    return incomingTuplesCount;
  }

  /**
   * Get parsedOutputCount
   *
   * @return parsedOutPutCount count of well parsed tuples.
   */
  @VisibleForTesting
  public long getParsedOutputCount()
  {
    return parsedOutputCount;
  }

  /**
   * Objects of this class represents a particular data member of the Class to be emitted.
   * Each data member  has a name, type and a accessor(setter) function associated with it.
   */
  static class TypeInfo
  {
    String name;
    Class type;
    PojoUtils.Setter setter;

    public TypeInfo(String name, Class<?> type)
    {
      this.name = name;
      this.type = type;
    }

    public String toString()
    {
      return "'name': " + name + " 'type': " + type;
    }
  }

}
