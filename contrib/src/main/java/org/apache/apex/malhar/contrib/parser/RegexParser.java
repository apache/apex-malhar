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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.parser.Parser;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConversionException;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.DateConverter;
import org.apache.commons.beanutils.converters.DateTimeConverter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.Context;

/**
 * Operator parses tuple based on regex pattern and populates POJO matching the user defined schema <br>
 * This operator expects the upstream operator to send every line in the file as a byte array.
 * splitRegexPattern contains the regex pattern of lines in the file <br>
 * Schema is specified in a json format as per {@link DelimitedSchema} that
 * contains field information and constraints for each field.<br>
 * Schema field names should match with the POJO variable names<br>
 * Assumption is that each field in the delimited data should map to a simple
 * java type.<br>
 * <br>
 * <b>Properties</b> <br>
 * <b>splitRegexPattern</b>:Regex pattern as a string<br>
 *
 * @displayName RegexParser
 * @category Parsers
 * @tags pojo parser regex logs server
 * @since 3.7.0
 */
@InterfaceStability.Evolving
public class RegexParser extends Parser<byte[], KeyValPair<String, String>>
{
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
   * Regex Pattern defined for the tuple
   */
  @NotNull
  private String splitRegexPattern;
  /**
   * Pattern to store the compiled regex
   */
  private transient Pattern pattern;

  @Override
  public void setup(Context.OperatorContext context)
  {
    delimitedParserSchema = new DelimitedSchema(schema);
    pattern = Pattern.compile(splitRegexPattern);
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
    if (StringUtils.isBlank(incomingString)) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(incomingString, "Blank tuple"));
      }
      errorTupleCount++;
      return;
    }
    try {
      if (out.isConnected() && clazz != null) {
        Matcher matcher = pattern.matcher(incomingString);
        boolean patternMatched = false;
        Constructor<?> ctor = clazz.getConstructor();
        Object object = ctor.newInstance();

        if (matcher.find()) {
          for (int i = 0; i <= matcher.groupCount() - 1; i++) {
            if (delimitedParserSchema.getFields().get(i).getType() == DelimitedSchema.FieldType.DATE) {
              DateTimeConverter dtConverter = new DateConverter();
              dtConverter.setPattern((String)delimitedParserSchema.getFields().get(i).getConstraints().get(DelimitedSchema.DATE_FORMAT));
              ConvertUtils.register(dtConverter, Date.class);
            }
            BeanUtils.setProperty(object, delimitedParserSchema.getFields().get(i).getName(), matcher.group(i + 1));
          }
          patternMatched = true;
        }
        if (!patternMatched) {
          throw new ConversionException("The incoming tuple do not match with the Regex pattern defined.");
        }

        out.emit(object);
        emittedObjectCount++;
      }

    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException | InstantiationException | ConversionException e) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(incomingString, e.getMessage()));
        logger.debug("Regex Expression : {} Incoming tuple : {}", splitRegexPattern, incomingString);
      }
      errorTupleCount++;
      logger.error("Tuple could not be parsed. Reason {}", e.getMessage());
    }
  }

  /**
   * Set the schema that defines the format of the tuple
   *
   * @param schema
   */
  public void setSchema(String schema)
  {
    this.schema = schema;
  }

  /**
   * Set the Regex Pattern expected for the incoming tuple
   *
   * @param splitRegexPattern
   */
  public void setSplitRegexPattern(String splitRegexPattern)
  {
    this.splitRegexPattern = splitRegexPattern;
  }

  /**
   * Get the schema value
   *
   * @return schema
   */
  public String getSchema()
  {
    return schema;
  }

  /**
   * Get the Regex Pattern value
   *
   * @return splitRegexPattern
   */
  public String getSplitRegexPattern()
  {
    return splitRegexPattern;
  }

  @Override
  public Object convert(byte[] tuple)
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public KeyValPair<String, String> processErrorTuple(byte[] input)
  {
    throw new UnsupportedOperationException("Not supported");
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

  private static final Logger logger = LoggerFactory.getLogger(RegexParser.class);
}
