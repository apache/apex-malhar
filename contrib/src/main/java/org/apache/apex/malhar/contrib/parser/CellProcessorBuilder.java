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

import java.util.Map;

import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseBool;
import org.supercsv.cellprocessor.ParseChar;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.DMinMax;
import org.supercsv.cellprocessor.constraint.Equals;
import org.supercsv.cellprocessor.constraint.LMinMax;
import org.supercsv.cellprocessor.constraint.StrMinMax;
import org.supercsv.cellprocessor.constraint.StrRegEx;
import org.supercsv.cellprocessor.constraint.Strlen;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.cellprocessor.ift.DoubleCellProcessor;
import org.supercsv.cellprocessor.ift.LongCellProcessor;
import org.supercsv.util.CsvContext;

import org.apache.apex.malhar.contrib.parser.Schema.FieldType;
import org.apache.commons.lang3.StringUtils;

/**
 * Helper class with methods to generate CellProcessor objects. Cell processors
 * are an integral part of reading and writing with Super CSV - they automate
 * the data type conversions, and enforce constraints. They implement the chain
 * of responsibility design pattern - each processor has a single, well-defined
 * purpose and can be chained together with other processors to fully automate
 * all of the required conversions and constraint validation for a single
 * delimited record.
 *
 *
 * @since 3.4.0
 */
public class CellProcessorBuilder
{

  /**
   * Method to get cell processors for given field type and constraints
   *
   * @param fieldType
   *          data type of the field
   * @param constraints
   *          a map of constraints
   * @return
   */
  public static CellProcessor getCellProcessor(FieldType fieldType, Map<String, Object> constraints)
  {
    switch (fieldType) {
      case STRING:
        return getStringCellProcessor(constraints);
      case INTEGER:
        return getIntegerCellProcessor(constraints);
      case LONG:
        return getLongCellProcessor(constraints);
      case FLOAT:
      case DOUBLE:
        return getDoubleCellProcessor(constraints);
      case CHARACTER:
        return getCharCellProcessor(constraints);
      case BOOLEAN:
        return getBooleanCellProcessor(constraints);
      case DATE:
        return getDateCellProcessor(constraints);
      default:
        return null;
    }
  }

  /**
   * Method to get cellprocessor for String with constraints. These constraints
   * are evaluated against the String field for which this cellprocessor is
   * defined.
   *
   * @param constraints
   *          map of constraints applicable to String
   * @return CellProcessor
   */
  private static CellProcessor getStringCellProcessor(Map<String, Object> constraints)
  {
    Boolean required = constraints.get(DelimitedSchema.REQUIRED) == null ? null : Boolean
        .parseBoolean((String)constraints.get(DelimitedSchema.REQUIRED));
    Integer strLen = constraints.get(DelimitedSchema.LENGTH) == null ? null : Integer.parseInt((String)constraints
        .get(DelimitedSchema.LENGTH));
    Integer minLength = constraints.get(DelimitedSchema.MIN_LENGTH) == null ? null : Integer
        .parseInt((String)constraints.get(DelimitedSchema.MIN_LENGTH));
    Integer maxLength = constraints.get(DelimitedSchema.MAX_LENGTH) == null ? null : Integer
        .parseInt((String)constraints.get(DelimitedSchema.MAX_LENGTH));
    String equals = constraints.get(DelimitedSchema.EQUALS) == null ? null : (String)constraints
        .get(DelimitedSchema.EQUALS);
    String pattern = constraints.get(DelimitedSchema.REGEX_PATTERN) == null ? null : (String)constraints
        .get(DelimitedSchema.REGEX_PATTERN);

    CellProcessor cellProcessor = null;
    if (StringUtils.isNotBlank(equals)) {
      cellProcessor = new Equals(equals);
    } else if (StringUtils.isNotBlank(pattern)) {
      cellProcessor = new StrRegEx(pattern);
    } else if (strLen != null) {
      cellProcessor = new Strlen(strLen);
    } else if (maxLength != null || minLength != null) {
      Long min = minLength == null ? 0L : minLength;
      Long max = maxLength == null ? LMinMax.MAX_LONG : maxLength;
      cellProcessor = new StrMinMax(min, max);
    }
    if (required == null || !required) {
      cellProcessor = addOptional(cellProcessor);
    }
    return cellProcessor;
  }

  /**
   * Method to get cellprocessor for Integer with constraints. These constraints
   * are evaluated against the Integer field for which this cellprocessor is
   * defined.
   *
   * @param constraints
   *          map of constraints applicable to Integer
   * @return CellProcessor
   */
  private static CellProcessor getIntegerCellProcessor(Map<String, Object> constraints)
  {
    Boolean required = constraints.get(DelimitedSchema.REQUIRED) == null ? null : Boolean
        .parseBoolean((String)constraints.get(DelimitedSchema.REQUIRED));
    Integer equals = constraints.get(DelimitedSchema.EQUALS) == null ? null : Integer.parseInt((String)constraints
        .get(DelimitedSchema.EQUALS));
    Integer minValue = constraints.get(DelimitedSchema.MIN_VALUE) == null ? null : Integer.parseInt((String)constraints
        .get(DelimitedSchema.MIN_VALUE));
    Integer maxValue = constraints.get(DelimitedSchema.MAX_VALUE) == null ? null : Integer.parseInt((String)constraints
        .get(DelimitedSchema.MAX_VALUE));

    CellProcessor cellProcessor = null;
    if (equals != null) {
      cellProcessor = new Equals(equals);
      cellProcessor = addParseInt(cellProcessor);
    } else if (minValue != null || maxValue != null) {
      cellProcessor = addIntMinMax(minValue, maxValue);
    } else {
      cellProcessor = addParseInt(null);
    }
    if (required == null || !required) {
      cellProcessor = addOptional(cellProcessor);
    }
    return cellProcessor;
  }

  /**
   * Method to get cellprocessor for Long with constraints. These constraints
   * are evaluated against the Long field for which this cellprocessor is
   * defined.
   *
   * @param constraints
   *          map of constraints applicable to Long
   * @return CellProcessor
   */
  private static CellProcessor getLongCellProcessor(Map<String, Object> constraints)
  {
    Boolean required = constraints.get(DelimitedSchema.REQUIRED) == null ? null : Boolean
        .parseBoolean((String)constraints.get(DelimitedSchema.REQUIRED));
    Long equals = constraints.get(DelimitedSchema.EQUALS) == null ? null : Long.parseLong((String)constraints
        .get(DelimitedSchema.EQUALS));
    Long minValue = constraints.get(DelimitedSchema.MIN_VALUE) == null ? null : Long.parseLong((String)constraints
        .get(DelimitedSchema.MIN_VALUE));
    Long maxValue = constraints.get(DelimitedSchema.MAX_VALUE) == null ? null : Long.parseLong((String)constraints
        .get(DelimitedSchema.MAX_VALUE));
    CellProcessor cellProcessor = null;
    if (equals != null) {
      cellProcessor = new Equals(equals);
      cellProcessor = addParseLong(cellProcessor);
    } else if (minValue != null || maxValue != null) {
      cellProcessor = addLongMinMax(minValue, maxValue);
    } else {
      cellProcessor = addParseLong(null);
    }
    if (required == null || !required) {
      cellProcessor = addOptional(cellProcessor);
    }
    return cellProcessor;
  }

  /**
   * Method to get cellprocessor for Float/Double with constraints. These
   * constraints are evaluated against the Float/Double field for which this
   * cellprocessor is defined.
   *
   * @param constraints
   *          map of constraints applicable to Float/Double
   * @return CellProcessor
   */
  private static CellProcessor getDoubleCellProcessor(Map<String, Object> constraints)
  {
    Boolean required = constraints.get(DelimitedSchema.REQUIRED) == null ? null : Boolean
        .parseBoolean((String)constraints.get(DelimitedSchema.REQUIRED));
    Double equals = constraints.get(DelimitedSchema.EQUALS) == null ? null : Double.parseDouble((String)constraints
        .get(DelimitedSchema.EQUALS));
    Double minValue = constraints.get(DelimitedSchema.MIN_VALUE) == null ? null : Double
        .parseDouble((String)constraints.get(DelimitedSchema.MIN_VALUE));
    Double maxValue = constraints.get(DelimitedSchema.MAX_VALUE) == null ? null : Double
        .parseDouble((String)constraints.get(DelimitedSchema.MAX_VALUE));
    CellProcessor cellProcessor = null;
    if (equals != null) {
      cellProcessor = new Equals(equals);
      cellProcessor = addParseDouble(cellProcessor);
    } else if (minValue != null || maxValue != null) {
      cellProcessor = addDoubleMinMax(minValue, maxValue);
    } else {
      cellProcessor = addParseDouble(null);
    }
    if (required == null || !required) {
      cellProcessor = addOptional(cellProcessor);
    }
    return cellProcessor;
  }

  /**
   * Method to get cellprocessor for Boolean with constraints. These constraints
   * are evaluated against the Boolean field for which this cellprocessor is
   * defined.
   *
   * @param constraints
   *          map of constraints applicable to Boolean
   * @return CellProcessor
   */
  private static CellProcessor getBooleanCellProcessor(Map<String, Object> constraints)
  {
    Boolean required = constraints.get(DelimitedSchema.REQUIRED) == null ? null : Boolean
        .parseBoolean((String)constraints.get(DelimitedSchema.REQUIRED));
    String trueValue = constraints.get(DelimitedSchema.TRUE_VALUE) == null ? null : (String)constraints
        .get(DelimitedSchema.TRUE_VALUE);
    String falseValue = constraints.get(DelimitedSchema.FALSE_VALUE) == null ? null : (String)constraints
        .get(DelimitedSchema.FALSE_VALUE);
    CellProcessor cellProcessor = null;
    if (StringUtils.isNotBlank(trueValue) && StringUtils.isNotBlank(falseValue)) {
      cellProcessor = new ParseBool(trueValue, falseValue);
    } else {
      cellProcessor = new ParseBool();
    }
    if (required == null || !required) {
      cellProcessor = addOptional(cellProcessor);
    }
    return cellProcessor;
  }

  /**
   * Method to get cellprocessor for Date with constraints. These constraints
   * are evaluated against the Date field for which this cellprocessor is
   * defined.
   *
   * @param constraints
   *          map of constraints applicable to Date
   * @return CellProcessor
   */
  private static CellProcessor getDateCellProcessor(Map<String, Object> constraints)
  {
    Boolean required = constraints.get(DelimitedSchema.REQUIRED) == null ? null : Boolean
        .parseBoolean((String)constraints.get(DelimitedSchema.REQUIRED));
    String format = constraints.get(DelimitedSchema.DATE_FORMAT) == null ? null : (String)constraints
        .get(DelimitedSchema.DATE_FORMAT);
    CellProcessor cellProcessor = null;
    String fmt = StringUtils.isNotBlank(format) ? format : "dd/MM/yyyy";
    cellProcessor = new ParseDate(fmt, false);
    if (required == null || !required) {
      cellProcessor = addOptional(cellProcessor);
    }
    return cellProcessor;
  }

  /**
   * Method to get cellprocessor for Char with constraints. These constraints
   * are evaluated against the Char field for which this cellprocessor is
   * defined.
   *
   * @param constraints
   *          map of constraints applicable to Char
   * @return CellProcessor
   */
  private static CellProcessor getCharCellProcessor(Map<String, Object> constraints)
  {
    Boolean required = constraints.get(DelimitedSchema.REQUIRED) == null ? null : Boolean
        .parseBoolean((String)constraints.get(DelimitedSchema.REQUIRED));
    Character equals = constraints.get(DelimitedSchema.EQUALS) == null ? null : ((String)constraints
        .get(DelimitedSchema.EQUALS)).charAt(0);

    CellProcessor cellProcessor = null;
    if (equals != null) {
      cellProcessor = new Equals(equals);
    }
    cellProcessor = addParseChar(cellProcessor);
    if (required == null || !required) {
      cellProcessor = addOptional(cellProcessor);
    }
    return cellProcessor;
  }

  /**
   * Get a Double Min Max cellprocessor.
   *
   * @param minValue
   *          minimum value.
   * @param maxValue
   *          maximum value.
   * @return CellProcessor
   */
  private static CellProcessor addDoubleMinMax(Double minValue, Double maxValue)
  {
    Double min = minValue == null ? DMinMax.MIN_DOUBLE : minValue;
    Double max = maxValue == null ? DMinMax.MAX_DOUBLE : maxValue;
    return new DMinMax(min, max);
  }

  /**
   * Get a Long Min Max cellprocessor.
   *
   * @param minValue
   *          minimum value.
   * @param maxValue
   *          maximum value.
   * @return CellProcessor
   */
  private static CellProcessor addLongMinMax(Long minValue, Long maxValue)
  {
    Long min = minValue == null ? LMinMax.MIN_LONG : minValue;
    Long max = maxValue == null ? LMinMax.MAX_LONG : maxValue;
    return new LMinMax(min, max);
  }

  /**
   * Get a Int Min Max cellprocessor.
   *
   * @param minValue
   *          minimum value.
   * @param maxValue
   *          maximum value.
   * @return CellProcessor
   */
  private static CellProcessor addIntMinMax(Integer minValue, Integer maxValue)
  {
    Integer min = minValue == null ? Integer.MIN_VALUE : minValue;
    Integer max = maxValue == null ? Integer.MAX_VALUE : maxValue;
    return new IntMinMax(min, max);
  }

  /**
   * Get Optional cellprocessor which means field is not mandatory.
   *
   * @param cellProcessor
   *          next processor in the chain.
   * @return CellProcessor
   */
  private static CellProcessor addOptional(CellProcessor cellProcessor)
  {
    if (cellProcessor == null) {
      return new Optional();
    }
    return new Optional(cellProcessor);
  }

  /**
   * Get cellprocessor to parse String as Integer.
   *
   * @param cellProcessor
   *          next processor in the chain.
   * @return CellProcessor
   */
  private static CellProcessor addParseInt(CellProcessor cellProcessor)
  {
    if (cellProcessor == null) {
      return new ParseInt();
    }
    return new ParseInt((LongCellProcessor)cellProcessor);
  }

  /**
   * Get cellprocessor to parse String as Long.
   *
   * @param cellProcessor
   *          next processor in the chain.
   * @return CellProcessor
   */
  private static CellProcessor addParseLong(CellProcessor cellProcessor)
  {
    if (cellProcessor == null) {
      return new ParseLong();
    }
    return new ParseLong((LongCellProcessor)cellProcessor);
  }

  /**
   * Get cellprocessor to parse String as Double.
   *
   * @param cellProcessor
   *          next processor in the chain.
   * @return CellProcessor
   */
  private static CellProcessor addParseDouble(CellProcessor cellProcessor)
  {
    if (cellProcessor == null) {
      return new ParseDouble();
    }
    return new ParseDouble((DoubleCellProcessor)cellProcessor);
  }

  /**
   * Get cellprocessor to parse String as Character.
   *
   * @param cellProcessor
   *          next processor in the chain.
   * @return CellProcessor
   */
  private static CellProcessor addParseChar(CellProcessor cellProcessor)
  {
    if (cellProcessor == null) {
      return new ParseChar();
    }
    return new ParseChar((DoubleCellProcessor)cellProcessor);
  }

  /**
   * Custom Cell processor to handle min max constraints for Integers
   */
  private static class IntMinMax extends LMinMax
  {
    public IntMinMax(int min, int max)
    {
      super(min, max);
    }

    @Override
    public Object execute(Object value, CsvContext context)
    {
      Long result = (Long)super.execute(value, context);
      return result.intValue();
    }
  }

}
