/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.schemas.Fields;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AggregatorDescriptor
{
  private static final Logger logger = LoggerFactory.getLogger(AggregatorDescriptor.class);

  public static final String DIMENSION_TIME = "time";

  public static final Set<String> RESERVED_DIMENSION_NAMES = ImmutableSet.of(DIMENSION_TIME);

  public static final String DELIMETER_EQUALS = "=";
  public static final String DELIMETER_SEPERATOR = ":";

  private DimensionsSchema schema;
  private TimeUnit timeUnit;
  private Fields fields;

  public AggregatorDescriptor(String aggregationString,
                              DimensionsSchema schema)
  {
    setSchema(schema);
    initialize(aggregationString);
  }

  private void initialize(String aggregationString)
  {
    String[] fieldArray = aggregationString.split(DELIMETER_SEPERATOR);
    Set<String> fieldSet = Sets.newHashSet();

    for(String field: fieldArray) {
      String[] fieldAndValue = field.split(DELIMETER_EQUALS);
      String fieldName = fieldAndValue[0];
      fieldSet.add(fieldName);

      if(fieldName.equals(DIMENSION_TIME)) {
        if(fieldAndValue.length == 2) {
          timeUnit = TimeUnit.valueOf(fieldAndValue[1]);
        }
      }
    }

    fields = new Fields(fieldSet);
  }

  private void setSchema(DimensionsSchema schema)
  {
    Preconditions.checkNotNull(schema);
    this.schema = schema;
  }

  public DimensionsSchema getSchema()
  {
    return schema;
  }

  public TimeUnit getTimeUnit()
  {
    return timeUnit;
  }

  public Fields getFields()
  {
    return fields;
  }

  public static boolean validateDimensions(DimensionsSchema schema,
                                           String aggregationString)
  {
    if(aggregationString.isEmpty()) {
      logger.error("The dimensions string cannot be empty.");
      return false;
    }

    Set<String> fieldSet = Sets.newHashSet();
    String[] fields = aggregationString.split(DELIMETER_SEPERATOR);

    for(String field: fields) {
      String[] fieldAndValue = field.split(DELIMETER_EQUALS);

      if(fieldAndValue.length == 0) {
        logger.error("There is no field data in: {}", aggregationString);
        return false;
      }

      String fieldName = fieldAndValue[0];

      if(!schema.getKeyFieldDescriptor().getFields().getFields().contains(fieldName)) {
        logger.error("{} is not a valid field.", fieldName);
        return false;
      }

      if(!fieldSet.add(fieldName)) {
        logger.error("Cannot duplicate the field {} in the aggregation string.", fieldName);
        return false;
      }

      if(fieldAndValue.length > 2) {
        logger.error("Cannot have more than one " +
                     DELIMETER_EQUALS +
                     " symbols in the field expression: {}",
                     field);
        return false;
      }
      else if(fieldAndValue.length == 2)
      {
        if(fieldName.equals(DIMENSION_TIME)) {
          String fieldValue = fieldAndValue[1];

          try {
            TimeUnit.valueOf(fieldValue);
          }
          catch(Exception e) {
            logger.error("No valid timeunit for " + fieldValue, e);
          }
        }
        else {
          logger.error("A bucket cannot be specified for field: {}", fieldName);
          return false;
        }
      }
    }

    return true;
  }
}
