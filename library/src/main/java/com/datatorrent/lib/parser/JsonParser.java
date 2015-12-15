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
package com.datatorrent.lib.parser;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Operator that converts JSON string to Pojo <br>
 * <b>Properties</b> <br>
 * <b>dateFormat</b>: date format e.g dd/MM/yyyy
 * 
 * @displayName JsonParser
 * @category Parsers
 * @tags json pojo parser
 * @since 3.2.0
 */
@InterfaceStability.Evolving
public class JsonParser extends Parser<String>
{

  private transient ObjectReader reader;
  protected String dateFormat;

  @Override
  public void activate(Context context)
  {
    try {
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      if (dateFormat != null) {
        mapper.setDateFormat(new SimpleDateFormat(dateFormat));
      }
      reader = mapper.reader(clazz);
    } catch (Throwable e) {
      throw new RuntimeException("Unable find provided class");
    }
  }

  @Override
  public void deactivate()
  {
  }

  @Override
  public Object convert(String tuple)
  {
    try {
      if (!StringUtils.isEmpty(tuple)) {
        return reader.readValue(tuple);
      }
    } catch (JsonProcessingException e) {
      logger.debug("Error while converting tuple {} {}", tuple, e.getMessage());
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
    return null;
  }

  /**
   * Get the date format
   * 
   * @return Date format string
   */
  public String getDateFormat()
  {
    return dateFormat;
  }

  /**
   * Set the date format
   * 
   * @param dateFormat
   */
  public void setDateFormat(String dateFormat)
  {
    this.dateFormat = dateFormat;
  }

  private static final Logger logger = LoggerFactory.getLogger(JsonParser.class);
}
