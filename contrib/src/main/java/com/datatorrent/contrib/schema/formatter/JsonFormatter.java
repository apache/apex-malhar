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
package com.datatorrent.contrib.schema.formatter;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Operator that converts POJO to JSON string <br>
 * <b>Properties</b> <br>
 * <b>dateFormat</b>: date format e.g dd/MM/yyyy
 * 
 * @displayName JsonFormatter
 * @category Formatter
 * @tags pojo json formatter
 */
@InterfaceStability.Evolving
public class JsonFormatter extends Formatter<String>
{
  private transient ObjectWriter writer;
  protected String dateFormat;

  @Override
  public void activate(Context context)
  {
    try {
      ObjectMapper mapper = new ObjectMapper();
      if (dateFormat != null) {
        mapper.setDateFormat(new SimpleDateFormat(dateFormat));
      }
      writer = mapper.writerWithType(clazz);
      mapper.configure(SerializationConfig.Feature.AUTO_DETECT_FIELDS, true);
      mapper.configure(SerializationConfig.Feature.AUTO_DETECT_GETTERS, true);
      mapper.configure(SerializationConfig.Feature.AUTO_DETECT_IS_GETTERS, true);
    } catch (Throwable e) {
      throw new RuntimeException("Unable find provided class");
    }
  }

  @Override
  public void deactivate()
  {

  }

  @Override
  public String convert(Object tuple)
  {
    try {
      return writer.writeValueAsString(tuple);
    } catch (JsonGenerationException | JsonMappingException e) {
      logger.debug("Error while converting tuple {} {}",tuple,e.getMessage());
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

  private static final Logger logger = LoggerFactory.getLogger(JsonFormatter.class);
}
