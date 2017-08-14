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

package org.apache.apex.malhar.lib.formatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.datatorrent.api.Context.OperatorContext;

/**
 * Operator that converts POJO to JSON string <br>
 *
 * @displayName JsonFormatter
 * @category Formatter
 * @tags pojo json formatter
 * @since 3.2.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class JsonFormatter extends Formatter<String>
{
  private transient ObjectMapper objMapper;

  @Override
  public void setup(OperatorContext context)
  {
    objMapper = new ObjectMapper();
  }

  @Override
  public String convert(Object tuple)
  {
    if (tuple == null) {
      return null;
    }
    try {
      return objMapper.writeValueAsString(tuple);
    } catch (JsonProcessingException e) {
      logger.error("Error while converting tuple {} {}", tuple, e);
    }
    return null;
  }

  private static final Logger logger = LoggerFactory.getLogger(JsonFormatter.class);
}
