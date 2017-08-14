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

import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * This implementation of {@link FSLoader} is used to load data from json file.
 * <p>
 * The input file needs to have one Json per line. E.g:
 * <p>
 * {"productCategory": 5, "productId": 0}
 * {"productCategory": 4, "productId": 1}
 * {"productCategory": 5, "productId": 2}
 * {"productCategory": 5, "productId": 3}
 * </p>
 * Each line in the input file should be a valid json object which represents a
 * record and each key/value pair in that json object represents the
 * fields/value.
 * <p>
 *
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class JsonFSLoader extends FSLoader
{

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final ObjectReader reader = mapper.reader(new TypeReference<Map<String, Object>>()
  {
  });

  private static final Logger logger = LoggerFactory.getLogger(JsonFSLoader.class);

  /**
   * Extracts the fields from a json record and returns a map containing field
   * names and values
   */
  @Override
  Map<String, Object> extractFields(String line)
  {
    try {
      return reader.readValue(line);
    } catch (IOException e) {
      logger.error("Exception while extracting fields {}", e);
    }
    return null;
  }

}
