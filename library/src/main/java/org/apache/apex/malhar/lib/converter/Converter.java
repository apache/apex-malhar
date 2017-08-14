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
package org.apache.apex.malhar.lib.converter;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Operators that are converting tuples from one format to another must
 * implement this interface. Eg. Parsers or formatters , that parse data of
 * certain format and convert them to another format.
 *
 * @param <INPUT>
 * @param <OUTPUT>
 * @since 3.2.0
 */
@InterfaceStability.Evolving
public interface Converter<INPUT, OUTPUT>
{
  /**
   * Provide the implementation for converting tuples from one format to the
   * other
   *
   * @param tuple tuple of certain format
   * @return OUTPUT tuple of converted format
   */
  public OUTPUT convert(INPUT tuple);
}
