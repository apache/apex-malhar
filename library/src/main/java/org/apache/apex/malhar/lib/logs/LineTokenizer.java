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
package org.apache.apex.malhar.lib.logs;

import org.apache.apex.malhar.lib.util.BaseLineTokenizer;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;

/**
 * This operator splits lines into tokens and emits token strings on the output port.
 * <p>
 * This module is a pass through. Ideal for applications like word count, or log
 * processing<br>
 * <br>
 * <b>StateFull : No, </b> tokens are processed in current window. <br>
 * <b>Partitions : Yes, </b> No state dependency in output tokens. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects String<br>
 * <b>tokens</b>: emits String<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <br>
 * <br>
 * </p>
 * @displayName Line Tokenizer
 * @category Tuple Converters
 * @tags string
 *
 * @since 0.3.3
 */
@Stateless
@OperatorAnnotation(partitionable = true)
public class LineTokenizer extends BaseLineTokenizer
{
  /**
   * The is the output port that emits string tokens.
   */
  public final transient DefaultOutputPort<String> tokens = new DefaultOutputPort<String>();

  /**
   * emits tokens on port "tokens" if tok is not empty
   *
   * @param tok
   */
  @Override
  public void processToken(String tok)
  {
    if (!tok.isEmpty()) {
      tokens.emit(tok);
    }
  }
}

