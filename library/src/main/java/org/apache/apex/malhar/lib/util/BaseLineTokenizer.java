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
package org.apache.apex.malhar.lib.util;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is an operator which consumes strings and splits them into tokens and sub-tokens.
 * <p>
 * processToken, and processSubToken are called for each token. Users should override calls backs to intercept at any level.
 * This operator is a base class for pass through operators
 * </p>
 * <p>
 * Ideal for applications like word count
 * Ports:<br>
 * <b>data</b>: expects String<br>
 * </p>
 * @displayName Base Line Tokenizer
 * @category Stream Manipulators
 * @tags string
 * @since 0.3.2
 */
public abstract class BaseLineTokenizer extends BaseOperator
{
  /**
   * This is the input port, which receives strings.
   */
  public final transient DefaultInputPort<String> data = new DefaultInputPort<String>()
  {
    /**
     * Processes tuples if validTuple returns true
     * Calls: beginProcessTokens, processTokens, endProcessTokens
     */
    @Override
    public void process(String tuple)
    {
      if (!validTuple(tuple)) { // emit error token?
        return;
      }
      beginProcessTokens();
      processTokens(tuple.split(splitBy));
      endProcessTokens();
    }
  };

  @NotNull()
  String splitBy = ";\t";
  String splitTokenBy = "";


  /**
   * getter function for splitBy
   * @return splitBy
   */
  @NotNull()
  public String getSplitBy()
  {
    return splitBy;
  }

  /**
   * getter function for splitTokeBy
   * @return splitTokenBy
   */
  public String getSplitTokenBy()
  {
    return splitTokenBy;
  }

  /**
   * getter function for splitTokeBy
   * @return splitTokenBy
   */
  public boolean hasSplitTokenBy()
  {
    return !splitTokenBy.isEmpty();
  }

  /**
   * setter function for splitBy
   * @param str
   */
  public void setSplitBy(String str)
  {
    splitBy = str;
  }

  /**
   * setter function for splitTokenBy
   * @param str
   */
  public void setSplitTokenBy(String str)
  {
    splitTokenBy = str;
  }

  /**
   * Called at the start of token processing
   */
  public void beginProcessTokens()
  {
  }

  /**
   * Called at the end of token processing
   */
  public void endProcessTokens()
  {
  }

  /**
   * Called at the start of sub token processing
   */
  public void beginProcessSubTokens()
  {
  }

  /**
   * Called at the end of sub token processing
   */
  public void endProcessSubTokens()
  {
  }

  /**
   * Processed each token one at a time in the order received if it is not null and if validToken returns true
   * @param tokens
   */
  public void processTokens(String[] tokens)
  {
    if (tokens == null) {
      return;
    }
    for (String tok: tokens) {
      if (validToken(tok)) {
        processToken(tok);
      }
    }
  }

  /**
   * Processes token
   * @param tok
   */
  public void processToken(String tok)
  {
    if (tok.isEmpty()) {
      return;
    }
    beginProcessSubTokens();
    if (splitTokenBy.isEmpty()) {
      processSubToken(tok);
    } else {
      String[] subtoks = tok.split(splitTokenBy);
      int i = 0;
      for (String subtok: subtoks) {
        if ((i == 0) && !validSubTokenKey(subtok)) { // first subtoken is the key
          break;
        }
        processSubToken(subtok);
        i++;
      }
    }
    endProcessSubTokens();
  }

  /**
   * Called for processing subtoken
   * @param subtok
   */
  public void processSubToken(String subtok)
  {
  }

  /**
   *
   * @param tuple
   * @return true is tuple is not empty
   */
  public boolean validTuple(String tuple)
  {
    return !tuple.isEmpty();
  }

  /**
   *
   * @param tok
   * @return true is tok is not empty
   */
  public boolean validToken(String tok)
  {
    return !tok.isEmpty();
  }

  /**
   *
   * @param subtok
   * @return true is subtok is not empty
   */
  public boolean validSubTokenKey(String subtok)
  {
    return !subtok.isEmpty();
  }
}
