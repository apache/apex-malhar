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

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.apex.malhar.lib.util.BaseLineTokenizer;
import org.apache.apex.malhar.lib.util.UnifierArrayList;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.annotation.Stateless;

/**
 * This operator splits string objects into tokens and sub tokens.&nbsp;
 * This operator emits a list of tokens, as well as a map from tokens to sub tokens.
 * <p>
 * An ArrayList of all tkns are emitted on output port "tokens".
 * An ArrayList of all subtokens are emitted on port splittokens<p>
 * This module is a pass through. Ideal for applications like log processing<br>
 * <br>
 * <b>StateFull : No, </b> tokens are processed in current window. <br>
 * <b>Partitions : Yes, </b> output port unifier operator. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects String<br>
 * <b>tokens</b>: emits ArrayList&lt;String&gt;<br>
 * <b>splittokens</b>: emits ArrayList&lt;HashMap&lt;String,ArrayList&lt;String&gt;&gt;&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val1,val2,.... If not specified the value is set to null. Default is "", i.e. tokens are not split<br>
 * </p>
 * @displayName Line To Token (ArrayList)
 * @category Tuple Converters
 * @tags string, arraylist
 *
 * @since 0.3.2
 */
@Stateless
@OperatorAnnotation(partitionable = true)
public class LineToTokenArrayList extends BaseLineTokenizer
{
  protected transient ArrayList<String> tokentuple = null;
  protected transient ArrayList<HashMap<String, ArrayList<String>>> stokentuple = null;
  protected transient HashMap<String, ArrayList<String>> smap = null;
  protected transient ArrayList<String> vals = null;


  /**
   * This emits the tokens a string is broken up into.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<ArrayList<String>> tokens = new DefaultOutputPort<ArrayList<String>>()
  {
    @Override
    public Unifier<ArrayList<String>> getUnifier()
    {
      return new UnifierArrayList<String>();
    }
  };

  /**
   * This output port emits a map from tokens to sub tokens.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<ArrayList<HashMap<String, ArrayList<String>>>> splittokens =
      new DefaultOutputPort<ArrayList<HashMap<String, ArrayList<String>>>>()
  {
    @Override
    public Unifier<ArrayList<HashMap<String, ArrayList<String>>>> getUnifier()
    {
      return new UnifierArrayList<>();
    }
  };

  /**
   * sets up output tuples
   */
  @Override
  public void beginProcessTokens()
  {
    tokentuple = new ArrayList<String>();
    stokentuple = new ArrayList<HashMap<String, ArrayList<String>>>();
  }

  /**
   * adds tok to output token tuple
   * if splittoken is set starts subtoken processing
   *
   * @param tok
   */
  @Override
  public void processToken(String tok)
  {
    if (tokens.isConnected()) {
      tokentuple.add(tok);
    }
    if (splittokens.isConnected() && hasSplitTokenBy()) {
      super.processToken(tok);
    }
  }

  /**
   * sets up data for sub token processing
   */
  @Override
  public void beginProcessSubTokens()
  {
    smap = new HashMap<String, ArrayList<String>>(1);
    vals = new ArrayList<String>(4);
  }

  /**
   * Added data to subtoken tuple
   */
  @Override
  public void endProcessSubTokens()
  {
    if (!smap.isEmpty()) {
      stokentuple.add(smap);
    }

    smap = null;
    vals = null;
  }

  /**
   * Addd first subtoken to key, and rest to value ArrayList
   *
   * @param subtok
   */
  @Override
  public void processSubToken(String subtok)
  {
    if (smap.isEmpty()) {
      smap.put(subtok, vals);
    } else {
      vals.add(subtok);
    }
  }

  /**
   * emits token tuple and subtoken tuple if respective ports are connected
   */
  @Override
  public void endProcessTokens()
  {
    if (tokens.isConnected()) {
      tokens.emit(tokentuple);
      tokentuple = null;
    }

    if (splittokens.isConnected()) {
      splittokens.emit(stokentuple);
      stokentuple = null;
      smap = null;
      vals = null;
    }
  }
}
