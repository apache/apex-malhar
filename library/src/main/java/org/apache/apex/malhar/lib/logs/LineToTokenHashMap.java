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
import org.apache.apex.malhar.lib.util.UnifierHashMap;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;

/**
 * This operator splits string objects into tokens.&nbsp;
 * A key value pair is emitted where the key is the first token in an input tuple
 * and the value is a list of the other tokens in an input tuple., and emits as a HashMap where the first token.
 * <p>
 * This module is a pass through<br>
 * <br>
 * <b>StateFull : No, </b> tokens are processed in current window. <br>
 * <b>Partitions : Yes, </b> output port unifier operator. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Input port, expects String<br>
 * <b>tokens</b>: Output port, emits HashMap&lt;String, ArrayList&lt;String&gt;&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>splitby</b>: The characters used to split the line. Default is ";\t "<br>
 * <b>splittokenby</b>: The characters used to split a token into key,val1,val2,.... Default is "", i.e. tokens are not split, and key=token, val=""<br>
 * </p>
 *
 * @displayName Line To Token (HashMap)
 * @category Tuple Converters
 * @tags string, hashmap
 *
 * @since 0.3.2
 */
@Stateless
@OperatorAnnotation(partitionable = true)
public class LineToTokenHashMap extends BaseLineTokenizer
{
  /**
   * This output port emits the split strings.
   */
  public final transient DefaultOutputPort<HashMap<String, ArrayList<String>>> tokens = new DefaultOutputPort<HashMap<String, ArrayList<String>>>()
  {
    @Override
    public Unifier<HashMap<String, ArrayList<String>>> getUnifier()
    {
      return new UnifierHashMap<String, ArrayList<String>>();
    }
  };

  protected transient HashMap<String, ArrayList<String>> otuple = null;
  protected transient ArrayList<String> vals = null;
  protected transient String tok = "";

  /**
   * sets up output tuple
   */
  @Override
  public void beginProcessTokens()
  {
    otuple = new HashMap<String, ArrayList<String>>();
  }


  /**
   * clears data for subtokens
   */
  @Override
  public void beginProcessSubTokens()
  {
    vals = null;
    tok = "";
  }

  /**
   * first token is key, the rest are added to ArrayList
   * @param subtok
   */
  @Override
  public void processSubToken(String subtok)
  {
    if (vals == null) {
      tok = subtok;
      vals = new ArrayList<String>();
    } else {
      vals.add(subtok);
    }
  }


  /**
   * Adds key,Arraylist pair to output tuple.
   */
  @Override
  public void endProcessSubTokens()
  {
    addSubToken(tok, vals);
    tok = "";
    vals = null;
  }

  /**
   * If you have multiple subtokens with same value, override and aggregate the values and then put
   * in the map
   * @param stok subtoken
   * @param svals subtoken val list
   */
  public void addSubToken(String stok, ArrayList<String> svals)
  {
    otuple.put(stok, svals);
  }

  /**
   * emits output tuple
   */
  @Override
  public void endProcessTokens()
  {
    if (tokens.isConnected()) {
      tokens.emit(otuple);
      otuple = null;
    }
  }
}
