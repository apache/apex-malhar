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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.regexp.Matcher;
import com.google.code.regexp.Pattern;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

/**
 * This operator parses unstructured log data into named fields.
 *
 * <p>Uses a regex with named capturing groups (http://www.regular-expressions.info/named.html) to extract portions of a string read
 * from the input port into a Map<String,String>. The capturing group name is used as the key name. The captured value is used as
 * the value.</p>
 *
 * <p>For example, given the input:
 *   <br><code>12345 "foo bar" baz;goober</code></p>
 *
 * <p>And the regular expression:
 *   <br><code>(?&lt;id&gt;\d+) "(?&lt;username&gt;[^"]+)" (?&lt;action&gt;[^;]+);(?&lt;cookie&gt;.+)</code></p>
 *
 * <p>The operator would emit a Map containing:<br>
 *  <table>
 *  <tr><th>KEY</th><th>VAL</th></tr>
 *  <tr><td>id</td><td>12345</td></tr>
 *  <tr><td>username</td><td>foo bar</td></tr>
 *  <tr><td>action</td><td>baz</td></tr>
 *  <tr><td>cookie</td><td>goober</td></tr>
 *  </table>
 *
 * <p>In the case where the regex does not match the input, nothing is emitted.</p>
 *
 * <p>Uses the named-regexp library originally from Google, but now maintained
 * by Anthony Trinh (https://github.com/tony19/named-regexp).</p>
 *
 * This is a passthrough operator<br>
 * <br>
 * <b>StateFull : No </b><br>
 * <b>Partitions : Yes</b>, No dependency among input values. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects String<br>
 * <b>output</b>: emits Map<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>regex</b>: defines the regex <br>
 *
 * @displayName Regex Match Map
 * @category Tuple Converters
 * @tags regex
 *
 * @since 1.0.5
 */
@Stateless
@OperatorAnnotation(partitionable = true)
public class RegexMatchMapOperator extends BaseOperator
{
  /**
   * The regex string
   */
  private String regex = null;
  private transient Pattern pattern = null;
  /**
   * Input log line port.
   */
  public final transient DefaultInputPort<String> data = new DefaultInputPort<String>()
  {
    @Override
    public void process(String s) throws RuntimeException
    {
      processTuple(s);
    }

  };
  /**
   * The output port which emits a map from input string section names to input string section values.
   */
  public final transient DefaultOutputPort<Map<String, Object>> output = new DefaultOutputPort<Map<String, Object>>();

  /**
   * @return the regex
   */
  public String getRegex()
  {
    return regex;
  }

  /**
   * @param regex
   * the regex to set
   */
  public void setRegex(String regex)
  {
    this.regex = regex;
    pattern = Pattern.compile(this.regex);
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if (this.regex != null) {
      pattern = Pattern.compile(this.regex);
    }
  }

  /**
   * Parses string with regex, and emits a Map corresponding to named capturing group names and their captured values.
   *
   * @param line tuple to parse
   * @throws RuntimeException
   */
  public void processTuple(String line) throws RuntimeException
  {
    if (pattern == null) {
      throw new RuntimeException("regex has not been set");
    }

    Matcher matcher = pattern.matcher(line);
    if (matcher.matches()) {
      Map<String, Object> outputMap = new HashMap<String, Object>();

      for (String key : pattern.groupNames()) {
        outputMap.put(key, matcher.group(key));
      }
      output.emit(outputMap);
    }
  }

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(RegexMatchMapOperator.class);

}
