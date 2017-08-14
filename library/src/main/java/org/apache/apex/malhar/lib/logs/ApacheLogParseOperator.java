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

import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

/**
 * Parse Apache log lines one line at a time.&nbsp;
 * Regex (getAccessLogRegex) is used as a parser.&nbsp;
 * The fields extracted include i/p (outputIPAddress), url (outputUrl),
 * status code (outputStatusCode), bytes (outputBytes), referer (outputReferer),
 * and agent (outputAgent).
 * <p>
 * This is a pass through operator<br>
 * <br>
 * <b>StateFull : No </b><br>
 * <b>Partitions : Yes</b>, No dependency among input values. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects String<br>
 * <b>outputIPAddress</b>: emits String<br>
 * <b>outputUrl</b>: emits String<br>
 * <b>outputStatusCode</b>: emits String<br>
 * <b>outputBytes</b>: emits String<br>
 * <b>outputReferer</b>: emits String<br>
 * <b>outputAgent</b>: emits String<br>
 * <br>
 * <b>Properties</b>: none<br>
 * </p>
 * @displayName Apache Log Parse
 * @category Tuple Converters
 * @tags apache, parse
 *
 * @since 0.3.3
 */
@Stateless
@OperatorAnnotation(partitionable = true)
public class ApacheLogParseOperator extends BaseOperator
{
  /**
   * This is the input port which receives apache log lines.
   */
  public final transient DefaultInputPort<String> data = new DefaultInputPort<String>()
  {
    @Override
    public void process(String s)
    {
      try {
        processTuple(s);
      } catch (ParseException ex) {
        // ignore
      }
    }
  };

  /**
   * Client IP address, output port.
   */
  public final transient DefaultOutputPort<String> outputIPAddress = new DefaultOutputPort<String>();

  /**
   * Access url port, output port.
   */
  public final transient DefaultOutputPort<String> outputUrl = new DefaultOutputPort<String>();

  /**
   * Apache status log, output port.
   */
  public final transient DefaultOutputPort<String> outputStatusCode = new DefaultOutputPort<String>();

  /**
   * Number of bytes served, output port.
   */
  public final transient DefaultOutputPort<Long> outputBytes = new DefaultOutputPort<Long>();

  /**
   * Referer name, output port.
   */
  public final transient DefaultOutputPort<String> outputReferer = new DefaultOutputPort<String>();

  /**
   * IP Agent, output port.
   */
  public final transient DefaultOutputPort<String> outputAgent = new DefaultOutputPort<String>();

  /**
   * Get apache log pattern regex.
   * @return regex string.
   */
  protected static String getAccessLogRegex()
  {
    String regex1 = "^([\\d\\.]+)"; // Client IP
    String regex2 = " (\\S+)"; // -
    String regex3 = " (\\S+)"; // -
    String regex4 = " \\[([\\w:/]+\\s[+\\-]\\d{4})\\]"; // Date
    String regex5 = " \"[A-Z]+ (.+?) HTTP/\\S+\""; // url
    String regex6 = " (\\d{3})"; // HTTP code
    String regex7 = " (\\d+)"; // Number of bytes
    String regex8 = " \"([^\"]+)\""; // Referer
    String regex9 = " \"([^\"]+)\""; // Agent
    String regex10 = ".*"; // ignore the rest
    return regex1 + regex2 + regex3 + regex4 + regex5 + regex6 + regex7
        + regex8 + regex9 + regex10;
  }

  /**
   * Parses Apache combined access log, and prints out the following <br>
   * 1. Requester IP <br>
   * 2. Date of Request <br>
   * 3. Requested Page Path
   *
   * @param line
   *          : tuple to parsee
   * @throws ParseException
   */
  public void processTuple(String line) throws ParseException
  {
    // Apapche log attaributes on each line.
    String url;
    String httpStatusCode;
    long numOfBytes;
    String referer;
    String agent;
    String ipAddr;

    // Parse each log line.
    Pattern accessLogPattern = Pattern.compile(getAccessLogRegex(),
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    Matcher accessLogEntryMatcher;
    accessLogEntryMatcher = accessLogPattern.matcher(line);

    if (accessLogEntryMatcher.matches()) {
      ipAddr = accessLogEntryMatcher.group(1);
      url = accessLogEntryMatcher.group(5);
      httpStatusCode = accessLogEntryMatcher.group(6);
      numOfBytes = Long.parseLong(accessLogEntryMatcher.group(7));
      referer = accessLogEntryMatcher.group(8);
      agent = accessLogEntryMatcher.group(9);

      outputIPAddress.emit(ipAddr);
      outputUrl.emit(url);
      outputStatusCode.emit(httpStatusCode);
      outputBytes.emit(numOfBytes);
      outputReferer.emit(referer);
      outputAgent.emit(agent);
    }
  }
}
