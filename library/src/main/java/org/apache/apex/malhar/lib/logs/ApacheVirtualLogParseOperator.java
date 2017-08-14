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

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

/**
 * Parse Apache log lines one line at a time.&nbsp;
 * Regex (getAccessLogRegex) is used as a parser.&nbsp;
 * The fields extracted include i/p (outputIPAddress), url (outputUrl),
 * status code (outputStatusCode), bytes (outputBytes), referer (outputReferer),
 * and agent (outputAgent).&nbsp;Each of the fields are emitted to a separate output port.
 * <p>
 * Please refer to docs for {@link org.apache.apex.malhar.lib.logs.ApacheLogParseOperator} documentation.
 * More output ports in this operator.
 * </p>
 * @displayName Apache Virtual Log Parse
 * @category Tuple Converters
 * @tags apache, parse
 *
 * @since 0.3.2
 */
@Stateless
public class ApacheVirtualLogParseOperator extends BaseOperator
{

  // default date format
  protected static final String dateFormat = "dd/MMM/yyyy:HH:mm:ss Z";
  /**
   *
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
   * This output port emits the IPAddresses contained in log file lines.
   */
  public final transient DefaultOutputPort<String> outputIPAddress = new DefaultOutputPort<String>();
  /**
   * This output port emits URLs contained in log file lines.
   */
  public final transient DefaultOutputPort<String> outputUrl = new DefaultOutputPort<String>();
  /**
   * This output port emits status codes contained in log file lines.
   */
  public final transient DefaultOutputPort<String> outputStatusCode = new DefaultOutputPort<String>();
  /**
   * This output pot emits a Map for each log file line,
   * which contains all the information extracted from the log file line.
   */
  public final transient DefaultOutputPort<Map<String, Integer>> outputBytes =
      new DefaultOutputPort<Map<String, Integer>>();
  /**
   * This output port emits the referers contained in the log file lines.
   */
  public final transient DefaultOutputPort<String> outputReferer = new DefaultOutputPort<String>();
  /**
   * This output port emits the agents contained in the log file lines.
   */
  public final transient DefaultOutputPort<String> outputAgent = new DefaultOutputPort<String>();
  /**
   * This output port emits the servernames contained in the log file lines.
   */
  public final transient DefaultOutputPort<String> outputServerName = new DefaultOutputPort<String>();
  /**
   * This output port emits the servernames contained in the log file lines.
   */
  public final transient DefaultOutputPort<String> outputServerName1 = new DefaultOutputPort<String>();
  /**
   * This output port emits the status codes corresponding to each url in a log file line.
   */
  public final transient DefaultOutputPort<Map<String, String>> outUrlStatus =
      new DefaultOutputPort<Map<String, String>>();
  /**
   * This output port emits the status associated with each server in a log file line.
   */
  public final transient DefaultOutputPort<Map<String, String>> outServerStatus =
      new DefaultOutputPort<Map<String, String>>();
  /**
   * This output port emits client data usage contained in log file lines.
   */
  public final transient DefaultOutputPort<Integer> clientDataUsage = new DefaultOutputPort<Integer>();
  /**
   * This output port emits the view counts contained in log file lines.
   */
  public final transient DefaultOutputPort<Integer> viewCount = new DefaultOutputPort<Integer>();

  protected static String getAccessLogRegex()
  {
    String regex0 = "^([^\"]+)";
    String regex1 = " ([\\d\\.]+)";                         // Client IP
    String regex2 = " (\\S+)";                             // -
    String regex3 = " (\\S+)";                             // -
    String regex4 = " \\[([\\w:/]+\\s[+\\-]\\d{4})\\]"; // Date
    String regex5 = " \"[A-Z]+ (.+?) HTTP/\\S+\"";                       //  url
    String regex6 = " (\\d{3})";                           // HTTP code
    String regex7 = " (\\d+)";                     // Number of bytes
    String regex8 = " \"([^\"]+)\"";                 // Referer
    String regex9 = " \"([^\"]+)\"";                // Agent
    String regex10 = ".*"; // ignore the rest
    return regex0 + regex1 + regex2 + regex3 + regex4 + regex5 + regex6 + regex7 + regex8 + regex9 + regex10;
  }

  /**
   * Parses Apache combined access log, and prints out the following <br>1.
   * Requester IP <br>2. Date of Request <br>3. Requested Page Path
   *
   * @param line : tuple to parsee
   * @throws ParseException
   * @throws IOException
   */
  public void processTuple(String line) throws ParseException
  {

    // Apache log properties.
    String url;
    String httpStatusCode;
    long numOfBytes;
    String referer;
    String agent;
    String ipAddr;
    String serverName;

    // Parser log.
    Pattern accessLogPattern = Pattern.compile(getAccessLogRegex(), Pattern.CASE_INSENSITIVE
        | Pattern.DOTALL);
    Matcher accessLogEntryMatcher;
    accessLogEntryMatcher = accessLogPattern.matcher(line);

    if (accessLogEntryMatcher.matches()) {

      serverName = accessLogEntryMatcher.group(1);
      ipAddr = accessLogEntryMatcher.group(2);
      url = accessLogEntryMatcher.group(6);
      httpStatusCode = accessLogEntryMatcher.group(7);
      numOfBytes = Long.parseLong(accessLogEntryMatcher.group(8));
      referer = accessLogEntryMatcher.group(9);
      agent = accessLogEntryMatcher.group(10);

      outputIPAddress.emit(ipAddr);
      outputUrl.emit(url);
      outputStatusCode.emit(httpStatusCode);
      Map<String, Integer> ipdata = new HashMap<String, Integer>();
      ipdata.put(ipAddr, (int)numOfBytes);
      outputBytes.emit(ipdata);
      outputReferer.emit(referer);
      outputAgent.emit(agent);
      outputServerName.emit(serverName);
      outputServerName1.emit(serverName);

      HashMap<String, String> urlStatus = new HashMap<String, String>();
      urlStatus.put(url, httpStatusCode);
      outUrlStatus.emit(urlStatus);

      HashMap<String, String> serverStatus = new HashMap<String, String>();
      serverStatus.put(serverName, httpStatusCode);
      outServerStatus.emit(serverStatus);

      clientDataUsage.emit((int)numOfBytes);
      viewCount.emit(new Integer(1));
    }
  }
}

