/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.logs;

import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;

/**
 * Parse Apache log lines one line at a time. logRegex is used as a parser. The fields extracted are defined as a
 * property
 * <p>
 * This is a pass through operator<br>
 * <br>
 * <b>StateFull : No </b><br>
 * <b>Partitions : Yes</b>, No dependency among input values. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects String<br>
 * <b>output</b>: emits Map<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>logRegex</b>: defines the regex <br>
 * <b>groupMap</b>: defines the mapping from the group ids to the names <br>
 *
 */
public class ApacheLogParseMapOutputOperator extends BaseOperator
{
  /**
   * The apache log pattern regex
   */
  private String logRegex = getDefaultAccessLogRegex();
  /**
   * This defines the mapping from group Ids to name
   */
  private String[] regexGroups = getDefaultRegexGroups();
  /**
   * This is the list of extractors
   */
  private Map<String, InformationExtractor> infoExtractors = new HashMap<String, InformationExtractor>();
  private transient Pattern accessLogPattern;
  /**
   * Input log line port.
   */
  public final transient DefaultInputPort<String> data = new DefaultInputPort<String>()
  {
    @Override
    public void process(String s)
    {
      try {
        processTuple(s);
      }
      catch (ParseException ex) {
        throw new RuntimeException("Could not parse the input string", ex);
      }
    }

  };
  /**
   * Client IP address, output port.
   */
  public final transient DefaultOutputPort<Map<String, Object>> output = new DefaultOutputPort<Map<String, Object>>();

  /**
   * @return the groups
   */
  public String[] getRegexGroups()
  {
    return Arrays.copyOf(regexGroups, regexGroups.length);
  }

  /**
   * @param regexGroups the regex group list to set
   */
  public void setRegexGroups(String[] regexGroups)
  {
    this.regexGroups = Arrays.copyOf(regexGroups, regexGroups.length);
  }

  /**
   * @return the logRegex
   */
  public String getLogRegex()
  {
    return logRegex;
  }

  /**
   * @param logRegex
   * the logRegex to set
   */
  public void setLogRegex(String logRegex)
  {
    this.logRegex = logRegex;
    // Parse each log line.
    accessLogPattern = Pattern.compile(this.logRegex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  }

  /**
   * Get apache log pattern regex.
   *
   * @return regex string.
   */
  private static String getDefaultAccessLogRegex()
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
    return regex1 + regex2 + regex3 + regex4 + regex5 + regex6 + regex7 + regex8 + regex9 + regex10;
  }

  private static String[] getDefaultRegexGroups()
  {
    return new String[] {
      null, "ip", null, "userid", "time", "url", "status", "bytes", "referer", "agent"
    };
  }

  /**
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    accessLogPattern = Pattern.compile(this.logRegex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    for (Map.Entry<String, InformationExtractor> entry : infoExtractors.entrySet()) {
      entry.getValue().setup();
    }
  }

  @Override
  public void teardown()
  {
    for (Map.Entry<String, InformationExtractor> entry : infoExtractors.entrySet()) {
      entry.getValue().teardown();
    }
    super.teardown();
  }

  /**
   * Parses Apache combined access log, and prints out the following <br>
   * 1. Requester IP <br>
   * 2. Date of Request <br>
   * 3. Requested Page Path
   *
   * @param line tuple to parse
   * @throws ParseException
   */
  public void processTuple(String line) throws ParseException
  {
    Matcher accessLogEntryMatcher = accessLogPattern.matcher(line);
    if (accessLogEntryMatcher.matches()) {
      Map<String, Object> outputMap = new HashMap<String, Object>();

      for (int i = 0; i < regexGroups.length; i++) {
        if (regexGroups[i] != null) {
          String value = accessLogEntryMatcher.group(i).trim();
          outputMap.put(regexGroups[i], value);
          InformationExtractor extractor = infoExtractors.get(regexGroups[i]);
          if (extractor != null) {
            Map<String, Object> m = extractor.extractInformation(value);
            if (m != null) {
              outputMap.putAll(m);
            }
          }
        }
      }
      output.emit(outputMap);
    }
  }

  public void registerInformationExtractor(String group, InformationExtractor extractor)
  {
    infoExtractors.put(group, extractor);
  }

}
