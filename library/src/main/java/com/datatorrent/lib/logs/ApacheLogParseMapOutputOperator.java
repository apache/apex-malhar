package com.datatorrent.lib.logs;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

public class ApacheLogParseMapOutputOperator extends BaseOperator
{
  /**
   * Input log line port.
   */
  public final transient DefaultInputPort<String> data = new DefaultInputPort<String>() {
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
  public final transient DefaultOutputPort<Map<String, Object>> output = new DefaultOutputPort<Map<String, Object>>();

  /**
   * Get apache log pattern regex.
   * 
   * @return regex string.
   */
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
   * Parses Apache combined access log, and prints out the following <br>
   * 1. Requester IP <br>
   * 2. Date of Request <br>
   * 3. Requested Page Path
   * 
   * @param line
   *          : tuple to parsee
   * @throws ParseException
   * @throws IOException
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
    String serverName;

    // Parse each log line.
    Pattern accessLogPattern = Pattern.compile(getAccessLogRegex(), Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    Matcher accessLogEntryMatcher;
    accessLogEntryMatcher = accessLogPattern.matcher(line);

   // System.out.println("Before MATCHED!");
    if (accessLogEntryMatcher.matches()) {
      //System.out.println("MATCHED!");
      serverName = accessLogEntryMatcher.group(1);
      ipAddr = accessLogEntryMatcher.group(2);
      url = accessLogEntryMatcher.group(6);
      httpStatusCode = accessLogEntryMatcher.group(7);
      numOfBytes = Long.parseLong(accessLogEntryMatcher.group(8));
      referer = accessLogEntryMatcher.group(9);
      agent = accessLogEntryMatcher.group(10);
      
      Map<String, Object> outputMap = new HashMap<String, Object>();

      outputMap.put("serverName",serverName);
      outputMap.put("ipAddr", ipAddr);
      outputMap.put("url", url);
      outputMap.put("status", httpStatusCode);
      outputMap.put("bytes", numOfBytes);
      outputMap.put("referer", referer);
      outputMap.put("agent", agent);
      output.emit(outputMap);
    }
  }
}
