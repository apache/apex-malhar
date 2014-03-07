package com.datatorrent.lib.logs;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;

/**
 * <p>
 * ApacheLogParseMapOutputOperator class.
 * </p>
 * 
 * @since 0.3.5
 */
public class ApacheLogParseMapOutputOperator extends BaseOperator
{
  /**
   * The apache log pattern regex
   */
  private String logRegex;
  /**
   * This defines the mapping from group Ids to name
   */
  @NotNull
  private Map<String,Integer> groupMap;
  private transient Pattern accessLogPattern;

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
        throw new RuntimeException("Could not parse the input string", ex);
      }
    }
  };

  /**
   * Client IP address, output port.
   */
  public final transient DefaultOutputPort<Map<String, Object>> output = new DefaultOutputPort<Map<String, Object>>();
  
  /**
   * @return the groupMap
   */
  public Map<String, Integer> getGroupMap()
  {
    return groupMap;
  }

  /**
   * @param groupMap the groupMap to set
   */
  public void setGroupMap(Map<String, Integer> groupMap)
  {
    this.groupMap = groupMap;
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
   *          the logRegex to set
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
  private String getAccessLogRegex()
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

  /**
   * 
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    if (logRegex == null) {
      setLogRegex(getAccessLogRegex());
    }
    if(groupMap == null || groupMap.size() ==0){
      throw new RuntimeException("The mapping from group Ids to names can't be null");
    }
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
    Matcher accessLogEntryMatcher = accessLogPattern.matcher(line);
    if (accessLogEntryMatcher.matches()) {
      Map<String, Object> outputMap = new HashMap<String, Object>();
      
      for(Map.Entry<String, Integer> entry : groupMap.entrySet()){
        outputMap.put(entry.getKey(),accessLogEntryMatcher.group(entry.getValue().intValue()).trim());        
      }
      output.emit(outputMap);
    }
  }
}
