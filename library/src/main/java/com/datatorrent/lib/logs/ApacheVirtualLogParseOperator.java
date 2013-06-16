package com.datatorrent.lib.logs;


import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * Parse Apache log lines one line at a time. Regex (getAccessLogRegex) is used as a parser. The fields extracted include i/p (outputIPAddress),
 * url (outputUrl), status code (outputStatusCode), bytes (outputBytes), referer (outputReferer), and agent (outputAgent)<p>
 * This is a pass through operator<br>
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
 * <b>Compile time checks</b>:<br>
 * <b>Run time checks</b>:<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for ApacheLogParseOperator operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>40K lines/sec</b></td><td>emits 6 output tuples per incoming line</td><td>In-bound rate and I/O are the bottlenecks</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for ApacheLogParseOperator operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th colspan=6>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i></th><th><i>outputIPAddress</i></th><th><i>outputUrl</i></th><th><i>outputStatusCode</i></th>
 * <th><i>outputBytes</i></th><th><i>outputReferer</i></th><th><i>outputAgent</i></th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>127.0.0.1 - - [04/Apr/2013:17:17:21 -0700] "GET /favicon.ico HTTP/1.1" 404 498 "-" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.43 Safari/537.31"</td>
 * <td>127.0.0.1</td><td>/favicon.ico</td><td>404</td><td>498</td><td>-</td><td>Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.43 Safari/537.31</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 *
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */

public class ApacheVirtualLogParseOperator extends BaseOperator {

    // default date format
    protected static final String dateFormat = "dd/MMM/yyyy:HH:mm:ss Z";
    public final transient DefaultInputPort<String> data = new DefaultInputPort<String>(this) {
        @Override
        public void process(String s) {
            try {
                processTuple(s);
            } catch (ParseException ex) {
                // ignore
            }
        }
    };
    public final transient DefaultOutputPort<String> outputIPAddress = new DefaultOutputPort<String>(this);
    public final transient DefaultOutputPort<String> outputUrl = new DefaultOutputPort<String>(this);
    public final transient DefaultOutputPort<String> outputStatusCode = new DefaultOutputPort<String>(this);
    public final transient DefaultOutputPort<Long> outputBytes = new DefaultOutputPort<Long>(this);
    public final transient DefaultOutputPort<String> outputReferer = new DefaultOutputPort<String>(this);
    public final transient DefaultOutputPort<String> outputAgent = new DefaultOutputPort<String>(this);
    public final transient DefaultOutputPort<String> outputServerName = new DefaultOutputPort<String>(this);
    public final transient DefaultOutputPort<Map<String, String>> outUrlStatus = new DefaultOutputPort<Map<String, String>>(this);
    public final transient DefaultOutputPort<Map<String, String>> outIpStatus = new DefaultOutputPort<Map<String, String>>(this);
    public final transient DefaultOutputPort<Map<String, Integer>> clientDataUsage = new DefaultOutputPort<Map<String, Integer>>(this);


    protected static String getAccessLogRegex() {
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
    public void processTuple(String line) throws ParseException {

        String requestTime;
        String url;
        String httpStatusCode;
        long numOfBytes;
        String referer;
        String agent;
        long requestTimeEpoch;
        String ipAddr;
        String serverName;

        //System.out.println("PROCESSING TUPLE "+line);

        SimpleDateFormat accesslogDateFormat = new SimpleDateFormat(dateFormat);

        Pattern accessLogPattern = Pattern.compile(getAccessLogRegex(), Pattern.CASE_INSENSITIVE
                | Pattern.DOTALL);
        Matcher accessLogEntryMatcher;
        accessLogEntryMatcher = accessLogPattern.matcher(line);

        if (accessLogEntryMatcher.matches()) {
            
        	  serverName = accessLogEntryMatcher.group(1);
            ipAddr = accessLogEntryMatcher.group(2);
            requestTimeEpoch = (accesslogDateFormat.parse(accessLogEntryMatcher.group(5))).getTime();
            url = accessLogEntryMatcher.group(6);
            httpStatusCode = accessLogEntryMatcher.group(7);
            numOfBytes = Long.parseLong(accessLogEntryMatcher.group(8));
            referer = accessLogEntryMatcher.group(9);
            agent = accessLogEntryMatcher.group(10);

            outputIPAddress.emit(ipAddr);
            outputUrl.emit(url);
            outputStatusCode.emit(httpStatusCode);
            outputBytes.emit(numOfBytes);
            outputReferer.emit(referer);
            outputAgent.emit(agent);
            outputServerName.emit(serverName);
            
            HashMap<String, String> urlStatus = new HashMap<String, String>();
            urlStatus.put(url, httpStatusCode);
            outUrlStatus.emit(urlStatus);
            HashMap<String, String> ipStatus = new HashMap<String, String>();
            ipStatus.put(ipAddr, httpStatusCode);
            outIpStatus.emit(ipStatus);
            HashMap<String, Integer> clientData = new HashMap<String, Integer>();
            clientData.put(ipAddr, (int) numOfBytes);
            clientDataUsage.emit(clientData);
        }
    }
}

