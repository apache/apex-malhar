package com.malhartech.lib.logs;

/**
 *
 * @author davidyan
 */
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ApacheLogParseOperator extends BaseOperator {

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

    protected static String getAccessLogRegex() {
        String regex1 = "^([\\d\\.]+)";                         // Client IP
        String regex2 = " (\\S+)";                             // -
        String regex3 = " (\\S+)";                             // - 
        String regex4 = " \\[([\\w:/]+\\s[+\\-]\\d{4})\\]"; // Date
        String regex5 = " \"[A-Z]+ (.+?) HTTP/\\S+\"";                       //  url 
        String regex6 = " (\\d{3})";                           // HTTP code
        String regex7 = " (\\d+)";                     // Number of bytes
        String regex8 = " \"([^\"]+)\"";                 // Referer
        String regex9 = " \"([^\"]+)\"";                // Agent
        String regex10 = ".*"; // ignore the rest
        return regex1 + regex2 + regex3 + regex4 + regex5 + regex6 + regex7 + regex8 + regex9 + regex10;
    }

    /**
     * Parses Apache combined access log, and prints out the following <br>1.
     * Requester IP <br>2. Date of Request <br>3. Requested Page Path
     *
     * @param String line
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

        //System.out.println("PROCESSING TUPLE "+line);
        
        SimpleDateFormat accesslogDateFormat = new SimpleDateFormat(dateFormat);

        Pattern accessLogPattern = Pattern.compile(getAccessLogRegex(), Pattern.CASE_INSENSITIVE
                | Pattern.DOTALL);
        Matcher accessLogEntryMatcher;
        accessLogEntryMatcher = accessLogPattern.matcher(line);

        if (accessLogEntryMatcher.matches()) {
            //System.out.println("MATCHED!");
            ipAddr = accessLogEntryMatcher.group(1);
            requestTimeEpoch = (accesslogDateFormat.parse(accessLogEntryMatcher.group(4))).getTime();
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

