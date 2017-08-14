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
package org.apache.apex.malhar.lib.io;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * Generates apache server log entries. The apache access log has the following
 * format
 *
 * %s %h %l %u %t "%r" %s %b "%{Referer}" "%{User-agent}"
 *
 * %s - server name - server0.mydomain.com:80 .......................  server9.mydomain.com:80
 * %h - The ip address of the client
 * %l - The identity of the client typically "-"
 * %u - The username of the user if HTTP authentication was used otherwise "-"
 * %t - The time the request was received e.g., [31/May/2013:08:03:46 -0700]
 * %r - The HTTP request string e.g., "GET /favicon.ico HTTP/1.1"
 * %s - The status code of the response e.g., 404
 * %b - The number of bytes in the response
 * %{Referer} - The referer web site reported by the client, "-" if there is none
 * %{User-agent} - Unique string identifying the client browser e.g.,
 * "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1468.0 Safari/537.36"
 *
 * Putting it all together a sample log string looks like :
 * --------------------------------------------------------
 * 127.0.0.1 - [31/May/2013:09:05:49 -0700] "GET /favicon.ico HTTP/1.1" 304 210 "-"
 * "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.22 (KHTML, like Gecko) Ubuntu Chromium/25.0.1364.160 Chrome/25.0.1364.160 Safari/537.22"
 *
 * @displayName Generate Random Apache Logs
 * @category Input
 * @tags log, input operator, generate
 *
 * @since 0.3.2
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class ApacheGenRandomLogs extends BaseOperator implements InputOperator
{
  /**
   * This is the output port which emits generated log strings.
   */
  public final transient DefaultOutputPort<String> outport = new DefaultOutputPort<String>();

  // server name/ip-address  random variable
  private Random rand = new Random();

  // Apache date format
  private static SimpleDateFormat apapcheDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");

  // http status codes
  private static String[] httpStatusCodes = {"100", "101", "200", "201", "202", "203", "204", "205", "206", "300", "301",
      "301", "302", "303", "304", "305", "306", "307", "400", "401", "402", "403",
      "405", "406", "407", "408", "409", "410", "411", "412", "413", "414",
      "415", "416", "417", "500", "501", "502", "503", "504", "505"};

  // possible url string formats
  private static String[] urlFormats = {
    "mydomain.com/home.php", "mydomain.com/products.php", "mydomain.com/products.php?productid=%d",
    "mydomain.com/solutions.php", "mydomain.com/solutions.php?solutionid=%d", "mydomain.com/support.php",
    "mydomain.com/about.php", "mydomain.com/contactus.php", "mydomain.com/services.php",
    "mydomain.com/services.php?serviceid=%d", "mydomain.com/partners.php", "mydomain.com/partners.php?partnerid=%d"
  };

  // browser id
  private static String[] browserIds = {
    "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:20.0) Gecko/%d Firefox/20.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0) Gecko/%d Firefox/18.0",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.7.8) Gecko/%d Fedora/1.0.4-4 Firefox/1.0.",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.10) Gecko/%d CentOS/1.5.0.10-0.1.el4.centos Firefox/1.5.0.10"
  };

  // generate server name and IP address for server
  private int genServerId()
  {
    return rand.nextInt(10);
  }

  private String genServerName(int serverId)
  {
    return new StringBuilder("server").append(new Integer(serverId).toString()).append(".mydomain.com:80").toString();
  }

  private String genIpAddress(int serverId)
  {
    return new StringBuilder().append(rand.nextInt(255))
        .append(".").append(rand.nextInt(255)).append(".").append(rand.nextInt(255))
        .append(".").append(rand.nextInt(255)).toString();
  }

  private String getTimeStamp()
  {
    return new StringBuilder("[").append(apapcheDateFormat.format(new Date())).append("]").toString();
  }

  private String genHttpCode()
  {
    return httpStatusCodes[rand.nextInt(httpStatusCodes.length)];
  }

  private String genUrl()
  {
    String format = urlFormats[rand.nextInt(urlFormats.length)];
    return String.format(format, rand.nextInt(100));
  }

  private String genBrowserId()
  {
    String format = browserIds[rand.nextInt(browserIds.length)];
    return String.format(format, rand.nextInt(100000));
  }

  // generate log string
  private String genLogString(String ipAddress, String browserId, String httpCode, String url)
  {
    // server/ipaddress
    int serverId = genServerId();
    String serverName = genServerName(serverId);
    if (ipAddress == null) {
      ipAddress = genIpAddress(serverId);
    }

    // time
    String logTime = getTimeStamp();

    // url
    if (url == null) {
      url = new StringBuilder("\"").append("GET").append(" ").append(genUrl()).append(" ").append("HTTP/1.1")
          .append("\"").toString();
    }

    // http code
    if (httpCode == null) {
      httpCode = genHttpCode();
    }

    // number of bytes
    int numBytes = rand.nextInt(4000);

    // browser id
    if (browserId == null) {
      browserId = genBrowserId();
    }

    // print
    return new StringBuilder().append(serverName).append(" ").append(ipAddress).append(" - - ").append(logTime)
        .append(" ").append(url).append(" ").append(httpCode).append(" ").append(numBytes).append(" \" \" \"")
        .append(browserId).append("\"").toString();
  }

  @Override
  public void beginWindow(long windowId)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void endWindow()
  {
    // TODO Auto-generated method stub

  }

  boolean genTuples;
  int attackInterval;

  @Override
  public void setup(OperatorContext context)
  {
    genTuples = true;
    attackInterval = rand.nextInt(10) + 1;
  }

  @Override
  public void teardown()
  {
    genTuples = false;
  }

  @Override
  public void emitTuples()
  {
    attackInterval--;
    String browserId = null;
    String ipAdddress = null;
    if (attackInterval == 0) {
      browserId = genBrowserId();
      ipAdddress = genIpAddress(rand.nextInt(10));
      attackInterval += rand.nextInt(10) + 1;
      for (int i = 0; i < rand.nextInt(3); i++) {
        outport.emit(genLogString(ipAdddress, browserId, "404", null));
      }
      String url = new StringBuilder("\"").append("GET").append(" ").append(genUrl()).append(" ").append("HTTP/1.1")
          .append("\"").toString();
      for (int i = 0; i < rand.nextInt(3); i++) {
        outport.emit(genLogString(ipAdddress, browserId, "404", url));
      }
    }
    for (int i = 0; i < rand.nextInt(100000); i++) {
      outport.emit(genLogString(ipAdddress, browserId, null, null));
    }
  }
}
