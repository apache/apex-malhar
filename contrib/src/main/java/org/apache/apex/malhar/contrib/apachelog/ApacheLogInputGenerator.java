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
package org.apache.apex.malhar.contrib.apachelog;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.concurrent.ArrayBlockingQueue;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;

/**
 * An implementation of input operator and activation listener that simulates the apache logs.
 * <p>
 * @displayName Apache Log Input Generator
 * @category Test Bench
 * @tags apache log, generator
 * @since 0.9.4
 */
public class ApacheLogInputGenerator implements InputOperator, Operator.ActivationListener<OperatorContext>
{
  private static final String delimiter = ";";

  private transient Random random;
  private transient int ipAddressCount;
  private transient int agentsCount;
  private transient int urlCount;
  private transient int refererCount;
  private transient SimpleDateFormat sdf;
  private transient List<String> ipAddress;
  private transient List<String> url;
  private transient List<String> agents;
  private transient List<Integer> bytes;
  private transient List<Integer> status;
  private transient List<String> referers;
  /**
   * The file from which to read IP Addresses
   */
  @NotNull
  private String ipAddressFile;
  /**
   * The file from which to read URLs
   */
  @NotNull
  private String urlFile;
  /**
   * The file from which to read Agents
   */
  @NotNull
  private String agentFile;
  /**
   * The file from which to read Referers
   */
  @NotNull
  private String refererFile;
  /**
   * The max amount of time between log entries
   */
  private int maxDelay = 1000;

  /**
   * The number of lines to emit
   */
  private long numberOfTuples = Long.MAX_VALUE;
  private int bufferSize = 1024 * 1024;

  protected transient ArrayBlockingQueue<String> holdingBuffer;
  protected transient Thread thread;

  @Override
  public void beginWindow(long arg0)
  {
    random.setSeed(System.currentTimeMillis());
  }

  @Override
  public void endWindow()
  {
  }

  private List<String> readLines(String file) throws IOException
  {
    List<String> lines = new ArrayList<>();
    InputStream in;
    File f = new File(file);
    if (f.exists()) {
      in = new FileInputStream(f);
    } else {
      in = getClass().getResourceAsStream(file);
    }
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    try {
      String line;
      while ((line = br.readLine()) != null) {
        lines.add(line);
      }
    } finally {
      br.close();
    }
    return lines;
  }


  @Override
  public void setup(OperatorContext arg0)
  {
    holdingBuffer = new ArrayBlockingQueue<String>(bufferSize);
    try {
      ipAddress = readLines(ipAddressFile);
      List<String> urlByteStatus = readLines(urlFile);
      referers = readLines(refererFile);
      agents = readLines(agentFile);
      //removing the first url if it starts with #
      if (urlByteStatus.get(0).startsWith("#")) {
        urlByteStatus.remove(0);
      }

      LOG.info("Number of IP Addresses: {}", ipAddress.size());
      LOG.info("Number of URLs: {}", urlByteStatus.size());
      LOG.info("Number of Referers: {}", referers.size());
      LOG.info("Number of User Agents: {}", agents.size());
      url = new ArrayList<String>();
      bytes = new ArrayList<Integer>();
      status = new ArrayList<Integer>();
      StringTokenizer token;
      for (String str : urlByteStatus) {
        token = new StringTokenizer(str, delimiter);
        url.add(token.nextToken().trim());
        bytes.add(Integer.parseInt(token.nextToken().trim()));
        status.add(Integer.parseInt(token.nextToken().trim()));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    random = new Random();
    ipAddressCount = ipAddress.size();
    agentsCount = agents.size();
    urlCount = url.size();
    refererCount = referers.size();
    sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void emitTuples()
  {
    long ntuples = numberOfTuples;
    if (ntuples > holdingBuffer.size()) {
      ntuples = holdingBuffer.size();
    }
    for (long i = ntuples; i-- > 0;) {
      String entry = holdingBuffer.poll();
      if (entry == null) {
        break;
      }
      //LOG.debug("Emitting {}", entry);
      output.emit(entry);
    }
  }

  @Override
  public void activate(OperatorContext context)
  {
    thread = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        int i = 0;
        while (true) {
          if (i++ > numberOfTuples) {
            return;
          }
          StringBuilder builder = new StringBuilder();
          builder.append(ipAddress.get(random.nextInt(ipAddressCount))); // url
          builder.append(" - - ");
          builder.append("[").append(sdf.format(new Date())).append("] "); // timestamp
          int urlIndex = random.nextInt(urlCount);
          builder.append(url.get(urlIndex)).append(" "); // url
          builder.append(status.get(urlIndex)).append(" "); // status
          builder.append(bytes.get(urlIndex)).append(" "); // bytes
          builder.append(referers.get(random.nextInt(refererCount))).append(" "); // referer
          builder.append(agents.get(random.nextInt(agentsCount))).append(" "); // agent
          //LOG.debug("Adding {}", builder.toString());
          holdingBuffer.add(builder.toString());

          if (maxDelay > 0) {
            try {
              Thread.sleep(random.nextInt(maxDelay));
            } catch (InterruptedException e) {
              return;
            }
          }
        }
      }

    });
    thread.start();
  }

  @Override
  public void deactivate()
  {
    try {
      thread.interrupt();
      thread.join();
    } catch (InterruptedException ex) {
      // ignore
    }
  }

  /**
   * @return the delay
   */
  public long getMaxDelay()
  {
    return maxDelay;
  }

  /**
   * @param maxDelay
   * the delay to set
   */
  public void setMaxDelay(int maxDelay)
  {
    this.maxDelay = maxDelay;
  }

  /**
   * @return the numberOfTuples
   */
  public long getNumberOfTuples()
  {
    return numberOfTuples;
  }

  /**
   * @param numberOfTuples
   * the numberOfTuples to set
   */
  public void setNumberOfTuples(int numberOfTuples)
  {
    this.numberOfTuples = numberOfTuples;
  }

  public int getBufferSize()
  {
    return bufferSize;
  }

  public void setBufferSize(int bufferSize)
  {
    this.bufferSize = bufferSize;
  }

  /**
   * @return the ipAddressFile
   */
  public String getIpAddressFile()
  {
    return ipAddressFile;
  }

  /**
   * @param ipAddressFile the ipAddressFile to set
   */
  public void setIpAddressFile(String ipAddressFile)
  {
    this.ipAddressFile = ipAddressFile;
  }

  /**
   * @return the urlFile
   */
  public String getUrlFile()
  {
    return urlFile;
  }

  /**
   * @param urlFile the urlFile to set
   */
  public void setUrlFile(String urlFile)
  {
    this.urlFile = urlFile;
  }

  /**
   * @return the agentFile
   */
  public String getAgentFile()
  {
    return agentFile;
  }

  /**
   * @param agentFile the agentFile to set
   */
  public void setAgentFile(String agentFile)
  {
    this.agentFile = agentFile;
  }

  /**
   * @return the referer files
   */
  public String getRefererFile()
  {
    return refererFile;
  }

  /**
   * @param refererFile the refererFile to set
   */
  public void setRefererFile(String refererFile)
  {
    this.refererFile = refererFile;
  }

  /**
   * Output port that emits a string into DAG.
   */
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  private static final Logger LOG = LoggerFactory.getLogger(ApacheLogInputGenerator.class);
}
