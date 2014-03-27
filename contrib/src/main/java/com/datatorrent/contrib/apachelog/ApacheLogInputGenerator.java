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
package com.datatorrent.contrib.apachelog;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This operator simulates the apache logs
 *
 */
public class ApacheLogInputGenerator implements InputOperator, ActivationListener<OperatorContext>
{
  private final static String[] referer = {"\"-\""};
  private final static String delimiter = ";";

  private transient Random random;
  private transient int ipAddressCount;
  private transient int agentsCount;
  private transient int urlCount;
  private transient int refererCount;
  private transient SimpleDateFormat sdf;
  private transient List<String> ipAddress;
  private transient List<String> url;
  private transient List<String> urlByteStatus;
  private transient List<String> agents;
  private transient List<Integer> bytes;
  private transient List<Integer> status;
  /**
   * The file from which to read IP Addresses
   */
  private String ipAddressFile;
  /**
   * The file from which to read URLs
   */
  private String urlFile;
  /**
   * The file from which to read Agents
   */
  private String agentFile;
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

  private void populateUrlAndBytes()
  {
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
  }

  @Override
  public void setup(OperatorContext arg0)
  {
    holdingBuffer = new ArrayBlockingQueue<String>(bufferSize);
    try {
      ipAddress = FileUtils.readLines(new File(ipAddressFile));
      urlByteStatus = FileUtils.readLines(new File(urlFile));
      agents = FileUtils.readLines(new File(agentFile));
      //removing the first url if it starts with #
      if (urlByteStatus.get(0).startsWith("#")) {
        urlByteStatus.remove(0);
      }
      populateUrlAndBytes();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    random = new Random();
    ipAddressCount = ipAddress.size();
    agentsCount = agents.size();
    urlCount = url.size();
    refererCount = referer.length;
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
          builder.append(referer[random.nextInt(refererCount)]).append(" "); // referer
          builder.append(agents.get(random.nextInt(agentsCount))).append(" "); // agent
          //LOG.debug("Adding {}", builder.toString());
          holdingBuffer.add(builder.toString());

          if (maxDelay > 0) {
            try {
              Thread.sleep(random.nextInt(maxDelay));
            }
            catch (InterruptedException e) {
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
    }
    catch (InterruptedException ex) {
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

  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  private static final Logger LOG = LoggerFactory.getLogger(ApacheLogInputGenerator.class);
}
