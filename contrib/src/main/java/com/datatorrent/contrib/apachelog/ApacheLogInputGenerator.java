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

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * This operator simulates the apache logs
 * 
 */
public class ApacheLogInputGenerator implements InputOperator
{
  private static String[] referer = { "\"-\"" };
  private static String delimiter = ";";
  
  private transient Random random;
  private transient int ipAddressCount;
  private transient int agentsCount;
  private transient int urlCount;
  private transient int statusCount;
  private transient int bytesCount;
  private transient int refererCount;
  private transient Calendar cal;
  private transient SimpleDateFormat sdf;
  private transient List<String> ipAddress;
  private transient List<String> url;
  private transient List<String> urlBytes;
  private transient List<String> agents;
  private transient List<Integer> bytes;
  private transient List<Integer> status;
  private transient List<Integer> weights;
  private transient List<String> statusWeights;
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
   * The file from which to read Status
   */
  private String statusFile;
  /**
   * The amount of time to wait for the file to be updated.
   */
  private long delay = 1;

  /**
   * The number of lines to emit in a single window
   */
  private int numberOfTuples = 1000;

  @Override
  public void beginWindow(long arg0)
  {
    random.setSeed(System.currentTimeMillis());
  }

  @Override
  public void endWindow()
  {
  }
  
  private void populateUrlAndBytes(){
    url = new ArrayList<String>();
    bytes = new ArrayList<Integer>();
    StringTokenizer token;
    for(String str: urlBytes){
      token = new StringTokenizer(str,delimiter);
      url.add(token.nextToken().trim());
      bytes.add(Integer.parseInt(token.nextToken().trim()));
    }    
  }
  
  private void populateStatusAndWeights()
  {
   status = new ArrayList<Integer>();
   weights = new ArrayList<Integer>();
   int currentWeight = 0;
   StringTokenizer token;
   for(String str: statusWeights){
     token = new StringTokenizer(str,delimiter);
     status.add(Integer.parseInt(token.nextToken().trim()));
     if(token.hasMoreTokens()){
       currentWeight += Integer.parseInt(token.nextToken().trim()); 
     }else{
       currentWeight++;
     }
     weights.add(currentWeight);
   }
   statusCount = currentWeight;
  }
  
  private int getNextStatus(){
    int weight = random.nextInt(statusCount);
    for(int i = 0; i< weights.size(); i++){
      if(weights.get(i) >= weight){
        return status.get(i);
      }
    }
    return status.get(0);
  }

  @Override
  public void setup(OperatorContext arg0)
  {
    try {
      ipAddress = FileUtils.readLines(new File(ipAddressFile));
      urlBytes = FileUtils.readLines(new File(urlFile));
      agents = FileUtils.readLines(new File(agentFile));
      statusWeights = FileUtils.readLines(new File(statusFile));
    //removing the first url if it starts with #
      if(urlBytes.get(0).startsWith("#")){
        urlBytes.remove(0);
      }     
      populateUrlAndBytes();
    //removing the first status if it starts with #
      if(statusWeights.get(0).startsWith("#")){
        statusWeights.remove(0);
      }
      populateStatusAndWeights();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
  
    random = new Random();
    ipAddressCount = ipAddress.size();
    agentsCount = agents.size();
    urlCount = url.size();
    bytesCount = bytes.size();
    refererCount = referer.length;
    cal = Calendar.getInstance();
    sdf  = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void emitTuples()
  {
    int localCounter = numberOfTuples;
    while (localCounter > 0) {
      StringBuilder builder = new StringBuilder();
      builder.append(ipAddress.get(random.nextInt(ipAddressCount))); // url
      builder.append(" - - ");
      builder.append("[").append(sdf.format(cal.getTime())).append("] "); // timestamp
      builder.append(url.get(random.nextInt(urlCount))).append(" "); // url
      builder.append(getNextStatus()).append(" "); // status
      builder.append(bytes.get(random.nextInt(bytesCount))).append(" "); // bytes
      builder.append(referer[random.nextInt(refererCount)]).append(" "); // referer
      builder.append(agents.get(random.nextInt(agentsCount))).append(" "); // agent
      output.emit(builder.toString());
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
      }
      --localCounter;
    }

  }

  /**
   * @return the delay
   */
  public long getDelay()
  {
    return delay;
  }

  /**
   * @param delay
   *          the delay to set
   */
  public void setDelay(long delay)
  {
    this.delay = delay;
  }

  /**
   * @return the numberOfTuples
   */
  public int getNumberOfTuples()
  {
    return numberOfTuples;
  }

  /**
   * @param numberOfTuples
   *          the numberOfTuples to set
   */
  public void setNumberOfTuples(int numberOfTuples)
  {
    this.numberOfTuples = numberOfTuples;
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
   * @return the statusFile
   */
  public String getStatusFile()
  {
    return statusFile;
  }

  /**
   * @param statusFile the statusFile to set
   */
  public void setStatusFile(String statusFile)
  {
    this.statusFile = statusFile;
  }

  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

}
