/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Pattern.Flag;
import com.datatorrent.api.Stats.OperatorStats.CustomStats;
import kafka.message.Message;


/**
 * Base Kafka Consumer class used by kafka input operator
 *
 * @since 0.9.0
 */
public abstract class KafkaConsumer
{
  protected final static String HIGHLEVEL_CONSUMER_ID_SUFFIX = "_stream_";
  
  protected final static String SIMPLE_CONSUMER_ID_SUFFIX = "_partition_";

  
  public KafkaConsumer()
  {
  }
  
  public KafkaConsumer(String topic)
  {
    this();
    this.topic = topic;
  }
  
  public KafkaConsumer(Set<String> brokerSet, String topic)
  {
    this.topic = topic;
    this.brokerSet = brokerSet;
  }

  private int consumerBuffer = 1024 * 1024;
  
  protected transient boolean isAlive = false;
  
  private transient ArrayBlockingQueue<Message> holdingBuffer;
  
  /**
   * The topic that this consumer consumes
   */
  @NotNull
  protected String topic = "default_topic";
  
  /**
   * A broker list to retrieve the metadata for the consumer
   * This property could be null
   * But it's mandatory for dynamic partition and fail-over 
   */
  @NotNull
  protected Set<String> brokerSet;
  
  
  /**
   * The initialOffset could be either earliest or latest
   * Earliest means the beginning the queue
   * Latest means the current sync point to consume the queue
   * This setting is case_insensitive
   * By default it always consume from the beginning of the queue
   */
  @Pattern(flags={Flag.CASE_INSENSITIVE}, regexp = "earliest|latest")
  protected String initialOffset = "latest";


  protected transient SnapShot statsSnapShot = new SnapShot();
  
  /**
   * This method is called in setup method of the operator
   */
  public void create(){
    holdingBuffer = new ArrayBlockingQueue<Message>(consumerBuffer);
  };

  /**
   * This method is called in the activate method of the operator
   */
  public void start(){
    isAlive = true;
    statsSnapShot.start();
  };

  /**
   * The method is called in the deactivate method of the operator
   */
  public void stop(){
    isAlive = false;
    statsSnapShot.stop();
    holdingBuffer.clear();
    _stop();
  };
  
  abstract protected void _stop();

  /**
   * This method is called in teardown method of the operator
   */
  public void teardown()
  {
    holdingBuffer.clear();
  }
  
  public boolean isAlive()
  {
    return isAlive;
  }
  
  public void setAlive(boolean isAlive)
  {
    this.isAlive = isAlive;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }
  
  public String getTopic()
  {
    return topic;
  }

  public Message pollMessage()
  {
    return holdingBuffer.poll();
  }

  public int messageSize()
  {
    return holdingBuffer.size();
  }
  
  public void setBrokerSet(Set<String> brokerSet)
  {
    this.brokerSet = brokerSet;
  }
  
  public Set<String> getBrokerSet()
  {
    return brokerSet;
  }
  
  public void setInitialOffset(String initialOffset)
  {
    this.initialOffset = initialOffset;
  }
  
  public String getInitialOffset()
  {
    return initialOffset;
  }
  
  
  final protected void putMessage(int partition, Message msg) throws InterruptedException{
    // block from receiving more message
    holdingBuffer.put(msg);
    statsSnapShot.mark(partition, msg.payloadSize());
  };
  

  protected abstract KafkaConsumer cloneConsumer(Set<Integer> partitionIds);
  
  protected abstract KafkaConsumer cloneConsumer(Set<Integer> partitionIds, Map<Integer, Long> startOffset);

  protected abstract void commitOffset();

  protected abstract Map<Integer, Long> getCurrentOffsets();
  
  public final KafkaMeterStats getConsumerStats()
  {
    return statsSnapShot.getStats();
  }
  
  static class KafkaMeterStats implements CustomStats
  {

    private static final long serialVersionUID = -2867402654990209006L;

    private Map<Integer, double[]> _1minMovingAvgPerPartition = new HashMap<Integer, double[]>();

    private double[] _1minMovingAvg = new double[]{0,0};
    
    public KafkaMeterStats()
    {
      
    }

    public KafkaMeterStats(Map<Integer, double[]> _1minMovingAvgPerPartition, double[] _1minMovingRate)
    {
      super();
      this.set_1minMovingAvgPerPartition(_1minMovingAvgPerPartition);
      this.set_1minMovingAvg(_1minMovingRate);
    }

    public Map<Integer, double[]> get_1minMovingAvgPerPartition()
    {
      return _1minMovingAvgPerPartition;
    }

    public void set_1minMovingAvgPerPartition(Map<Integer, double[]> _1minMovingAvgPerPartition)
    {
      this._1minMovingAvgPerPartition = _1minMovingAvgPerPartition;
    }

    public double[] get_1minMovingAvg()
    {
      return _1minMovingAvg;
    }

    public void set_1minMovingAvg(double[] _1minMovingAvg)
    {
      this._1minMovingAvg = _1minMovingAvg;
    }

}

  
  
  /**
   * A snapshot of consuming rate within 1 min
   */
  static class SnapShot {
    
    // msgs/s and bytes/s for each partition
    
    /**
     * 1 min total msg number for each partition 
     */
    private final Map<Integer, long[]> _1_min_msg_sum_par = new HashMap<Integer, long[]>();
    
    /**
     * 1 min total byte number for each partition
     */
    private final Map<Integer, long[]> _1_min_byte_sum_par = new HashMap<Integer, long[]>();
    
    private static int cursor = 0;
    
    /**
     * total msg for each sec, msgSec[60] is total msg for a min
     */
    private long[] msgSec = new long[61];
    
    /**
     * total bytes for each sec, bytesSec[60] is total bytes for a min
     */
    private long[] bytesSec = new long[61];
    
    private short last = 0;
    
    private ScheduledExecutorService service;

    public synchronized void moveNext()
    {
      cursor = (cursor + 1) % 60;
      msgSec[60] -= msgSec[cursor];
      bytesSec[60] -= msgSec[cursor];
      msgSec[cursor] = 0;
      bytesSec[cursor] = 0;
      for (Entry<Integer, long[]> item : _1_min_msg_sum_par.entrySet()) {
        long[] msgv = item.getValue();
        long[] bytesv = _1_min_byte_sum_par.get(item.getKey());
        msgv[60] -= msgv[cursor];
        bytesv[60] -= bytesv[cursor];
        msgv[cursor] = 0;
        bytesv[cursor] = 0;
      }
      
    }
    
    
    public void start(){
      if(service==null){
        service = Executors.newScheduledThreadPool(1);
      }
      service.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run()
        {
          moveNext();
          if(last<60)last++;
        }
      }, 1, 1, TimeUnit.SECONDS);
      
    }
    
    public void stop(){
      if(service!=null){
        service.shutdown();
      }
    }
    
    public synchronized void mark(int partition, long bytes){
      msgSec[cursor]++;
      msgSec[60]++;
      bytesSec[cursor] += bytes;
      bytesSec[60] += bytes;
      long[] msgv = _1_min_msg_sum_par.get(partition);
      long[] bytev = _1_min_byte_sum_par.get(partition);
      if(msgv == null){
        msgv = new long[61];
        bytev = new long[61];
        _1_min_msg_sum_par.put(partition, msgv);
        _1_min_byte_sum_par.put(partition, bytev); 
      }
      msgv[cursor]++;
      msgv[60]++;
      bytev[cursor] += bytes;
      bytev[60] += bytes;
    };
    
    public synchronized KafkaMeterStats getStats(){
      double[] _1minAvg = {(double)msgSec[60]/(double)last, (double)bytesSec[60]/(double)last};
      Map<Integer, double[]> _1minAvgPartition = new HashMap<Integer, double[]>();
      for (Entry<Integer, long[]> item : _1_min_msg_sum_par.entrySet()) {
        long[] msgv =item.getValue();
        long[] bytev = _1_min_byte_sum_par.get(item.getKey());
        double[] _1minAvgPar = {(double)msgv[60]/(double)last, (double)bytev[60]/(double)last};
        _1minAvgPartition.put(item.getKey(), _1minAvgPar);
      }
      return new KafkaMeterStats(_1minAvgPartition, _1minAvg);
    }
  }

}
