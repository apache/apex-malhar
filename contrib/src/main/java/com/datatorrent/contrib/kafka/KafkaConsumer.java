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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Pattern.Flag;
import org.apache.commons.lang3.tuple.Pair;
import com.datatorrent.api.Stats.OperatorStats.CustomStats;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
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
  
  //use yammer metrics to tick consumers' msg rate
  // msg/s and bytes/s for each partition
  protected transient final Map<Integer, Pair<Meter, Meter>> ingestRate = new HashMap<Integer, Pair<Meter,Meter>>(); 
  
  // total msg/s for this kafkaconsumer wrapper
  private transient Meter msgPerSec;
  
  // total bytes/s for this kafkaconsumer wrapper
  private transient Meter bytesPerSec;
  
  public KafkaConsumer()
  {
    brokerSet = new HashSet<String>();
    brokerSet.add("localhost:9092");
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
   * The startoffset could be either earliest or latest
   * Earliest means the beginning the queue
   * Latest means the current sync point to consume the queue
   * This setting is case_insensitive
   * By default it always consume from the beginning of the queue
   */
  @Pattern(flags={Flag.CASE_INSENSITIVE}, regexp = "earliest|latest")
  protected String startOffset = "latest";

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
    msgPerSec = Metrics.defaultRegistry().newMeter(new MetricName(getClass().getPackage().getName(), "KafkaConsumer", "MsgsPerSec"),"messages", TimeUnit.SECONDS);
    bytesPerSec = Metrics.defaultRegistry().newMeter(new MetricName(getClass().getPackage().getName(), "KafkaConsumer", "BytesPerSec"),"bytes", TimeUnit.SECONDS);
  };

  /**
   * The method is called in the deactivate method of the operator
   */
  public void stop(){
    msgPerSec.stop();
    bytesPerSec.stop();
    for (Entry<Integer, Pair<Meter, Meter>> e : ingestRate.entrySet()) {
      e.getValue().getLeft().stop();
      e.getValue().getRight().stop();
      Metrics.defaultRegistry().removeMetric(new MetricName(getClass().getPackage().getName(), "KafkaConsumerPartition" + e.getKey(), "MsgsPerSec"));
      Metrics.defaultRegistry().removeMetric(new MetricName(getClass().getPackage().getName(), "KafkaConsumerPartition" + e.getKey(), "BytesPerSec"));
    }
    Metrics.defaultRegistry().removeMetric(new MetricName(getClass().getPackage().getName(), "KafkaConsumer", "MsgsPerSec"));
    Metrics.defaultRegistry().removeMetric(new MetricName(getClass().getPackage().getName(), "KafkaConsumer", "BytesPerSec"));
    
    isAlive = false;
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
  
  public void setStartOffset(String startOffset)
  {
    this.startOffset = startOffset;
  }
  
  public String getStartOffset()
  {
    return startOffset;
  }
  
  
  final protected void putMessage(int partition, Message msg) throws InterruptedException{
    // block from receiving more message
    holdingBuffer.put(msg);
    // add stats in the per partition rate and total rate 
    Pair<Meter, Meter> par = ingestRate.get(partition);
    if(par==null){
      Meter partitionMsgsParSec = Metrics.defaultRegistry().newMeter(new MetricName(getClass().getPackage().getName(), "KafkaConsumerPartition" + partition, "MsgsPerSec"),"messages", TimeUnit.SECONDS);
      Meter partitionBytesPerSec = Metrics.defaultRegistry().newMeter(new MetricName(getClass().getPackage().getName(), "KafkaConsumerPartition" + partition, "BytesPerSec"),"messages", TimeUnit.SECONDS);
      par = Pair.of(partitionMsgsParSec, partitionBytesPerSec);
      ingestRate.put(partition, par);
    }
    msgPerSec.mark();
    bytesPerSec.mark(msg.payloadSize());
    par.getLeft().mark();
    par.getRight().mark(msg.payloadSize());
  };
  

  protected abstract KafkaConsumer cloneConsumer(Set<Integer> partitionIds);

  protected abstract void commitOffset();

  
  public final KafkaMeterStats getConsumerStats()
  {
    Map<Integer, Pair<Double, Double>> rates = new HashMap<Integer, Pair<Double, Double>>();
    for (int parid : ingestRate.keySet()) {
      rates.put(parid, Pair.of(ingestRate.get(parid).getLeft().oneMinuteRate(), ingestRate.get(parid).getRight().oneMinuteRate()));
    }
    return new KafkaMeterStats(rates, Pair.of(msgPerSec.oneMinuteRate(), bytesPerSec.oneMinuteRate()));
  }
  
  static class KafkaMeterStats implements CustomStats
  {

    private static final long serialVersionUID = -2867402654990209006L;

    private Map<Integer, Pair<Double, Double>> _1minMovingAvgPerPartition = new HashMap<Integer, Pair<Double, Double>>();

    private Pair<Double, Double> _1minMovingAvg = Pair.of(0.0, 0.0);
    
    public KafkaMeterStats()
    {
      
    }

    public KafkaMeterStats(Map<Integer, Pair<Double, Double>> _1minMovingAvgPerPartition, Pair<Double, Double> _15minMovingRate)
    {
      super();
      this._1minMovingAvgPerPartition = _1minMovingAvgPerPartition;
      this._1minMovingAvg = _15minMovingRate;
    }

    public Map<Integer, Pair<Double, Double>> get_1minMovingAvgPerPartition()
    {
      return _1minMovingAvgPerPartition;
    }

    public void set_1minMovingAvgPerPartition(Map<Integer, Pair<Double, Double>> _1minMovingAvgPerPartition)
    {
      this._1minMovingAvgPerPartition = _1minMovingAvgPerPartition;
    }

    public Pair<Double, Double> get_1minMovingAvg()
    {
      return _1minMovingAvg;
    }

    public void set_1minMovingAvg(Pair<Double, Double> _1minMovingAvg)
    {
      this._1minMovingAvg = _1minMovingAvg;
    }

  }

}
