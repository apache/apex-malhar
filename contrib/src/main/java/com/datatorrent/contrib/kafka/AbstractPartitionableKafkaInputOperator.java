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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.javaapi.PartitionMetadata;
import com.datatorrent.api.CheckpointListener;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitionable;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.contrib.kafka.KafkaConsumer.KafkaMeterStats;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

/**
 *
 * This kafka input operator will be automatically partitioned per upstream kafka partition.<br> <br>
 * This is not real dynamic partition, The partition number is decided by number of partition set for the topic in kafka.<br> <br>
 *
 * <b>Patition Strategy:</b> <br>
 * <p><b>1. _11 partition</b> Each operator partition will consume from only one kafka partition </p>
 * <p><b>2. _1NH partition</b> Each operator partition consumer from multiple kafka partition with some hard ingestion rate limit</p>
 * <p><b>3. _1NS partition</b> Each operator partition consumer from multiple kafka partition and partition number depends on real time bottle neck(soft limit)</p>
 * <br>
 * <b>Basic Algorithm:</b> <br>
 * <p>1.Pull the metadata(how many partitions) of the topic from brokerList of {@link KafkaConsumer}</p>
 * <p>2.cloneConsumer method is used to initialize the new {@link KafkaConsumer} instance for the new partition operator</p>
 * <p>3.cloneOperator method is used to initialize the new {@link AbstractPartitionableKafkaInputOperator} instance for the new partition operator</p>
 * <p>4.1 to N partition use first-fit decreasing algorithm(http://en.wikipedia.org/wiki/Bin_packing_problem) to minimize the partition operator
 * <br>
 * <br>
 * <b>Load balance:</b> refer to {@link SimpleKafkaConsumer} and {@link HighlevelKafkaConsumer} <br>
 * <b>Kafka partition failover:</b> refer to {@link SimpleKafkaConsumer} and {@link HighlevelKafkaConsumer}
 *
 * @since 0.9.0
 */
public abstract class AbstractPartitionableKafkaInputOperator extends AbstractKafkaInputOperator<KafkaConsumer> implements Partitionable<AbstractPartitionableKafkaInputOperator>, CheckpointListener, StatsListener
{
  
  // By default the partition policy is 1:1  
  public  PartitionStrategy strategy = PartitionStrategy._11;
  
  private transient OperatorContext context = null;
  
  // default resource is unlimited in terms of msg per second
  private long msgRateUpperBound = Long.MAX_VALUE;
  
  // default resource is unlimited in terms of bytes per second
  private long byteRateUpperBound = Long.MAX_VALUE;
  
  private static final Logger logger = LoggerFactory.getLogger(AbstractPartitionableKafkaInputOperator.class);
  
  // Store the current partition topology
  private transient List<PartitionInfo> currentPartitionInfo = new LinkedList<AbstractPartitionableKafkaInputOperator.PartitionInfo>();
  
  // Store the current collected kafka consumer stats
  private transient Map<Integer, List<KafkaMeterStats>> kafkaStatsHolder = new HashMap<Integer, List<KafkaConsumer.KafkaMeterStats>>();
  
  // To avoid uneven data stream, only allow at most 1 repartition in every 30 seconds
  private transient long repartitionInterval = 30000L;
  
  // Check the collect stats every 5 seconds 
  private transient long repartitionCheckInterval = 5000L;
  
  private transient long lastCheckTime = 0L;
  
  private transient long lastRepartitionTime = 0L;
  

  @Override
  public Collection<Partition<AbstractPartitionableKafkaInputOperator>> definePartitions(Collection<Partition<AbstractPartitionableKafkaInputOperator>> partitions, int incrementalCapacity)
  {

    
    // check if it's the initial partition
    boolean isInitialParitition = partitions.iterator().next().getStats() == null;
    
    // get partition metadata for topics.
    // Whatever operator is using high-level or simple kafka consumer, the operator always create a temporary simple kafka consumer to get the metadata of the topic
    // The initial value of brokerList of the KafkaConsumer is used to retrieve the topic metadata
    List<PartitionMetadata> kafkaPartitionList = KafkaMetadataUtil.getPartitionsForTopic(getConsumer().getBrokerSet(), getConsumer().getTopic());
    
    
    // Operator partitions
    List<Partition<AbstractPartitionableKafkaInputOperator>> newPartitions = null;

    switch (strategy) {
    
    // For the 1 to 1 mapping The framework will create number of operator partitions based on kafka topic partitions
    // Each operator partition will consume from only one kafka partition 
    case _11:
      
      logger.info("[_11]: Initializing partition(s)");
      
      // initialize the number of operator partitions according to number of kafka partitions

      newPartitions = new ArrayList<Partition<AbstractPartitionableKafkaInputOperator>>(kafkaPartitionList.size());
      for (int i = 0; i < kafkaPartitionList.size(); i++) {
        logger.info("Create operator partition for kafka partition: " + kafkaPartitionList.get(i).partitionId() + ", topic: " + this.getConsumer().topic);
        Partition<AbstractPartitionableKafkaInputOperator> p = new DefaultPartition<AbstractPartitionableKafkaInputOperator>(cloneOperator());
        PartitionMetadata pm = kafkaPartitionList.get(i);
        KafkaConsumer newConsumerForPartition = getConsumer().cloneConsumer(Sets.newHashSet(pm.partitionId()));
        p.getPartitionedInstance().setConsumer(newConsumerForPartition);
        PartitionInfo pif = new PartitionInfo();
        pif.kpids = Sets.newHashSet(pm.partitionId());
        currentPartitionInfo.add(pif);
        newPartitions.add(p);
      }
      break;
    // For the 1 to N static mapping The initial partition number is defined by stream application
    // Afterwards, the framework will dynamically adjust the partition and allocate consumers to as less operator partitions as it can
    //  and guarantee the overall intake rate for each operator partition is under some threshold  
    case _1NH:
      
      if(isInitialParitition){
        logger.info("[_1NH]: Initializing partition(s)");
        // Initial partition 
        int size = incrementalCapacity + 1;
        @SuppressWarnings("unchecked")
        Set<Integer>[] pIds = new Set[size];
        newPartitions = new ArrayList<Partition<AbstractPartitionableKafkaInputOperator>>(size);
        for (int i = 0; i < kafkaPartitionList.size(); i++) {
          PartitionMetadata pm = kafkaPartitionList.get(i);
          if(pIds[i%size] == null) pIds[i%size] = new HashSet<Integer>();
          pIds[i%size].add(pm.partitionId());
        }
        for (int i = 0; i < pIds.length; i++) {
          logger.info("[_1NH]: Create operator partition for kafka partition(s): " + StringUtils.join(pIds[i], ", ") + ", topic: " + this.getConsumer().topic);
          Partition<AbstractPartitionableKafkaInputOperator> p = new DefaultPartition<AbstractPartitionableKafkaInputOperator>(_cloneOperator());
          KafkaConsumer newConsumerForPartition = getConsumer().cloneConsumer(pIds[i]);
          p.getPartitionedInstance().setConsumer(newConsumerForPartition);
          newPartitions.add(p);
          PartitionInfo pif = new PartitionInfo();
          pif.kpids = pIds[i];
          currentPartitionInfo.add(pif);
        }
        
        
      } else {
        
        logger.info("[_1NH]: Repartition the operator(s) under " + msgRateUpperBound + " msg/s and " + byteRateUpperBound + " bytes/s hard limit");
        // size of the list depends on the load and capacity of each operator
        newPartitions = new LinkedList<Partition<AbstractPartitionableKafkaInputOperator>>();
        
        // Use first-fit decreasing algorithm to minimize the container number and somewhat balance the partition
        // try to balance the load and minimize the number of containers with each container's load under the threshold
        // the partition based on the latest 15 minutes moving average
        final Map<Integer, Pair<Double, Double>> kPIntakeRate = new HashMap<Integer, Pair<Double, Double>>();
        for (Partition<AbstractPartitionableKafkaInputOperator> partition : partitions) {
          List<OperatorStats> opss = partition.getStats().getLastWindowedStats();
          if(opss==null || opss.size()==0){
            continue;
          }
          // Get the latest stats
          OperatorStats stat = partition.getStats().getLastWindowedStats().get(partition.getStats().getLastWindowedStats().size() - 1);
          if (stat.customStats instanceof KafkaMeterStats) {
            KafkaMeterStats kms = (KafkaMeterStats) stat.customStats;
            for (Integer kParId : kms.get_1minMovingAvgPerPartition().keySet()) {
              kPIntakeRate.put(kParId, kms.get_1minMovingAvgPerPartition().get(kParId));
            }
          }
        }
        
        
        List<PartitionInfo> partitionInfos = firstFitDecreasingAlgo(kPIntakeRate);
        
        for (PartitionInfo r  : partitionInfos) {
          logger.info("[_1NH]: Create operator partition for kafka partition(s): " + StringUtils.join(r.kpids, ", ") + ", topic: " + this.getConsumer().topic);
          Partition<AbstractPartitionableKafkaInputOperator> p = new DefaultPartition<AbstractPartitionableKafkaInputOperator>(_cloneOperator());
          KafkaConsumer newConsumerForPartition = getConsumer().cloneConsumer(r.kpids);
          p.getPartitionedInstance().setConsumer(newConsumerForPartition);
          newPartitions.add(p);
        }
        
        currentPartitionInfo.addAll(partitionInfos);
      }
    
      break;

    case _1NS:
      throw new IllegalArgumentException("[_1NS]: Not implemented yet");
    default:
      break;
    }

    return newPartitions;
  }

  private List<PartitionInfo> firstFitDecreasingAlgo(final Map<Integer, Pair<Double, Double>> kPIntakeRate)
  {
    // (Decreasing) Sort the map by msg/s and bytes/s in descending order
    List<Entry<Integer, Pair<Double,Double>>> sortedMapEntry = new LinkedList<Entry<Integer, Pair<Double,Double>>>(kPIntakeRate.entrySet());
    Collections.sort(sortedMapEntry,new Comparator<Entry<Integer, Pair<Double,Double>>>() {
      @Override
      public int compare(Entry<Integer, Pair<Double, Double>> firstEntry, Entry<Integer, Pair<Double, Double>> secondEntry)
      {
        Pair<Double, Double> firstPair = firstEntry.getValue();
        Pair<Double, Double> secondPair = secondEntry.getValue();
        if (msgRateUpperBound == Long.MAX_VALUE || firstPair.getLeft().longValue() == secondPair.getLeft().longValue())
          return (int) (secondPair.getRight().longValue() - firstPair.getRight().longValue());
        else
          return (int)(secondPair.getLeft().longValue() - firstPair.getLeft().longValue());
      }
    });
    
    
    // (First-fit) Look for first fit operator  to assign the consumer
    // Go over all the kafka partitions and look for the right operator to assign to
    
    
    // Each record has a set of kafka partition ids and the resource left for that operator after assigned the consumers for those partitions
    List<PartitionInfo> pif = new LinkedList<PartitionInfo>();
    outer:
    for (Entry<Integer, Pair<Double,Double>> entry : sortedMapEntry) {
      Pair<Double, Double> resourceRequired = entry.getValue();
//      System.out.println("After sorted kafka partition " + entry.getKey() + "********" + resourceRequired);
      for (PartitionInfo r : pif) {
        if (r.msgRateLeft > resourceRequired.getLeft().longValue() && r.byteRateLeft > resourceRequired.getRight().longValue()) {
//          System.out.println("  Found existing record " + r.msgRateLeft + ", " + r.byteRateLeft + " for resource " + resourceRequired.getLeft().longValue()  + ", " + resourceRequired.getRight().longValue());
          // found first fit operator partition that has enough resource for this consumer
          // add consumer to the operator partition
          r.kpids.add(entry.getKey());
          // update the resource left in this partition
          r.msgRateLeft -= r.msgRateLeft == Long.MAX_VALUE ? 0 : resourceRequired.getLeft();
          r.byteRateLeft -= r.byteRateLeft == Long.MAX_VALUE ? 0 : resourceRequired.getRight();
          continue outer;
        }
      }
      // didn't find the existing "operator" to assign this consumer
      PartitionInfo nr = new PartitionInfo();
      nr.kpids = Sets.newHashSet(entry.getKey());
      nr.msgRateLeft = msgRateUpperBound == Long.MAX_VALUE ? msgRateUpperBound : msgRateUpperBound - resourceRequired.getLeft().longValue();
      nr.byteRateLeft = byteRateUpperBound == Long.MAX_VALUE ? byteRateUpperBound : byteRateUpperBound - resourceRequired.getLeft().longValue();
      pif.add(nr);
    }
    
//    System.out.println("********   Found the records " + pif.size());
    return pif;
  }
  
  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    
    Response resp = new Response();
    switch (strategy) {
    case _11:
      // You can not add/remove kafka partition at runtime. So for now, repartition is not required for 1:1 mapping
      resp.repartitionRequired = false;
      break;
    case _1NH:
      resp.repartitionRequired = needPartition(stats);
      break;
    default:
      throw new IllegalArgumentException("[_1NS]: Not implemented yet");
    }

    return resp;
  }
  
  
  /**
   * This method must be synchronized because making decision of whether the operator needs to be repartitioned
   * depends on the overall kafka stats cache and don't let the other stats reporting threads stale the cache
   * But this method should return very quickly most time because the expensive algorithm used to decide the repartition
   * only get called every 5s  
   * @param stats
   * @return
   */
  private synchronized boolean needPartition(BatchedOperatorStats stats){
    
    long t = new Date().getTime();
    
    if(t - lastRepartitionTime < repartitionInterval){
      // ignore the stats report and return immediately if it's within repartioinInterval since last repartition
      return false;
    }

    //preprocess the stats
    List<KafkaMeterStats> kmss = new LinkedList<KafkaConsumer.KafkaMeterStats>();
    for (OperatorStats os : stats.getLastWindowedStats()) {
      if (os != null && os.customStats instanceof KafkaMeterStats) {
        kmss.add((KafkaMeterStats) os.customStats);
      }
    }
    kafkaStatsHolder.put(stats.getOperatorId(), kmss);

    if(t - lastCheckTime < repartitionCheckInterval || kafkaStatsHolder.size() != currentPartitionInfo.size()){
      // skip checking if there exist more optimal partition 
      // if it's still within repartitionCheckInterval seconds since last check
      // or if the operator hasn't collected all the stats from all the current partitions
      return false;
    } 
    // This is expensive part and only every repartitionCheckInterval it will check existing the overall partitions and see if there is more optimal solution
    // The decision is made by 2 constraint
    // Hard constraint which is upperbound overall msg/s or bytes/s
    // Soft constraint which is more optimal solution
    lastCheckTime = t;
    boolean b = breakHardConstraint(kmss) || breakSoftConstraint();
    if (b) {
      currentPartitionInfo.clear();
      kafkaStatsHolder.clear();
      lastRepartitionTime = t;
    }
    return b;
  }

  private boolean breakSoftConstraint()
  {
    if(kafkaStatsHolder.size() != currentPartitionInfo.size()){
      return false;
    }
    int length = kafkaStatsHolder.get(kafkaStatsHolder.keySet().iterator().next()).size();
    for (int j = 0; j < length; j++) {
      Map<Integer, Pair<Double, Double>> kPIntakeRate = new HashMap<Integer, Pair<Double, Double>>();
      for (Integer pid : kafkaStatsHolder.keySet()) {
        kPIntakeRate.putAll(kafkaStatsHolder.get(pid).get(j).get_1minMovingAvgPerPartition());
      }
      if (kPIntakeRate.size() == 0) {
        return false;
      }
      List<PartitionInfo> partitionInfo = firstFitDecreasingAlgo(kPIntakeRate);
      if (partitionInfo.size() == 0 || partitionInfo.size() == currentPartitionInfo.size()) {
        return false;
      }
    }
    // if all windowed stats indicate different partition size we need to adjust the partition
    return true;
  }

  private boolean breakHardConstraint(List<KafkaMeterStats> kmss)
  {
    // Only care about the KafkaMeterStats

    // if there is no kafka meter stats at all, don't repartition
    if(kmss==null || kmss.size()==0){
      return false;
    }
    // if all the stats within the window have msg/s above the upper bound threshold (hard limit) 
    boolean needRP = Iterators.all(kmss.iterator(), new Predicate<KafkaMeterStats>(){
      @Override
      public boolean apply(KafkaMeterStats kms)
      {
        return kms.get_1minMovingAvg().getLeft() > msgRateUpperBound;
      }
    });
    
    // or all the stats within the window have bytes/s above the upper bound threshold (hard limit)
    needRP = needRP || Iterators.all(kmss.iterator(), new Predicate<KafkaMeterStats>(){
      @Override
      public boolean apply(KafkaMeterStats kms)
      {
        return kms.get_1minMovingAvg().getRight() > byteRateUpperBound;
      }
    });
    
    return needRP;
    
    
  }

  private final AbstractPartitionableKafkaInputOperator _cloneOperator(){
    AbstractPartitionableKafkaInputOperator newOp = cloneOperator();
    newOp.msgRateUpperBound = this.msgRateUpperBound;
    newOp.byteRateUpperBound = this.byteRateUpperBound;
    newOp.strategy = this.strategy;
    return newOp;
  }
  
  /**
   * Implement this method to initialize new operator instance for new partition.
   * Please carefully include all the properties you want to clone to new instance
   * @return
   */
  protected abstract AbstractPartitionableKafkaInputOperator cloneOperator();

  @Override
  public void checkpointed(long windowId)
  {
    // commit the kafka consummer offset
    getConsumer().commitOffset();
  }

  @Override
  public void committed(long windowId)
  {
  }
  
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    this.context = context;
  }
  
  @Override
  public void endWindow()
  {
    
    super.endWindow();
    
    if (strategy == PartitionStrategy._1NH) {
      //send the stats to AppMaster and let the AppMaster decide if it wants to repartition
      context.setCustomStats(getConsumer().getConsumerStats());
    }
  }
  
  public static enum PartitionStrategy{
    /**
     * 1 to 1 partition
     */
    _11,
    /**
     * 1 to N partition based on the overall hard throughput limits of each operator partition
     * For now it <b>only</b> support <b>simple kafka consumer</b>
     */
    _1NH,
    /**
     * 1 to N partition based on the dynamic factors (soft limits)
     * <b>NOT</b> implemented yet
     * TODO implement this later
     */
    _1NS
  }
  
  public long getMsgRateUpperBound()
  {
    return msgRateUpperBound;
  }

  public void setMsgRateUpperBound(long msgRateUpperBound)
  {
    this.msgRateUpperBound = msgRateUpperBound;
  }

  public long getByteRateUpperBound()
  {
    return byteRateUpperBound;
  }

  public void setByteRateUpperBound(long byteRateUpperBound)
  {
    this.byteRateUpperBound = byteRateUpperBound;
  }
  
  public void setStartOffset(String startOffset){
    this.consumer.startOffset = startOffset;
  }
  
  //@Pattern(regexp="_11|_1nh|_1ns", flags={Flag.CASE_INSENSITIVE})
  public void setStrategy(String policy){
    this.strategy = PartitionStrategy.valueOf(policy.toUpperCase());
  }
  
  
  static class PartitionInfo{ 
    Set<Integer> kpids;
    long msgRateLeft;
    long byteRateLeft;
  }

}




