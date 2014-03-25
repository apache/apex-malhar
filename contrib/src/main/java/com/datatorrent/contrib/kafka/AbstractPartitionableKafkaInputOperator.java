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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.javaapi.PartitionMetadata;
import com.datatorrent.api.CheckpointListener;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
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
 * <b>Partition Strategy:</b>
 * <p><b>1. ONE_TO_ONE partition</b> Each operator partition will consume from only one kafka partition </p>
 * <p><b>2. ONE_TO_MANY partition</b> Each operator partition consumer from multiple kafka partition with some hard ingestion rate limit</p>
 * <p><b>3. ONE_TO_MANY_HEURISTIC partition</b>(Not implemented yet) Each operator partition consumer from multiple kafka partition and partition number depends on heuristic function(real time bottle neck)</p>
 * <p><b>Note:</b> ONE_TO_MANY partition only support simple kafka consumer because
 * <p>  1) high-level consumer can only balance the number of brokers it consumes from rather than the actual load from each broker</p>
 * <p>  2) high-level consumer can not reset offset once it's committed so the tuples are not replayable </p>
 * <p></p>
 * <br>
 * <br>
 * <b>Basic Algorithm:</b>
 * <p>1.Pull the metadata(how many partitions) of the topic from brokerList of {@link KafkaConsumer}</p>
 * <p>2.cloneConsumer method is used to initialize the new {@link KafkaConsumer} instance for the new partition operator</p>
 * <p>3.cloneOperator method is used to initialize the new {@link AbstractPartitionableKafkaInputOperator} instance for the new partition operator</p>
 * <p>4.ONE_TO_MANY partition use first-fit decreasing algorithm(http://en.wikipedia.org/wiki/Bin_packing_problem) to minimize the partition operator
 * <br>
 * <br>
 * <b>Load balance:</b> refer to {@link SimpleKafkaConsumer} and {@link HighlevelKafkaConsumer} <br>
 * <b>Kafka partition failover:</b> refer to {@link SimpleKafkaConsumer} and {@link HighlevelKafkaConsumer}
 * <br>
 * <br>
 * <b>Self adjust to Kafka partition change:</b>
 * <p><b>EACH</b> operator partition periodically check the leader broker(s) change which it consumes from and adjust connection without repartition</p>
 * <p><b>ONLY APPMASTER</b> operator periodically check overall kafka partition layout and add operator partition due to kafka partition add(no delete supported by kafka for now)</p>
 * <br>
 * <br>
 *
 * @since 0.9.0
 */
public abstract class AbstractPartitionableKafkaInputOperator extends AbstractKafkaInputOperator<KafkaConsumer> implements Partitioner<AbstractPartitionableKafkaInputOperator>, CheckpointListener, StatsListener
{

  // By default the partition policy is 1:1
  public  PartitionStrategy strategy = PartitionStrategy.ONE_TO_ONE;

  private transient OperatorContext context = null;

  // default resource is unlimited in terms of msgs per second
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

  private transient List<Integer> newWaitingPartition = new LinkedList<Integer>();

  @Override
  public void partitioned(Map<Integer, Partition<AbstractPartitionableKafkaInputOperator>> partitions)
  {
  }

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
    case ONE_TO_ONE:

      if (isInitialParitition) {
        lastRepartitionTime = System.currentTimeMillis();
        logger.info("[ONE_TO_ONE]: Initializing partition(s)");

        // initialize the number of operator partitions according to number of kafka partitions

        newPartitions = new ArrayList<Partition<AbstractPartitionableKafkaInputOperator>>(kafkaPartitionList.size());
        for (int i = 0; i < kafkaPartitionList.size(); i++) {
          logger.info("[ONE_TO_ONE]: Create operator partition for kafka partition: " + kafkaPartitionList.get(i).partitionId() + ", topic: " + this.getConsumer().topic);
          Partition<AbstractPartitionableKafkaInputOperator> p = new DefaultPartition<AbstractPartitionableKafkaInputOperator>(cloneOperator());
          PartitionMetadata pm = kafkaPartitionList.get(i);
          KafkaConsumer newConsumerForPartition = getConsumer().cloneConsumer(Sets.newHashSet(pm.partitionId()));
          p.getPartitionedInstance().setConsumer(newConsumerForPartition);
          PartitionInfo pif = new PartitionInfo();
          pif.kpids = Sets.newHashSet(pm.partitionId());
          currentPartitionInfo.add(pif);
          newPartitions.add(p);
        }
      } else if (newWaitingPartition.size() != 0) {
        // add partition for new kafka partition
        for (int pid : newWaitingPartition) {
          logger.info("[ONE_TO_ONE]: Add operator partition for kafka partition " + pid);
          Partition<AbstractPartitionableKafkaInputOperator> p = new DefaultPartition<AbstractPartitionableKafkaInputOperator>(cloneOperator());
          KafkaConsumer newConsumerForPartition = getConsumer().cloneConsumer(Sets.newHashSet(pid));
          p.getPartitionedInstance().setConsumer(newConsumerForPartition);
          PartitionInfo pif = new PartitionInfo();
          pif.kpids = Sets.newHashSet(pid);
          currentPartitionInfo.add(pif);
          partitions.add(p);
        }
        newWaitingPartition.clear();
        return partitions;

      }
      break;
    // For the 1 to N mapping The initial partition number is defined by stream application
    // Afterwards, the framework will dynamically adjust the partition and allocate consumers to as less operator partitions as it can
    //  and guarantee the total intake rate for each operator partition is below some threshold
    case ONE_TO_MANY:


      if(getConsumer() instanceof HighlevelKafkaConsumer){
        throw new UnsupportedOperationException("[ONE_TO_MANY]: The high-level consumer is not supported for ONE_TO_MANY partition strategy.");
      }

      if(isInitialParitition){
        lastRepartitionTime = System.currentTimeMillis();
        logger.info("[ONE_TO_MANY]: Initializing partition(s)");
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
          logger.info("[ONE_TO_MANY]: Create operator partition for kafka partition(s): " + StringUtils.join(pIds[i], ", ") + ", topic: " + this.getConsumer().topic);
          Partition<AbstractPartitionableKafkaInputOperator> p = new DefaultPartition<AbstractPartitionableKafkaInputOperator>(_cloneOperator());
          KafkaConsumer newConsumerForPartition = getConsumer().cloneConsumer(pIds[i]);
          p.getPartitionedInstance().setConsumer(newConsumerForPartition);
          newPartitions.add(p);
          PartitionInfo pif = new PartitionInfo();
          pif.kpids = pIds[i];
          currentPartitionInfo.add(pif);
        }

      } else if (newWaitingPartition.size() != 0) {

        logger.info("[ONE_TO_MANY]: Add operator partition for kafka partition(s): " + StringUtils.join(newWaitingPartition, ", ") + ", topic: " + this.getConsumer().topic);
        Partition<AbstractPartitionableKafkaInputOperator> p = new DefaultPartition<AbstractPartitionableKafkaInputOperator>(_cloneOperator());
        KafkaConsumer newConsumerForPartition = getConsumer().cloneConsumer(Sets.newHashSet(newWaitingPartition));
        p.getPartitionedInstance().setConsumer(newConsumerForPartition);
        partitions.add(p);
        PartitionInfo pif = new PartitionInfo();
        pif.kpids = Sets.newHashSet(newWaitingPartition);
        currentPartitionInfo.add(pif);
        newWaitingPartition.clear();
        return partitions;
      } else {

        logger.info("[ONE_TO_MANY]: Repartition the operator(s) under " + msgRateUpperBound + " msgs/s and " + byteRateUpperBound + " bytes/s hard limit");
        // size of the list depends on the load and capacity of each operator
        newPartitions = new LinkedList<Partition<AbstractPartitionableKafkaInputOperator>>();

        // Use first-fit decreasing algorithm to minimize the container number and somewhat balance the partition
        // try to balance the load and minimize the number of containers with each container's load under the threshold
        // the partition based on the latest 15 minutes moving average
        final Map<Integer, double[]> kPIntakeRate = new HashMap<Integer, double[]>();
        // get the offset for all partitions of each consumer
        Map<Integer, Long> offsetTrack = new HashMap<Integer, Long>();
        for (Partition<AbstractPartitionableKafkaInputOperator> partition : partitions) {
          List<OperatorStats> opss = partition.getStats().getLastWindowedStats();
          if(opss==null || opss.size()==0){
            continue;
          }
          offsetTrack.putAll(partition.getPartitionedInstance().consumer.getCurrentOffsets());
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
          logger.info("[ONE_TO_MANY]: Create operator partition for kafka partition(s): " + StringUtils.join(r.kpids, ", ") + ", topic: " + this.getConsumer().topic);
          Partition<AbstractPartitionableKafkaInputOperator> p = new DefaultPartition<AbstractPartitionableKafkaInputOperator>(_cloneOperator());
          KafkaConsumer newConsumerForPartition = getConsumer().cloneConsumer(r.kpids, offsetTrack);
          p.getPartitionedInstance().setConsumer(newConsumerForPartition);
          newPartitions.add(p);
        }

        currentPartitionInfo.addAll(partitionInfos);
      }

      break;

    case ONE_TO_MANY_HEURISTIC:
      throw new UnsupportedOperationException("[ONE_TO_MANY_HEURISTIC]: Not implemented yet");
    default:
      break;
    }

    return newPartitions;
  }

  private List<PartitionInfo> firstFitDecreasingAlgo(final Map<Integer, double[]> kPIntakeRate)
  {
    // (Decreasing) Sort the map by msgs/s and bytes/s in descending order
    List<Entry<Integer, double[]>> sortedMapEntry = new LinkedList<Entry<Integer, double[]>>(kPIntakeRate.entrySet());
    Collections.sort(sortedMapEntry,new Comparator<Entry<Integer, double[]>>() {
      @Override
      public int compare(Entry<Integer, double[]> firstEntry, Entry<Integer, double[]> secondEntry)
      {
        double[] firstPair = firstEntry.getValue();
        double[] secondPair = secondEntry.getValue();
        if (msgRateUpperBound == Long.MAX_VALUE || firstPair[0] == secondPair[0])
          return (int) (secondPair[1] - firstPair[1]);
        else
          return (int)(secondPair[0] - firstPair[0]);
      }
    });


    // (First-fit) Look for first fit operator to assign the consumer
    // Go over all the kafka partitions and look for the right operator to assign to
    // Each record has a set of kafka partition ids and the resource left for that operator after assigned the consumers for those partitions
    List<PartitionInfo> pif = new LinkedList<PartitionInfo>();
    outer:
    for (Entry<Integer, double[]> entry : sortedMapEntry) {
      double[] resourceRequired = entry.getValue();
      for (PartitionInfo r : pif) {
        if (r.msgRateLeft > (long)resourceRequired[0] && r.byteRateLeft > (long)resourceRequired[1]) {
          // found first fit operator partition that has enough resource for this consumer
          // add consumer to the operator partition
          r.kpids.add(entry.getKey());
          // update the resource left in this partition
          r.msgRateLeft -= r.msgRateLeft == Long.MAX_VALUE ? 0 : resourceRequired[0];
          r.byteRateLeft -= r.byteRateLeft == Long.MAX_VALUE ? 0 : resourceRequired[1];
          continue outer;
        }
      }
      // didn't find the existing "operator" to assign this consumer
      PartitionInfo nr = new PartitionInfo();
      nr.kpids = Sets.newHashSet(entry.getKey());
      nr.msgRateLeft = msgRateUpperBound == Long.MAX_VALUE ? msgRateUpperBound : msgRateUpperBound - (long)resourceRequired[0];
      nr.byteRateLeft = byteRateUpperBound == Long.MAX_VALUE ? byteRateUpperBound : byteRateUpperBound - (long)resourceRequired[1];
      pif.add(nr);
    }

    return pif;
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {

    Response resp = new Response();
    resp.repartitionRequired = needPartition(stats);
    return resp;
  }


  /**
   * This method must be synchronized because whether the operator needs to be repartitioned depends on the kafka stats cache
   * So it must keep it from changing by other stats reporting threads
   * But this method should return very quickly at most time because the expensive algorithm used to check the repartition condition
   * only get called every 5s
   * @param stats
   * @return
   */
  private boolean needPartition(BatchedOperatorStats stats){

    long t = System.currentTimeMillis();

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

    if(t - lastCheckTime < repartitionCheckInterval || kafkaStatsHolder.size() != currentPartitionInfo.size() || currentPartitionInfo.size()==0 ){
      // skip checking if there exist more optimal partition
      // if it's still within repartitionCheckInterval seconds since last check
      // or if the operator hasn't collected all the stats from all the current partitions
      return false;
    }

    lastCheckTime = t;

    // monitor if new kafka partition change
    {
      Set<Integer> existingIds = new HashSet<Integer>();
      for (PartitionInfo pio : currentPartitionInfo) {
        existingIds.addAll(pio.kpids);
      }

      for (PartitionMetadata metadata : KafkaMetadataUtil.getPartitionsForTopic(consumer.brokerSet, consumer.getTopic())) {
        if (!existingIds.contains(metadata.partitionId())) {
          newWaitingPartition.add(metadata.partitionId());
        }
      }
      if (newWaitingPartition.size() != 0) {
        // found new kafka partition
        lastRepartitionTime = t;
        return true;
      }
    }

    if(strategy == PartitionStrategy.ONE_TO_ONE){
      return false;
    }

    // This is expensive part and only every repartitionCheckInterval it will check existing the overall partitions and see if there is more optimal solution
    // The decision is made by 2 constraint
    // Hard constraint which is upper bound overall msgs/s or bytes/s
    // Soft constraint which is more optimal solution

    boolean b = breakHardConstraint(kmss) || breakSoftConstraint();
    if (b) {
      currentPartitionInfo.clear();
      kafkaStatsHolder.clear();
      lastRepartitionTime = t;
    }
    return b;
  }

  /**
   * Check to see if there is other more optimal(less partition) partition assignment based on current statistics
   * @return
   */
  private boolean breakSoftConstraint()
  {
    if(kafkaStatsHolder.size() != currentPartitionInfo.size()){
      return false;
    }
    int length = kafkaStatsHolder.get(kafkaStatsHolder.keySet().iterator().next()).size();
    for (int j = 0; j < length; j++) {
      Map<Integer, double[]> kPIntakeRate = new HashMap<Integer, double[]>();
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


  /**
   * Check if all the statistics within the windows break the upper bound hard limit in msgs/s or bytes/s
   * @param kmss
   * @return
   */
  private boolean breakHardConstraint(List<KafkaMeterStats> kmss)
  {
    // Only care about the KafkaMeterStats

    // if there is no kafka meter stats at all, don't repartition
    if(kmss==null || kmss.size()==0){
      return false;
    }
    // if all the stats within the window have msgs/s above the upper bound threshold (hard limit)
    boolean needRP = Iterators.all(kmss.iterator(), new Predicate<KafkaMeterStats>(){
      @Override
      public boolean apply(KafkaMeterStats kms)
      {
        return kms.get_1minMovingAvgPerPartition().size()>1 && kms.get_1minMovingAvg()[0] > msgRateUpperBound;
      }
    });

    // or all the stats within the window have bytes/s above the upper bound threshold (hard limit)
    needRP = needRP || Iterators.all(kmss.iterator(), new Predicate<KafkaMeterStats>(){
      @Override
      public boolean apply(KafkaMeterStats kms)
      {
        return kms.get_1minMovingAvgPerPartition().size()>1 && kms.get_1minMovingAvg()[1] > byteRateUpperBound;
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
   * Please carefully include all the properties you want to keep in new instance
   * @return
   */
  protected abstract AbstractPartitionableKafkaInputOperator cloneOperator();

  @Override
  public void checkpointed(long windowId)
  {
    // commit the kafka consumer offset
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

    if (strategy == PartitionStrategy.ONE_TO_MANY) {
      //send the stats to AppMaster and let the AppMaster decide if it wants to repartition
      context.setCustomStats(getConsumer().getConsumerStats());
    }
  }


  public static enum PartitionStrategy{
    /**
     * Each operator partition connect to only one kafka partition
     */
    ONE_TO_ONE,
    /**
     * Each operator consumes from several kafka partitions with overall input rate under some certain hard limit in msgs/s or bytes/s
     * For now it <b>only</b> support <b>simple kafka consumer</b>
     */
    ONE_TO_MANY,
    /**
     * 1 to N partition based on the heuristic function
     * <b>NOT</b> implemented yet
     * TODO implement this later
     */
    ONE_TO_MANY_HEURISTIC
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

  public void setInitialOffset(String initialOffset){
    this.consumer.initialOffset = initialOffset;
  }

  //@Pattern(regexp="ONE_TO_ONE|ONE_TO_MANY|ONE_TO_MANY_HEURISTIC", flags={Flag.CASE_INSENSITIVE})
  public void setStrategy(String policy){
    this.strategy = PartitionStrategy.valueOf(policy.toUpperCase());
  }


  static class PartitionInfo{
    Set<Integer> kpids;
    long msgRateLeft;
    long byteRateLeft;
  }

}
