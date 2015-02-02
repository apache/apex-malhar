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

import kafka.javaapi.PartitionMetadata;

import javax.validation.constraints.Min;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

import com.datatorrent.contrib.kafka.KafkaConsumer.KafkaMeterStats;

import static com.datatorrent.contrib.kafka.KafkaConsumer.KafkaMeterStatsUtil.*;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.annotation.OperatorAnnotation;

import com.google.common.collect.Lists;
/**
 * This is a base implementation of a Kafka input operator, which consumes data from Kafka message bus.&nbsp;
 * It will be dynamically partitioned based on the upstream kafka partition.
 * <p>
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
 * </p>
 *
 * @displayName Abstract Partitionable Kafka Input
 * @category Messaging
 * @tags input operator
 *
 * @since 0.9.0
 */
@OperatorAnnotation(partitionable = true)
public abstract class AbstractPartitionableKafkaInputOperator extends AbstractKafkaInputOperator<KafkaConsumer> implements Partitioner<AbstractPartitionableKafkaInputOperator>, StatsListener
{

  // By default the partition policy is 1:1
  public PartitionStrategy strategy = PartitionStrategy.ONE_TO_ONE;

  private transient OperatorContext context = null;

  // default resource is unlimited in terms of msgs per second
  private long msgRateUpperBound = Long.MAX_VALUE;

  // default resource is unlimited in terms of bytes per second
  private long byteRateUpperBound = Long.MAX_VALUE;

  private static final Logger logger = LoggerFactory.getLogger(AbstractPartitionableKafkaInputOperator.class);

  // Store the current operator partition topology
  private transient List<PartitionInfo> currentPartitionInfo = Lists.<PartitionInfo>newLinkedList();

  // Store the current collected kafka consumer stats
  private transient Map<Integer, List<KafkaMeterStats>> kafkaStatsHolder = new HashMap<Integer, List<KafkaConsumer.KafkaMeterStats>>();

  private OffsetManager offsetManager = null;

  // Minimal interval between 2 (re)partition actions
  private long repartitionInterval = 30000L;

  // Minimal interval between checking collected stats and decide whether it needs to repartition or not.
  // And minimal interval between 2 offset updates
  private long repartitionCheckInterval = 5000L;

  private transient long lastCheckTime = 0L;

  private transient long lastRepartitionTime = 0L;

  // A list store the newly discovered partitions
  private transient List<KafkaPartition> newWaitingPartition = new LinkedList<KafkaPartition>();

  @Min(1)
  private int initialPartitionCount = 1;

  @Override
  public void partitioned(Map<Integer, Partition<AbstractPartitionableKafkaInputOperator>> partitions)
  {
    // update the last repartition time
    lastRepartitionTime = System.currentTimeMillis();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Collection<Partition<AbstractPartitionableKafkaInputOperator>> definePartitions(Collection<Partition<AbstractPartitionableKafkaInputOperator>> partitions, PartitioningContext context)
  {
    // Initialize brokers from zookeepers
    getConsumer().initBrokers();

    // check if it's the initial partition
    boolean isInitialParitition = partitions.iterator().next().getStats() == null;

    // get partition metadata for topics.
    // Whatever operator is using high-level or simple kafka consumer, the operator always create a temporary simple kafka consumer to get the metadata of the topic
    // The initial value of brokerList of the KafkaConsumer is used to retrieve the topic metadata
    Map<String, List<PartitionMetadata>> kafkaPartitions = KafkaMetadataUtil.getPartitionsForTopic(getConsumer().brokers, getConsumer().getTopic());

    // Operator partitions
    List<Partition<AbstractPartitionableKafkaInputOperator>> newPartitions = null;

    // initialize the offset
    Map<KafkaPartition, Long> initOffset = null;
    if(isInitialParitition && offsetManager !=null){
      initOffset = offsetManager.loadInitialOffsets();
      logger.info("Initial offsets: {} ", "{ " + Joiner.on(", ").useForNull("").withKeyValueSeparator(": ").join(initOffset) + " }");
    }

    switch (strategy) {

      // For the 1 to 1 mapping The framework will create number of operator partitions based on kafka topic partitions
      // Each operator partition will consume from only one kafka partition
      case ONE_TO_ONE:

        if (isInitialParitition) {
          lastRepartitionTime = System.currentTimeMillis();
          logger.info("[ONE_TO_ONE]: Initializing partition(s)");

          // initialize the number of operator partitions according to number of kafka partitions

          newPartitions = new LinkedList<Partition<AbstractPartitionableKafkaInputOperator>>();
          for (Entry<String, List<PartitionMetadata>> kp : kafkaPartitions.entrySet()) {
            String clusterId = kp.getKey();
            for (PartitionMetadata pm : kp.getValue()) {
              logger.info("[ONE_TO_ONE]: Create operator partition for cluster {}, topic {}, kafka partition {} ", clusterId, getConsumer().topic, pm.partitionId());
              Partition<AbstractPartitionableKafkaInputOperator> p = new DefaultPartition<AbstractPartitionableKafkaInputOperator>(cloneOperator());
              KafkaConsumer newConsumerForPartition = getConsumer().cloneConsumer(Sets.newHashSet(new KafkaPartition(clusterId, consumer.topic, pm.partitionId())), initOffset);
              p.getPartitionedInstance().setConsumer(newConsumerForPartition);
              PartitionInfo pif = new PartitionInfo();
              pif.kpids = Sets.<KafkaPartition>newHashSet(new KafkaPartition(clusterId, consumer.topic, pm.partitionId()));
              currentPartitionInfo.add(pif);
              newPartitions.add(p);
            }
          }
          
        }
        else if (newWaitingPartition.size() != 0) {
          // add partition for new kafka partition
          for (KafkaPartition newPartition : newWaitingPartition) {
            logger.info("[ONE_TO_ONE]: Add operator partition for cluster {}, topic {}, partition {}", newPartition.getClusterId(), getConsumer().topic, newPartition.getPartitionId());
            Partition<AbstractPartitionableKafkaInputOperator> p = new DefaultPartition<AbstractPartitionableKafkaInputOperator>(cloneOperator());
            KafkaConsumer newConsumerForPartition = getConsumer().cloneConsumer(Sets.newHashSet(newPartition));
            p.getPartitionedInstance().setConsumer(newConsumerForPartition);
            PartitionInfo pif = new PartitionInfo();
            pif.kpids = Sets.<KafkaPartition>newHashSet(new KafkaPartition(newPartition.getClusterId(), consumer.topic, newPartition.getPartitionId()));
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

        if (getConsumer() instanceof HighlevelKafkaConsumer) {
          throw new UnsupportedOperationException("[ONE_TO_MANY]: The high-level consumer is not supported for ONE_TO_MANY partition strategy.");
        }

        if (isInitialParitition) {
          lastRepartitionTime = System.currentTimeMillis();
          logger.info("[ONE_TO_MANY]: Initializing partition(s)");
          int size = initialPartitionCount;
          Set<KafkaPartition>[] kps = new Set[size];
          newPartitions = new ArrayList<Partition<AbstractPartitionableKafkaInputOperator>>(size);
          int i = 0;
          for (Entry<String, List<PartitionMetadata>> en : kafkaPartitions.entrySet()) {
            String clusterId = en.getKey();
            for (PartitionMetadata pm : en.getValue()) {
              if (kps[i % size] == null) {
                kps[i % size] = new HashSet<KafkaPartition>();
              }
              kps[i % size].add(new KafkaPartition(clusterId, consumer.topic, pm.partitionId()));
              i++;
            }
          }
          for (i = 0; i < kps.length; i++) {
            logger.info("[ONE_TO_MANY]: Create operator partition for kafka partition(s): {} ", StringUtils.join(kps[i], ", "));
            Partition<AbstractPartitionableKafkaInputOperator> p = new DefaultPartition<AbstractPartitionableKafkaInputOperator>(_cloneOperator());
            KafkaConsumer newConsumerForPartition = getConsumer().cloneConsumer(kps[i], initOffset);
            p.getPartitionedInstance().setConsumer(newConsumerForPartition);
            newPartitions.add(p);
            PartitionInfo pif = new PartitionInfo();
            pif.kpids = kps[i];
            currentPartitionInfo.add(pif);
          }

        }
        else if (newWaitingPartition.size() != 0) {

          logger.info("[ONE_TO_MANY]: Add operator partition for kafka partition(s): {} ", StringUtils.join(newWaitingPartition, ", "));
          Partition<AbstractPartitionableKafkaInputOperator> p = new DefaultPartition<AbstractPartitionableKafkaInputOperator>(_cloneOperator());
          KafkaConsumer newConsumerForPartition = getConsumer().cloneConsumer(Sets.newHashSet(newWaitingPartition));
          p.getPartitionedInstance().setConsumer(newConsumerForPartition);
          partitions.add(p);
          PartitionInfo pif = new PartitionInfo();
          pif.kpids = Sets.newHashSet(newWaitingPartition);
          currentPartitionInfo.add(pif);
          newWaitingPartition.clear();
          return partitions;
        }
        else {

          logger.info("[ONE_TO_MANY]: Repartition the operator(s) under " + msgRateUpperBound + " msgs/s and " + byteRateUpperBound + " bytes/s hard limit");
          // size of the list depends on the load and capacity of each operator
          newPartitions = new LinkedList<Partition<AbstractPartitionableKafkaInputOperator>>();

          // Use first-fit decreasing algorithm to minimize the container number and somewhat balance the partition
          // try to balance the load and minimize the number of containers with each container's load under the threshold
          // the partition based on the latest 1 minute moving average
          Map<KafkaPartition, long[]> kPIntakeRate = new HashMap<KafkaPartition, long[]>();
          // get the offset for all partitions of each consumer
          Map<KafkaPartition, Long> offsetTrack = new HashMap<KafkaPartition, Long>();
          for (Partition<AbstractPartitionableKafkaInputOperator> partition : partitions) {
            List<OperatorStats> opss = partition.getStats().getLastWindowedStats();
            if (opss == null || opss.size() == 0) {
              continue;
            }
            offsetTrack.putAll(partition.getPartitionedInstance().consumer.getCurrentOffsets());
            // Get the latest stats

            OperatorStats stat = partition.getStats().getLastWindowedStats().get(partition.getStats().getLastWindowedStats().size() - 1);
            if (stat.counters instanceof KafkaMeterStats) {
              KafkaMeterStats kms = (KafkaMeterStats) stat.counters;
              kPIntakeRate.putAll(get_1minMovingAvgParMap(kms));
            }
          }

          List<PartitionInfo> partitionInfos = firstFitDecreasingAlgo(kPIntakeRate);

          for (PartitionInfo r : partitionInfos) {
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

  private List<PartitionInfo> firstFitDecreasingAlgo(final Map<KafkaPartition, long[]> kPIntakeRate)
  {
    // (Decreasing) Sort the map by msgs/s and bytes/s in descending order
    List<Entry<KafkaPartition, long[]>> sortedMapEntry = new LinkedList<Entry<KafkaPartition, long[]>>(kPIntakeRate.entrySet());
    Collections.sort(sortedMapEntry, new Comparator<Entry<KafkaPartition, long[]>>()
    {
      @Override
      public int compare(Entry<KafkaPartition, long[]> firstEntry, Entry<KafkaPartition, long[]> secondEntry)
      {
        long[] firstPair = firstEntry.getValue();
        long[] secondPair = secondEntry.getValue();
        if (msgRateUpperBound == Long.MAX_VALUE || firstPair[0] == secondPair[0]) {
          return (int) (secondPair[1] - firstPair[1]);
        }
        else {
          return (int) (secondPair[0] - firstPair[0]);
        }
      }
    });

    // (First-fit) Look for first fit operator to assign the consumer
    // Go over all the kafka partitions and look for the right operator to assign to
    // Each record has a set of kafka partition ids and the resource left for that operator after assigned the consumers for those partitions
    List<PartitionInfo> pif = new LinkedList<PartitionInfo>();
  outer:
    for (Entry<KafkaPartition, long[]> entry : sortedMapEntry) {
      long[] resourceRequired = entry.getValue();
      for (PartitionInfo r : pif) {
        if (r.msgRateLeft > resourceRequired[0] && r.byteRateLeft > resourceRequired[1]) {
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
      nr.msgRateLeft = msgRateUpperBound == Long.MAX_VALUE ? msgRateUpperBound : msgRateUpperBound - resourceRequired[0];
      nr.byteRateLeft = byteRateUpperBound == Long.MAX_VALUE ? byteRateUpperBound : byteRateUpperBound - resourceRequired[1];
      pif.add(nr);
    }

    return pif;
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {

    Response resp = new Response();
    List<KafkaMeterStats> kstats = extractKafkaStats(stats);
    resp.repartitionRequired = needPartition(stats.getOperatorId(), kstats);
    return resp;
  }

  private void updateOffsets(List<KafkaMeterStats> kstats)
  {
    //In every partition check interval, call offsetmanager to update the offsets
    if (offsetManager != null) {
      offsetManager.updateOffsets(getOffsetsForPartitions(kstats));
    }
  }

  private List<KafkaMeterStats> extractKafkaStats(BatchedOperatorStats stats)
  {
  //preprocess the stats
    List<KafkaMeterStats> kmsList = new LinkedList<KafkaConsumer.KafkaMeterStats>();
    for (OperatorStats os : stats.getLastWindowedStats()) {
      if (os != null && os.counters instanceof KafkaMeterStats) {
        kmsList.add((KafkaMeterStats) os.counters);
      }
    }
    return kmsList;
  }

  /**
   *
   * Check whether the operator needs repartition based on reported stats
   *
   * @param stats
   * @return true if repartition is required
   * false if repartition is not required
   */
  private boolean needPartition(int opid, List<KafkaMeterStats> kstats)
  {

    long t = System.currentTimeMillis();

    if (t - lastCheckTime < repartitionCheckInterval) {
      // return false if it's within repartitionCheckInterval since last time it check the stats
      return false;
    }

    logger.debug("Use OffsetManager to update offsets");
    updateOffsets(kstats);


    if(repartitionInterval < 0){
      // if repartition is disabled
      return false;
    }

    if(t - lastRepartitionTime < repartitionInterval) {
      // return false if it's still within repartitionInterval since last (re)partition
      return false;
    }


    kafkaStatsHolder.put(opid, kstats);

    if (kafkaStatsHolder.size() != currentPartitionInfo.size() || currentPartitionInfo.size() == 0) {
      // skip checking if the operator hasn't collected all the stats from all the current partitions
      return false;
    }

    try {

      // monitor if new kafka partition added
      {
        Set<KafkaPartition> existingIds = new HashSet<KafkaPartition>();
        for (PartitionInfo pio : currentPartitionInfo) {
          existingIds.addAll(pio.kpids);
        }

        for (Entry<String, List<PartitionMetadata>> en : KafkaMetadataUtil.getPartitionsForTopic(consumer.brokers, consumer.getTopic()).entrySet()) {
          for (PartitionMetadata pm : en.getValue()) {
            KafkaPartition pa = new KafkaPartition(en.getKey(), consumer.topic, pm.partitionId());
            if(!existingIds.contains(pa)){
              newWaitingPartition.add(pa);
            }
          }
        }
        if (newWaitingPartition.size() != 0) {
          // found new kafka partition
          lastRepartitionTime = t;
          return true;
        }
      }

      if (strategy == PartitionStrategy.ONE_TO_ONE) {
        return false;
      }

      // This is expensive part and only every repartitionCheckInterval it will check existing the overall partitions
      // and see if there is more optimal solution
      // The decision is made by 2 constraint
      // Hard constraint which is upper bound overall msgs/s or bytes/s
      // Soft constraint which is more optimal solution

      boolean b = breakHardConstraint(kstats) || breakSoftConstraint();
      if (b) {
        currentPartitionInfo.clear();
        kafkaStatsHolder.clear();
      }
      return b;
    } finally {
      // update last  check time
      lastCheckTime = System.currentTimeMillis();
    }
  }

  /**
   * Check to see if there is other more optimal(less partition) partition assignment based on current statistics
   *
   * @return True if all windowed stats indicate different partition size we need to adjust the partition.
   */
  private boolean breakSoftConstraint()
  {
    if (kafkaStatsHolder.size() != currentPartitionInfo.size()) {
      return false;
    }
    int length = kafkaStatsHolder.get(kafkaStatsHolder.keySet().iterator().next()).size();
    for (int j = 0; j < length; j++) {
      Map<KafkaPartition, long[]> kPIntakeRate = new HashMap<KafkaPartition, long[]>();
      for (Integer pid : kafkaStatsHolder.keySet()) {
        kPIntakeRate.putAll(get_1minMovingAvgParMap(kafkaStatsHolder.get(pid).get(j)));
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
   *
   * @param kmss
   * @return True if all the statistics within the windows break the upper bound hard limit in msgs/s or bytes/s.
   */
  private boolean breakHardConstraint(List<KafkaMeterStats> kmss)
  {
    // Only care about the KafkaMeterStats

    // if there is no kafka meter stats at all, don't repartition
    if (kmss == null || kmss.size() == 0) {
      return false;
    }
    // if all the stats within the window have msgs/s above the upper bound threshold (hard limit)
    boolean needRP = Iterators.all(kmss.iterator(), new Predicate<KafkaMeterStats>()
    {
      @Override
      public boolean apply(KafkaMeterStats kms)
      {
        // If there are more than 1 kafka partition and the total msg/s reach the limit
        return kms.partitionStats.size() > 1 && kms.totalMsgPerSec > msgRateUpperBound;
      }
    });

    // or all the stats within the window have bytes/s above the upper bound threshold (hard limit)
    needRP = needRP || Iterators.all(kmss.iterator(), new Predicate<KafkaMeterStats>()
    {
      @Override
      public boolean apply(KafkaMeterStats kms)
      {
        //If there are more than 1 kafka partition and the total bytes/s reach the limit
        return kms.partitionStats.size() > 1 && kms.totalBytesPerSec > byteRateUpperBound;
      }
    });

    return needRP;

  }

  private final AbstractPartitionableKafkaInputOperator _cloneOperator()
  {
    AbstractPartitionableKafkaInputOperator newOp = cloneOperator();
    newOp.msgRateUpperBound = this.msgRateUpperBound;
    newOp.byteRateUpperBound = this.byteRateUpperBound;
    newOp.strategy = this.strategy;
    newOp.setMaxTuplesPerWindow(getMaxTuplesPerWindow());
    return newOp;
  }

  /**
   * Implement this method to initialize new operator instance for new partition.
   * Please carefully include all the properties you want to keep in new instance
   *
   * @return
   */
  protected abstract AbstractPartitionableKafkaInputOperator cloneOperator();

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
      context.setCounters(getConsumer().getConsumerStats());
    }
  }

  public static enum PartitionStrategy
  {
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

  public void setInitialPartitionCount(int partitionCount)
  {
    this.initialPartitionCount = partitionCount;
  }

  public int getInitialPartitionCount()
  {
    return initialPartitionCount;
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

  public void setInitialOffset(String initialOffset)
  {
    this.consumer.initialOffset = initialOffset;
  }

  public void setOffsetManager(OffsetManager offsetManager)
  {
    this.offsetManager = offsetManager;
  }

  public void setRepartitionCheckInterval(long repartitionCheckInterval)
  {
    this.repartitionCheckInterval = repartitionCheckInterval;
  }

  public long getRepartitionCheckInterval()
  {
    return repartitionCheckInterval;
  }

  public void setRepartitionInterval(long repartitionInterval)
  {
    this.repartitionInterval = repartitionInterval;
  }

  public long getRepartitionInterval()
  {
    return repartitionInterval;
  }

  //@Pattern(regexp="ONE_TO_ONE|ONE_TO_MANY|ONE_TO_MANY_HEURISTIC", flags={Flag.CASE_INSENSITIVE})
  public void setStrategy(String policy)
  {
    this.strategy = PartitionStrategy.valueOf(policy.toUpperCase());
  }

  static class PartitionInfo
  {
    Set<KafkaPartition> kpids;
    long msgRateLeft;
    long byteRateLeft;
  }

}
