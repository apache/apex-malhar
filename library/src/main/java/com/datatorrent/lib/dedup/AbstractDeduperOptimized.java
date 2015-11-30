/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dedup;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.lib.bucket.AbstractBucket;
import com.datatorrent.lib.bucket.bloomFilter.BloomFilter;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * This is the optimized implementation of Deduper. It has various optimizations listed below:
 * {@link #isUseBloomFilter}: Use Bloom Filter
 * {@link #isSaveDataAtCheckpoint()}: Save data at checkpoint
 * Additionally it also has the following
 * features:
 * {@link #orderedOutput}: Whether or not to output in the same order as the input tuples
 *
 * @displayName Deduper
 * @category Deduplication
 * @tags dedupe
 *
 * @param <INPUT>
 *          type of input tuple
 * @param <OUTPUT>
 *          type of output tuple
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public abstract class AbstractDeduperOptimized<INPUT, OUTPUT> extends AbstractDeduper<INPUT, OUTPUT> implements
    Operator.CheckpointListener
{
  static int DEF_BLOOM_EXPECTED_TUPLES = 10000;
  static double DEF_BLOOM_FALSE_POS_PROB = 0.01;

  /**
   * The output port on which expired events are emitted.
   */
  public final transient DefaultOutputPort<OUTPUT> expired = new DefaultOutputPort<OUTPUT>();
  /**
   * The output port on which error events are emitted.
   */
  public final transient DefaultOutputPort<OUTPUT> error = new DefaultOutputPort<OUTPUT>();

  private boolean saveDataAtCheckpoint = false;
  protected long appWindow = 0;

  private boolean orderedOutput = false;

  /**
   * Map to hold the result of a tuple processing (unique, duplicate, expired or error) until previous
   * tuples get processed. This is used only when {@link #orderedOutput} is true.
   */
  private transient Map<INPUT, Decision> decisions;

  // Bloom filter configurations
  private boolean isUseBloomFilter = true;
  protected transient Map<Long, BloomFilter<Object>> bloomFilters;
  private int expectedNumTuples = DEF_BLOOM_EXPECTED_TUPLES;
  private double falsePositiveProb = DEF_BLOOM_FALSE_POS_PROB;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if (orderedOutput) {
      decisions = Maps.newLinkedHashMap();
    }
    if (isUseBloomFilter) {
      bloomFilters = Maps.newHashMap();
    }
  }

  /**
   * {@inheritDoc} An invalid tuple may be either expired or an error tuple. This method identifies and processes them
   * separately.
   */
  @Override
  protected void processInvalid(INPUT tuple, long bucketKey)
  {
    super.processInvalid(tuple, bucketKey);
    if (bucketKey == -1) {
      if (orderedOutput && !decisions.isEmpty()) {
        recordDecision(tuple, Decision.EXPIRED);
      } else {
        processExpired(tuple);
      }
    } else if (bucketKey == -2) {
      if (orderedOutput && !decisions.isEmpty()) {
        recordDecision(tuple, Decision.ERROR);
      } else {
        processError(tuple);
      }
    }
  }

  /**
   * {@inheritDoc} Additionally uses Bloom filter to identify if the tuple is a unique tuple directly (i.e. without
   * looking into the bucket for the tuple)
   */
  @Override
  protected void processValid(INPUT tuple, AbstractBucket<INPUT> bucket, long bucketKey)
  {
    if (isUseBloomFilter && !waitingEvents.containsKey(bucketKey)) {
      Object tupleKey = getEventKey(tuple);

      if (bloomFilters.containsKey(bucketKey)) {
        if (!bloomFilters.get(bucketKey).contains(tupleKey)) {
          bloomFilters.get(bucketKey).add(tupleKey); // Add tuple key to Bloom filter

          bucketManager.newEvent(bucketKey, tuple);
          processUnique(tuple, bucket);
          return;
        }
      }
    }
    super.processValid(tuple, bucket, bucketKey);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void processUnique(INPUT tuple, AbstractBucket<INPUT> bucket)
  {
    if (isUseBloomFilter && bucket != null) {
      // Add event to bloom filter
      if (!bloomFilters.containsKey(bucket.bucketKey)) {
        bloomFilters.put(bucket.bucketKey, new BloomFilter<Object>(expectedNumTuples, falsePositiveProb));
      }
      bloomFilters.get(bucket.bucketKey).add(getEventKey(tuple));
    }

    if (orderedOutput && !decisions.isEmpty()) {
      recordDecision(tuple, Decision.UNIQUE);
    } else {
      uniqueEvents++;
      output.emit(convert(tuple));
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void processDuplicate(INPUT tuple, AbstractBucket<INPUT> bucket)
  {
    if (orderedOutput && !decisions.isEmpty()) {
      recordDecision(tuple, Decision.DUPLICATE);
    } else {
      duplicateEvents++;
      duplicates.emit(tuple);
    }
  }

  /**
   * Processes an expired tuple
   *
   * @param tuple
   */
  protected void processExpired(INPUT tuple)
  {
    expiredEvents++;
    expired.emit(convert(tuple));
  }

  /**
   * Processes an error tuple
   *
   * @param tuple
   */
  protected void processError(INPUT tuple)
  {
    errorEvents++;
    error.emit(convert(tuple));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void processWaitingEvent(INPUT tuple, AbstractBucket<INPUT> bucket, long bucketKey)
  {
    super.processWaitingEvent(tuple, bucket, bucketKey);
    if (orderedOutput) {
      recordDecision(tuple, Decision.UNKNOWN);
    }
  }

  /**
   * Records a decision for use later. This is needed to ensure that the order of incoming tuples is maintained.
   *
   * @param tuple
   * @param d
   */
  protected void recordDecision(INPUT tuple, Decision d)
  {
    decisions.put(tuple, d);
  }

  @Override
  public void endWindow()
  {
    try {
      bucketManager.blockUntilAllRequestsServiced();
      handleIdleTime();
      Preconditions.checkArgument(waitingEvents.isEmpty(), waitingEvents.keySet());
      if (orderedOutput) {
        emitProcessedTuples();
        Preconditions.checkArgument(decisions.isEmpty(), "events pending - " + decisions.size());
      }
      appWindow++;
      bucketManager.endWindow(super.getCurrentWindow());
      /*
       * TODO: In case saveDataAtCheckpoint is set, Call endWindow for bucketManager if this is the last endWindow call
       * before checkpointing. Waiting for APEX-78
       */
      // bucketManager.endWindow(super.getCurrentWindow());
    } catch (Throwable cause) {
      DTThrowable.rethrow(cause);
    }
  }

  @Override
  public void handleIdleTime()
  {
    if (orderedOutput) {
      emitProcessedTuples();
    }
    super.handleIdleTime();
  }

  /**
   * Processes tuples for which the decision (unique / duplicate / expired / error) has been made.
   */
  protected void emitProcessedTuples()
  {
    Iterator<Entry<INPUT, Decision>> entries = decisions.entrySet().iterator();
    while (entries.hasNext()) {
      Entry<INPUT, Decision> td = entries.next();
      switch (td.getValue()) {
        case UNIQUE:
          uniqueEvents++;
          output.emit(convert(td.getKey()));
          entries.remove();
          break;
        case DUPLICATE:
          duplicateEvents++;
          duplicates.emit(td.getKey());
          entries.remove();
          break;
        case EXPIRED:
          expiredEvents++;
          expired.emit(convert(td.getKey()));
          entries.remove();
          break;
        case ERROR:
          errorEvents++;
          error.emit(convert(td.getKey()));
          entries.remove();
          break;
        default:
          /*
           * Decision for this is still UNKNOWN. Tuple is still waiting for bucket to be loaded. Break and come back
           * later in endWindow.
           */
          break;
      }
    }
  }

  @Override
  public void bucketLoaded(AbstractBucket<INPUT> loadedBucket)
  {
    if (isUseBloomFilter) {
      // Load bloom filter for this bucket
      Set<Object> keys = loadedBucket.getWrittenEventKeys();
      if (keys != null) {
        if (!bloomFilters.containsKey(loadedBucket.bucketKey)) {
          bloomFilters.put(loadedBucket.bucketKey, new BloomFilter<Object>(expectedNumTuples, falsePositiveProb));
        }
        BloomFilter<Object> bf = bloomFilters.get(loadedBucket.bucketKey);
        for (Object key : keys) {
          bf.add(key);
        }
      }
    }
    super.bucketLoaded(loadedBucket);
  }

  @Override
  public void bucketDeleted(long bucketKey)
  {
    if (isUseBloomFilter && bloomFilters != null && bloomFilters.containsKey(bucketKey)) {
      // Remove bloom filter for this bucket and all previous buckets
      Iterator<Map.Entry<Long, BloomFilter<Object>>> it = bloomFilters.entrySet().iterator();
      while (it.hasNext()) {
        long key = it.next().getKey();
        if (key <= bucketKey) {
          it.remove();
        }
      }
    }
    super.bucketDeleted(bucketKey);
  }

  @Override
  public void checkpointed(long windowId)
  {
    bucketManager.checkpointed(windowId);
  }

  @Override
  public void committed(long windowId)
  {
    bucketManager.committed(windowId);
  }

  // Getters and Setters
  public boolean isSaveDataAtCheckpoint()
  {
    return saveDataAtCheckpoint;
  }

  /**
   * If set to true, the deduper saves buckets on to persistent store only after a checkpoint Otherwise, this task is
   * done every application window.
   *
   * @param saveDataAtCheckpoint
   */
  public void setSaveDataAtCheckpoint(boolean saveDataAtCheckpoint)
  {
    this.saveDataAtCheckpoint = saveDataAtCheckpoint;
  }

  public boolean isOrderedOutput()
  {
    return orderedOutput;
  }

  /**
   * If set to true, the deduper will emit tuples in the order in which they were received. Tuples which arrived later
   * will wait for previous tuples to get processed and be emitted. If not set, the order of tuples may change as tuples
   * may be emitted out of order as and when they get processed.
   *
   * @param orderedOutput
   */
  public void setOrderedOutput(boolean orderedOutput)
  {
    this.orderedOutput = orderedOutput;
  }

  /**
   * Sets whether to use bloom filter
   *
   * @param isUseBloomFilter
   */
  public void setUseBloomFilter(boolean isUseBloomFilter)
  {
    this.isUseBloomFilter = isUseBloomFilter;
  }

  /**
   * Sets expected number of tuples to be inserted in the bloom filter to guarantee the false positive probability as
   * configured
   *
   * @param expectedNumTuples
   */
  public void setExpectedNumTuples(int expectedNumTuples)
  {
    this.expectedNumTuples = expectedNumTuples;
  }

  /**
   * Sets the false positive probability for the bloom filter.
   *
   * @param falsePositiveProb
   */
  public void setFalsePositiveProb(double falsePositiveProb)
  {
    this.falsePositiveProb = falsePositiveProb;
  }

  public boolean isUseBloomFilter()
  {
    return isUseBloomFilter;
  }

  public int getExpectedNumTuples()
  {
    return expectedNumTuples;
  }

  public double getFalsePositiveProb()
  {
    return falsePositiveProb;
  }

  /**
   * Enum for holding all possible values for a decision for a tuple
   */
  protected enum Decision
  {
    UNIQUE, DUPLICATE, EXPIRED, ERROR, UNKNOWN
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDeduperOptimized.class);
}
