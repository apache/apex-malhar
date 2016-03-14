package com.example.myapexapp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.Size;
import javax.validation.ConstraintViolation;
import javax.validation.ValidatorFactory;
import javax.validation.Validator;
import javax.validation.Validation;

import com.datatorrent.api.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.api.Partitioner.PartitionKeys;
import com.datatorrent.api.Partitioner.PartitioningContext;

import com.datatorrent.common.util.BaseOperator;

/**
 * Simple operator to test partitioning
 */
public class TestPartition extends BaseOperator implements Partitioner<TestPartition>
{
  private static final Logger LOG = LoggerFactory.getLogger(TestPartition.class);

  private transient int id;             // operator/partition id
  private transient long curWindowId;   // current window id
  private transient long cnt;           // per-window tuple count

  @Min(1) @Max(20)
  private int nPartitions = 3;

  public final transient DefaultInputPort<Integer> in = new DefaultInputPort<Integer>() {
    @Override
    public void process(Integer tuple)
    {
      LOG.debug("{}: tuple = {}, operator id = {}", cnt, tuple, id);
      ++cnt;
    }
  };

  //public final transient DefaultOutputPort<Integer> out = new DefaultOutputPort<Integer>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);

    long appWindowId = context.getValue(context.ACTIVATION_WINDOW_ID);
    id = context.getId();
    LOG.debug("Started setup, appWindowId = {}, operator id = {}", appWindowId, id);
  }

  @Override
  public void beginWindow(long windowId)
  {
    cnt = 0;
    curWindowId = windowId;
    LOG.debug("window id = {}, operator id = {}", curWindowId, id);
  }

  @Override
  public void endWindow()
  {
    LOG.debug("window id = {}, operator id = {}, cnt = {}", curWindowId, id, cnt);
  }

  @Override
  public void partitioned(Map<Integer, Partition<TestPartition>> partitions)
  {
    //Do nothing
  }

  @Override
  public Collection<Partition<TestPartition>> definePartitions(
    Collection<Partition<TestPartition>> partitions,
    PartitioningContext context)
  {
    int oldSize = partitions.size();
    LOG.debug("partitionCount: current = {} requested = {}", oldSize, nPartitions);

    // each partition i in 0...nPartitions receives tuples divisible by i but not by any other
    // j in that range; all other tuples ignored
    //
    if (3 != nPartitions) return getPartitions(partitions, context);

    // special case of 3 partitions: All odd numbers to partition 0; even numbers divisible
    // by 4 to partition 1, those divisible by 2 but not 4 to partition 2.

    // mask used to extract discriminant from tuple hashcode
    int mask = 0x03;

    Partition<TestPartition>[] newPartitions = new Partition[] {
      new DefaultPartition<TestPartition>(new TestPartition()),
      new DefaultPartition<TestPartition>(new TestPartition()),
      new DefaultPartition<TestPartition>(new TestPartition()) };

    HashSet<Integer>[] set
      = new HashSet[] {new HashSet<>(), new HashSet<>(), new HashSet<>()};
    set[0].add(0);
    set[1].add(1);
    set[2].add(2);

    PartitionKeys[] keys = {
      new PartitionKeys(mask, set[0]),
      new PartitionKeys(mask, set[1]),
      new PartitionKeys(mask, set[2]) };

    for (int i = 0; i < 3; ++i ) {
      Partition<TestPartition> partition = newPartitions[i];
      partition.getPartitionKeys().put(in, keys[i]);
    }

    return new ArrayList<Partition<TestPartition>>(Arrays.asList(newPartitions));
  }  // definePartitions

  private Collection<Partition<TestPartition>> getPartitions(
    Collection<Partition<TestPartition>> partitions,
    PartitioningContext context)
  {
    // create array of partitions to return
    Collection<Partition<TestPartition>> result
      = new ArrayList<Partition<TestPartition>>(nPartitions);

    int mask = getMask(nPartitions);
    for (int i = 0; i < nPartitions; ++i) {
      HashSet<Integer> set = new HashSet<>();
      set.add(i);
      PartitionKeys keys = new PartitionKeys(mask, set);
      Partition partition = new DefaultPartition<TestPartition>(new TestPartition());
      partition.getPartitionKeys().put(in, keys);
    }

    return result;
  }  // getPartitions

  // return mask with bits 0..N set where N is the highest set bit of argument
  private int getMask(final int n) {
    return -1 >>> Integer.numberOfLeadingZeros(n);
  }  // getMask

  // accessors
  public int getNPartitions() { return nPartitions; }
  public void setNPartitions(int v) { nPartitions = v; }
}

