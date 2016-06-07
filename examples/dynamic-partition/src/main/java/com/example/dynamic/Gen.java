package com.example.dynamic;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * Operator that dynamically partitions itself after 500 tuples have been emitted
 */
public class Gen extends BaseOperator implements InputOperator, Partitioner<Gen>, StatsListener
{
  private static final Logger LOG = LoggerFactory.getLogger(Gen.class);

  private static final int MAX_PARTITIONS = 4;    // maximum number of partitions

  private int partitions = 2;                     // initial number of partitions

  @NotNull
  private int numTuples;                          // number of tuples to emit per window

  private transient int count = 0;

  public final transient DefaultOutputPort<Double> out = new DefaultOutputPort<Double>();

  @Override
  public void partitioned(Map<Integer, Partition<Gen>> map)
  {
    if (partitions != map.size()) {
      String msg = String.format("partitions = %d, map.size = %d%n", partitions, map.size());
      throw new RuntimeException(msg);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
  }

  @Override
  public void emitTuples()
  {
    if (count < numTuples) {
      ++count;
      out.emit(Math.random());
    }
  }

  public int getNumTuples()
  {
    return numTuples;
  }

  /**
   * Sets the number of tuples to be emitted every window.
   * @param numTuples number of tuples
   */
  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
  }

  @Override
  public Response processStats(BatchedOperatorStats batchedOperatorStats) {

    final long emittedCount = batchedOperatorStats.getTuplesEmittedPSMA();

    // we only perform a single dynamic repartition
    Response res = new Response();
    res.repartitionRequired = false;
    if (emittedCount > 500 && partitions < MAX_PARTITIONS) {
      LOG.info("processStats: trying repartition of input operator current {} required {}",
               partitions, MAX_PARTITIONS);
      LOG.info("**** operator id = {}, window id = {}, tuplesProcessedPSMA = {}, tuplesEmittedPSMA = {}",
              batchedOperatorStats.getOperatorId(),
              batchedOperatorStats.getCurrentWindowId(),
              batchedOperatorStats.getTuplesProcessedPSMA(),
              emittedCount);
      partitions = MAX_PARTITIONS;
      res.repartitionRequired = true;
    }

    return res;
  }  // processStats

  /**
   * Clone object by serializing and deserializing using Kryo.
   * Note this is different from using {@link Kryo#copy(Object)}, which will attempt to also clone transient fields.
   *
   * @param kryo kryo object used to clone objects
   * @param src src object that copy from
   * @return cloned object
   */
  @SuppressWarnings("unchecked")
  private static <SRC> SRC cloneObject(Kryo kryo, SRC src)
  {
    kryo.setClassLoader(src.getClass().getClassLoader());
    ByteArrayOutputStream bos = null;
    Output output;
    Input input = null;
    try {
      bos = new ByteArrayOutputStream();
      output = new Output(bos);
      kryo.writeObject(output, src);
      output.close();
      input = new Input(bos.toByteArray());
      return (SRC)kryo.readObject(input, src.getClass());
    } finally {
      IOUtils.closeQuietly(input);
      IOUtils.closeQuietly(bos);
    }
  }

  @Override
  public Collection<Partition<Gen>> definePartitions(
      Collection<Partition<Gen>> list, PartitioningContext context)
  {
    if (partitions < 0) {    // error
      String msg = String.format("Error: Bad value: partitions = %d%n", partitions);
      LOG.error(msg);
      throw new RuntimeException(msg);
    }

    final int prevCount = list.size();
    if (1 == prevCount) {    // initial call
      LOG.info("definePartitions: First call, prevCount = {}, partitions = {}",
               prevCount, partitions);
    }

    if (prevCount == partitions) {
      LOG.info("definePartitions: Nothing to do in definePartitions");
      return list;    // nothing to do
    }

    LOG.debug("definePartitions: Repartitioning from {} to {}", prevCount, partitions);

    Kryo kryo = new Kryo();

    // return value: new list of partitions (includes old list)
    List<Partition<Gen>> newPartitions = Lists.newArrayListWithExpectedSize(partitions);

    for (int i = 0; i < partitions; i++) {
      Gen oper = cloneObject(kryo, this);
      newPartitions.add(new DefaultPartition<>(oper));
    }

    LOG.info("definePartition: returning {} partitions", newPartitions.size());
    return newPartitions;
  }

}
