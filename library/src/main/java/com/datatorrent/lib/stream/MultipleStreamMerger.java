/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.lib.stream;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * A helper class  that adds functionality to bypass limitations preventing us from combining more than two
 * streams at a time with the existing Stream Merger.
 *
 * Usage:
 *
 * dag.addOperator("Stream_1", op1);
 * dag.addOperator("Stream_2", op2);
 * dag.addOperator("Stream_3", op3);
 *
 * MultipleStreamMerger merger = new MultipleStreamMerger();
 * DefaultOutputPort streamOut = merger.merge(op1.out)
 * .merge(op2.out)
 * .merge(op3.out)
 * .mergeStreams(dag, conf);
 *
 * dag.addStream("merger-counter", streamOut, counterOp.counter);
 *
 * @param <K>
 */
public class MultipleStreamMerger<K>
{
  public class Stream
  {
    DefaultInputPort<K> destPort;
    SourcedOutputPort sourcePort;
    String name;

    public Stream(String name, SourcedOutputPort sourcePort, DefaultInputPort<K> destPort)
    {
      this.destPort = destPort;
      this.sourcePort = sourcePort;
      this.name = name;
    }
  }

  public class NamedMerger
  {
    StreamMerger<K> merger;
    String name;

    public NamedMerger(String name, StreamMerger<K> merger)
    {
      this.merger = merger;
      this.name = name;
    }
  }

  /**
   * A simple class to allow us to track whether the port to be merged is internal (allowing it to be thread local)
   * or external
   */
  public class SourcedOutputPort
  {
    boolean internal;
    DefaultOutputPort<K> port;

    public SourcedOutputPort(DefaultOutputPort<K> port)
    {
      this.internal = false;
      this.port = port;
    }

    public SourcedOutputPort(boolean internal, DefaultOutputPort<K> port)
    {
      this.internal = internal;
      this.port = port;
    }
  }

  ArrayList<DefaultOutputPort<K>> streamsToMerge = new ArrayList<>();

  private DefaultOutputPort<K> streamOutput = new DefaultOutputPort<>();

  /**
   * Used to define all the sources to be merged into a single stream.
   *
   * @param sourcePort - The output port from the upstream operator that provides data
   * @return The updated MultipleStreamMerger object that tracks which streams should be unified.
   */
  public MultipleStreamMerger<K> merge(DefaultOutputPort<K> sourcePort)
  {
    streamsToMerge.add(sourcePort);
    return this;
  }


  /**
   * Given the streams to merge have been selected with {@link #merge(DefaultOutputPort)}, create a subDAG and add it
   * to an existing DAG.
   *
   * To merge more than two streams at a time, we construct a tiered hierarchy of thread-local StreamMerger operators
   * E.g.
   *
   * Stream 0 ->
   *            StreamMerger_1 ->
   * Stream 1 ->
   *                               StreamMerger_Final -> Out
   * Stream 2 ->
   *            StreamMerger_2 ->
   * Stream 3 ->
   *
   * @param dag - The DAG to update
   * @param conf - The configuration
   *
   */
  public DefaultOutputPort<K> mergeStreams(DAG dag, Configuration conf)
  {
    if (streamsToMerge.size() < 2) {
      throw new IllegalArgumentException("Not enough streams to merge, at least two streams must be selected for " +
          "merging with `.merge()`.");
    }

    ArrayList<Stream> streamsToAddToDag = new ArrayList<>();
    ArrayList<NamedMerger> operatorsToAdd = new ArrayList<>();

    // Determine operators and streams to add to the DAG
    constructMergeTree(streamsToAddToDag, operatorsToAdd);

    for (NamedMerger m : operatorsToAdd) {
      dag.addOperator(m.name, m.merger);
    }

    for (Stream s : streamsToAddToDag) {
      DAG.StreamMeta stream = dag.addStream(s.name, s.sourcePort.port, s.destPort);
      if (s.sourcePort.internal) {
        stream.setLocality(DAG.Locality.CONTAINER_LOCAL);
      }
    }

    return streamOutput;
  }

  /**
   * Given a set of streams to be merged (defined via {@link #merge(DefaultOutputPort)}), compute the optimal
   * structure of cascading mergers that need to be instantiated, added to the dag, and linked together.
   * @param streamsToAddToDag - (output)  A list that is populated with streams that should be added to the  DAG
   * @param operatorsToAdd - (output) A list that is populated with operators to be added to the DAG
   */
  public void constructMergeTree(
      ArrayList<Stream> streamsToAddToDag,
      ArrayList<NamedMerger> operatorsToAdd)
  {
    if (streamsToMerge.size() < 2) {
      throw new IllegalArgumentException("Not enough streams to merge. Ensure `.merge` was called for each stream " +
          "to be added.");
    }

    /**
     * We can unify all unmerged streams by using a Queue. Unconnected ports are pushed to a queue (FIFO) and then for
     * every pair of unconnected ports, we create a Merger. If only one port remains, we simply use the last added
     * merger.
     *
     * We use SourcedOutputPorts to track whether a given port comes from inside or outside the module, allowing us to
     * set operator locality for those ports that are strictly from inside the module.
     */
    Queue<SourcedOutputPort> unconnectedPorts = new LinkedList<>();
    for (DefaultOutputPort<K> streamToAdd : streamsToMerge) {
      unconnectedPorts.add(new SourcedOutputPort(true, streamToAdd));
    }

    long mergerCount = 0;
    while (!unconnectedPorts.isEmpty()) {
      if (unconnectedPorts.size() >= 2) {
        StreamMerger<K> merger = new StreamMerger<>();
        SourcedOutputPort firstPort = unconnectedPorts.poll();
        SourcedOutputPort secondPort = unconnectedPorts.poll();

        streamsToAddToDag.add(new Stream("Merger_" + mergerCount + "_P1", firstPort, merger.data1));
        streamsToAddToDag.add(new Stream("Merger_" + mergerCount + "_P2", secondPort, merger.data2));
        operatorsToAdd.add(new NamedMerger("Merger_" + mergerCount++, merger));
        unconnectedPorts.add(new SourcedOutputPort(merger.out));
      } else {
        // Don't need to create any more mergers, just use the last added merger.
        streamOutput = (operatorsToAdd.get(operatorsToAdd.size() - 1).merger.out);
        break;
      }
    }
  }
}
