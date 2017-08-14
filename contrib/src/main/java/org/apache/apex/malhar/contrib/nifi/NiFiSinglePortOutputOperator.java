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
package org.apache.apex.malhar.contrib.nifi;

import java.util.ArrayList;
import java.util.List;

import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.nifi.remote.client.SiteToSiteClient;

import com.datatorrent.api.DefaultInputPort;

/**
 * NiFi output adapter operator with a single input port. Clients should provide a NiFiDataPacketBuilder implementation
 * for converting incoming tuples to NiFiDataPackets.
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: Have only one input port<br>
 * <b>Output</b>: No output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 * </p>
 *
 * @displayName NiFi Single Port Output
 * @category Messaging
 * @tags output operator
 *
 *
 * @since 3.4.0
 */
public class NiFiSinglePortOutputOperator<T> extends AbstractNiFiOutputOperator<T>
{

  public final transient BufferingInputPort inputPort;

  // required by Kyro serialization
  private NiFiSinglePortOutputOperator()
  {
    this(null, null, null, 0);
  }

  /**
   * @param siteToSiteBuilder the builder for a NiFi SiteToSiteClient
   * @param dataPacketBuilder a builder to produce NiFiDataPackets from incoming data
   * @param windowDataManager  a WindowDataManager to save and load state for windows of tuples
   * @param batchSize the maximum number of tuples to send to NiFi in a single transaction
   */
  public NiFiSinglePortOutputOperator(
      final SiteToSiteClient.Builder siteToSiteBuilder,
      final NiFiDataPacketBuilder<T> dataPacketBuilder,
      final WindowDataManager windowDataManager,
      final int batchSize)
  {
    super(siteToSiteBuilder, dataPacketBuilder, windowDataManager);
    this.inputPort = new BufferingInputPort(batchSize);
  }

  @Override
  protected void endNewWindow()
  {
    // flush any tuples that may have been buffered between the last flush and endWindow()
    inputPort.flush();
  }

  /**
   * An InputPort that accumulates tuples up to the provided batch size before flushing.
   */
  public class BufferingInputPort extends DefaultInputPort<T>
  {

    private final int batchSize;
    private final List<T> tuples;

    public BufferingInputPort(final int batchSize)
    {
      this.tuples = new ArrayList<>();
      this.batchSize = batchSize;
    }

    @Override
    public void process(T tuple)
    {
      if (!skipProcessingTuple) {
        tuples.add(tuple);

        if (tuples.size() >= batchSize) {
          flush();
        }
      }
    }

    public void flush()
    {
      processTuples(tuples);
      tuples.clear();
    }

  }

}
