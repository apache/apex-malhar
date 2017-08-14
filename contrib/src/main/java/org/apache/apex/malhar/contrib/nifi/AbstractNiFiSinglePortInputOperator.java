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

import java.util.List;

import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.nifi.remote.client.SiteToSiteClient;

import com.datatorrent.api.DefaultOutputPort;

/**
 * This is the base implementation of a NiFi input operator with a single output port.&nbsp;
 * Subclasses should implement the methods which convert NiFi DataPackets to tuples.
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Have only one output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method createTuple(DataPacket dp) <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 * </p>
 *
 * @displayName Abstract NiFi Single Port Input
 * @category Messaging
 * @tags input operator
 *
 *
 * @since 3.4.0
 */
public abstract class AbstractNiFiSinglePortInputOperator<T> extends AbstractNiFiInputOperator<T>
{

  /**
   * This is the output port on which tuples extracted from NiFi data packets are emitted.
   */
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<>();

  /**
   *
   * @param siteToSiteBuilder the builder for a NiFi SiteToSiteClient
   * @param windowDataManager a WindowDataManager to save and load state for windows of tuples
   */
  public AbstractNiFiSinglePortInputOperator(final SiteToSiteClient.Builder siteToSiteBuilder,
      final WindowDataManager windowDataManager)
  {
    super(siteToSiteBuilder, windowDataManager);
  }

  @Override
  protected void emitTuples(final List<T> tuples)
  {
    for (T tuple : tuples) {
      outputPort.emit(tuple);
    }
  }

}
