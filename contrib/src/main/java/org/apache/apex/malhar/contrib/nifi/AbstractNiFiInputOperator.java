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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.protocol.DataPacket;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * This is the base implementation of a NiFi input operator.&nbsp;
 * Subclasses should implement the methods which convert NiFi DataPackets to tuples and emit them.
 * <p>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Can have any number of output ports<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * Classes derived from this have to implement the abstract methods emitTuples(List<T> tuples)&nbsp;
 * and createTuple(DataPacket dp)<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * </p>
 *
 * @displayName Abstract NiFi Input
 * @category Messaging
 * @tags input operator
 *
 * @since 3.4.0
 */

public abstract class AbstractNiFiInputOperator<T> implements InputOperator
{

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNiFiInputOperator.class);

  private transient SiteToSiteClient client;
  private final SiteToSiteClient.Builder siteToSiteBuilder;

  private transient int operatorContextId;
  private transient long currentWindowId;
  private transient List<T> currentWindowTuples;
  private transient List<T> recoveredTuples;
  private final WindowDataManager windowDataManager;

  /**
   * @param siteToSiteBuilder the builder for a NiFi SiteToSiteClient
   * @param windowDataManager a WindowDataManager to save and load state for windows of tuples
   */
  public AbstractNiFiInputOperator(final SiteToSiteClient.Builder siteToSiteBuilder,
      final WindowDataManager windowDataManager)
  {
    this.siteToSiteBuilder = siteToSiteBuilder;
    this.windowDataManager = windowDataManager;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.client = siteToSiteBuilder.build();
    this.operatorContextId = context.getId();
    this.currentWindowTuples = new ArrayList<>();
    this.recoveredTuples = new ArrayList<>();
    this.windowDataManager.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;

    // if the current window is now less than the largest window, then we need to replay data
    if (currentWindowId <= windowDataManager.getLargestCompletedWindow()) {
      try {
        List<T> recoveredData =  (List<T>)this.windowDataManager.retrieve(windowId);
        if (recoveredData == null) {
          return;
        }

        // if we recovered tuples then load them to be processed by next call to emitTuples()
        recoveredTuples.addAll(recoveredData);
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

  @Override
  public void emitTuples()
  {
    // if we have recovered tuples we must be replaying a previous window so emit them,
    // clear the recovered list, and return until we have no more recovered data
    if (recoveredTuples.size() > 0) {
      emitTuples(recoveredTuples);
      recoveredTuples.clear();
      return;
    }

    // no recovered data so start a transaction and pull new data
    try {
      final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);
      if (transaction == null) {
        LOGGER.warn("A transaction could not be created, returning...");
        return;
      }

      DataPacket dataPacket = transaction.receive();
      if (dataPacket == null) {
        transaction.confirm();
        transaction.complete();
        LOGGER.debug("No data available to pull, returning and will try again...");
        return;
      }

      // read all of the available data packets and convert to the given type
      final List<T> tuples = new ArrayList<>();
      do {
        tuples.add(createTuple(dataPacket));
        dataPacket = transaction.receive();
      }
      while (dataPacket != null);

      // confirm all of the expected data was received by comparing check-sums, does not complete the transaction
      transaction.confirm();

      // ensure we have the data saved before proceeding in case anything goes wrong
      currentWindowTuples.addAll(tuples);
      windowDataManager.save(currentWindowTuples, currentWindowId);

      // we now have the data saved so we can complete the transaction
      transaction.complete();

      // delegate to sub-classes to emit the tuples
      emitTuples(tuples);

    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  /**
   * Provides mechanism for converting a DataPacket to the given type.
   *
   * @param dataPacket a DataPacket from the NiFi Site-To-Site client.
   * @return the given type of tuple
   */
  protected abstract T createTuple(final DataPacket dataPacket) throws IOException;

  /**
   * Provided mechanism to emit the list of tuples for follow-on processing.
   *
   * @param tuples a list of tuples received from NiFi.
   */
  protected abstract void emitTuples(final List<T> tuples);

  @Override
  public void endWindow()
  {
    // save the final state of the window and clear the current window list
    try {
      windowDataManager.save(currentWindowTuples, currentWindowId);
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
    currentWindowTuples.clear();
  }

  @Override
  public void teardown()
  {
    LOGGER.debug("Tearing down operator...");
    windowDataManager.teardown();
    try {
      client.close();
    } catch (IOException e) {
      throw new RuntimeException("Error closing SiteToSiteClient", e);
    }
  }

}
