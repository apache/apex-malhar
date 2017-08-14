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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * This is the base implementation of a NiFi output operator.&nbsp;
 * A concrete operator should be created from this skeleton implementation.
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have any number of input ports<br>
 * <b>Output</b>: no output port<br>
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
 * @displayName Abstract NiFi Output
 * @category Messaging
 * @tags output operator
 *
 *
 * @since 3.4.0
 */
public abstract class AbstractNiFiOutputOperator<T> extends BaseOperator
{

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNiFiOutputOperator.class);

  protected final SiteToSiteClient.Builder siteToSiteBuilder;
  protected final NiFiDataPacketBuilder<T> dataPacketBuilder;
  protected final WindowDataManager windowDataManager;

  protected transient SiteToSiteClient client;

  private transient int operatorContextId;
  private transient long currentWindowId;
  private transient long largestRecoveryWindowId;
  protected transient boolean skipProcessingTuple = false;

  /**
   * @param siteToSiteBuilder the builder for a NiFi SiteToSiteClient
   * @param dataPacketBuilder a builder to produce NiFiDataPackets from incoming data
   * @param windowDataManager  a WindowDataManager to save and load state for windows of tuples
   */
  public AbstractNiFiOutputOperator(final SiteToSiteClient.Builder siteToSiteBuilder,
      final NiFiDataPacketBuilder<T> dataPacketBuilder, final WindowDataManager windowDataManager)
  {
    this.siteToSiteBuilder = siteToSiteBuilder;
    this.dataPacketBuilder = dataPacketBuilder;
    this.windowDataManager = windowDataManager;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.client = siteToSiteBuilder.build();
    this.operatorContextId = context.getId();
    this.windowDataManager.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    largestRecoveryWindowId = windowDataManager.getLargestCompletedWindow();

    // if processing a window we've already seen, don't resend the tuples
    if (currentWindowId <= largestRecoveryWindowId) {
      skipProcessingTuple = true;
    } else {
      skipProcessingTuple = false;
    }
  }

  @Override
  public void endWindow()
  {
    // if replaying then nothing to do
    if (currentWindowId <= largestRecoveryWindowId) {
      return;
    }

    // if processing a new window then give sub-classes a chance to take action
    endNewWindow();

    // mark that we processed the window
    try {
      windowDataManager.save("processedWindow", currentWindowId);
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  /**
   * Called in endWindow() to give sub-classes a chance to take action when processing a new window.
   *
   * If the current window is <= the largest recovery window, this method will never be called.
   */
  protected abstract void endNewWindow();


  @Override
  public void teardown()
  {
    LOGGER.debug("Tearing down operator...");
    windowDataManager.teardown();
    try {
      client.close();
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  /**
   * Send the given batch of tuples to NiFi in a transaction, using the provided builder to
   * first convert each tuple into a NiFiDataPacket.
   *
   * @param tuples a list of tuples to process
   */
  protected void processTuples(List<T> tuples)
  {
    if (tuples == null || tuples.size() == 0) {
      return;
    }

    // create a transaction and send the data packets
    try {
      final Transaction transaction = client.createTransaction(TransferDirection.SEND);
      if (transaction == null) {
        throw new IllegalStateException("Unable to create a NiFi Transaction to send data");
      }

      // convert each tuple to a NiFiDataPacket using the provided builder
      for (T tuple : tuples) {
        NiFiDataPacket dp = dataPacketBuilder.createNiFiDataPacket(tuple);
        transaction.send(dp.getContent(), dp.getAttributes());
      }

      transaction.confirm();
      transaction.complete();
    } catch (IOException ioe) {
      DTThrowable.rethrow(ioe);
    }

  }

}
