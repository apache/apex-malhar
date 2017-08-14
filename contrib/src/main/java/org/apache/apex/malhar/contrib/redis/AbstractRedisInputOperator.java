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
package org.apache.apex.malhar.contrib.redis;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.db.AbstractKeyValueStoreInputOperator;
import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.apex.malhar.lib.wal.WindowDataManager;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.CheckpointNotificationListener;
import com.datatorrent.netlet.util.DTThrowable;

import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

/**
 * This is the base implementation of a Redis input operator.
 *
 * @displayName Abstract Redis Input
 * @category Input
 * @tags redis, key value
 *
 * @param <T>
 *          The tuple type.
 * @since 0.9.3
 */
public abstract class AbstractRedisInputOperator<T> extends AbstractKeyValueStoreInputOperator<T, RedisStore> implements CheckpointNotificationListener
{
  protected transient List<String> keys = new ArrayList<String>();
  protected transient Integer scanOffset;
  protected transient ScanParams scanParameters;
  private transient boolean scanComplete;
  private transient Integer backupOffset;
  private int scanCount;
  private transient boolean replay;
  private transient boolean skipOffsetRecovery = true;

  @NotNull
  private WindowDataManager windowDataManager;

  private transient OperatorContext context;
  private transient long currentWindowId;
  private transient Integer sleepTimeMillis;
  private transient Integer scanCallsInCurrentWindow;
  private RecoveryState recoveryState;

  /*
   * Recovery State contains last offset processed in window and number of times
   * ScanKeys was invoked in window We need to capture to capture number of
   * calls to ScanKeys because, last offset returned by scanKeys call is not
   * always monotonically increasing. Storing offset and number of times scan
   * was done for each window, guarantees idempotency for each window
   */
  public static class RecoveryState implements Serializable
  {
    public Integer scanOffsetAtBeginWindow;
    public Integer numberOfScanCallsInWindow;
  }

  public AbstractRedisInputOperator()
  {
    scanCount = 100;
    recoveryState = new RecoveryState();
    recoveryState.scanOffsetAtBeginWindow = 0;
    recoveryState.numberOfScanCallsInWindow = 0;
    setWindowDataManager(new FSWindowDataManager());
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    scanCallsInCurrentWindow = 0;
    replay = false;
    if (currentWindowId <= getWindowDataManager().getLargestCompletedWindow()) {
      replay(windowId);
    }
  }

  private void replay(long windowId)
  {
    try {
      // For first recovered window, offset is already part of recovery state.
      // So skip reading from idempotency manager
      if (!skipOffsetRecovery) {
        // Begin offset for this window is recovery offset stored for the last
        // window
        RecoveryState recoveryStateForLastWindow = (RecoveryState)getWindowDataManager().retrieve(windowId - 1);
        recoveryState.scanOffsetAtBeginWindow = recoveryStateForLastWindow.scanOffsetAtBeginWindow;
      }
      skipOffsetRecovery = false;
      RecoveryState recoveryStateForCurrentWindow = (RecoveryState)getWindowDataManager().retrieve(windowId);
      recoveryState.numberOfScanCallsInWindow = recoveryStateForCurrentWindow.numberOfScanCallsInWindow;
      if (recoveryState.scanOffsetAtBeginWindow != null) {
        scanOffset = recoveryState.scanOffsetAtBeginWindow;
      }
      replay = true;

    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  private void scanKeysFromOffset()
  {
    if (!scanComplete) {
      if (replay && scanCallsInCurrentWindow >= recoveryState.numberOfScanCallsInWindow) {
        try {
          Thread.sleep(sleepTimeMillis);
        } catch (InterruptedException e) {
          DTThrowable.rethrow(e);
        }
        return;
      }

      ScanResult<String> result = store.ScanKeys(scanOffset, scanParameters);
      backupOffset = scanOffset;
      scanOffset = Integer.parseInt(result.getStringCursor());
      if (scanOffset == 0) {
        // Redis store returns 0 after all data is read
        scanComplete = true;

        // point scanOffset to the end in this case for reading any new tuples
        scanOffset = backupOffset + result.getResult().size();
      }
      keys = result.getResult();
    }
    scanCallsInCurrentWindow++;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    sleepTimeMillis = context.getValue(context.SPIN_MILLIS);
    getWindowDataManager().setup(context);
    this.context = context;
    scanOffset = 0;
    scanComplete = false;
    scanParameters = new ScanParams();
    scanParameters.count(scanCount);

    // For the 1st window after checkpoint, windowID - 1 would not have recovery
    // offset stored in windowDataManager
    // But recoveryOffset is non-transient, so will be recovered with
    // checkPointing
    // Offset recovery from idempotency storage can be skipped in this case
    scanOffset = recoveryState.scanOffsetAtBeginWindow;
    skipOffsetRecovery = true;
  }

  @Override
  public void endWindow()
  {
    while (replay && scanCallsInCurrentWindow < recoveryState.numberOfScanCallsInWindow) {
      // If less keys got scanned in this window, scan till recovery offset
      scanKeysFromOffset();
      processTuples();
    }
    super.endWindow();
    recoveryState.scanOffsetAtBeginWindow = scanOffset;
    recoveryState.numberOfScanCallsInWindow = scanCallsInCurrentWindow;

    if (currentWindowId > getWindowDataManager().getLargestCompletedWindow()) {
      try {
        getWindowDataManager().save(recoveryState, currentWindowId);
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    getWindowDataManager().teardown();
  }

  /*
   * get number of keys to read for each redis key scan
   */
  public int getScanCount()
  {
    return scanCount;
  }

  /*
   * set number of keys to read for each redis key scan
   */
  public void setScanCount(int scanCount)
  {
    this.scanCount = scanCount;
  }

  @Override
  public void emitTuples()
  {
    scanKeysFromOffset();
    processTuples();
  }

  public abstract void processTuples();

  @Override
  public void beforeCheckpoint(long windowId)
  {
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
    try {
      getWindowDataManager().committed(windowId);
    } catch (IOException e) {
      throw new RuntimeException("committing", e);
    }
  }

  /*
   * get Idempotent Storage manager instance
   */
  public WindowDataManager getWindowDataManager()
  {
    return windowDataManager;
  }

  /*
   * set Idempotent storage manager instance
   */
  public void setWindowDataManager(WindowDataManager windowDataManager)
  {
    this.windowDataManager = windowDataManager;
  }
}
