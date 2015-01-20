/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.io;

import java.io.IOException;
import java.util.*;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.lib.io.fs.AbstractFileInputOperator;
import com.datatorrent.lib.util.FSStorageAgent;

/**
 * An idempotent storage manager allows an operator to emit the same tuples in every replayed application window. An idempotent agent
 * cannot make any guarantees about the tuples emitted in the application window which fails.
 *
 * The order of tuples is guaranteed for ordered input sources.
 *
 * <b>Important:</b> In order for an idempotent storage manager to function correctly it cannot allow
 * checkpoints to occur within an application window and checkpoints must be aligned with
 * application window boundaries.
 */

public interface IdempotentStorageManager extends StorageAgent, Component<Context.OperatorContext>
{
  /**
   * Gets the largest window for which there is recovery data.
   */
  long getLargestRecoveryWindow();

  /**
   * When an operator can partition itself dynamically then there is no guarantee that an input state which was being handled
   * by one instance previously will be handled by the same instance after partitioning. <br/>
   * For eg. An {@link AbstractFileInputOperator} instance which reads a File X till offset l (not check-pointed) may no longer be the
   * instance that handles file X after repartitioning as no. of instances may have changed and file X is re-hashed to another instance. <br/>
   * The new instance wouldn't know from what point to read the File X unless it reads the idempotent storage of all the operators for the window
   * being replayed and fix it's state.
   *
   * @param windowId window id.
   * @return mapping of operator id to the corresponding state
   * @throws IOException
   */
  Map<Integer, Object> load(long windowId) throws IOException;

  /**
   * Delete the artifacts of the operator for windows <= windowId.
   *
   * @param operatorId
   * @param windowId
   * @throws IOException
   */
  public void deleteUpTo(int operatorId, long windowId) throws IOException;

  /**
   * This informs the idempotent storage manager that operator is partitioned so that it can set properties and distribute state.
   *
   * @param newManagers        all the new idempotent storage managers.
   * @param removedOperatorIds set of operator ids which were removed after partitioning.
   */
  void partitioned(Collection<IdempotentStorageManager> newManagers, Set<Integer> removedOperatorIds);

  IdempotentStorageManager newInstance();

  /**
   * An {@link IdempotentStorageManager} that uses FS to persist state.
   */
  public static class FSIdempotentStorageManager implements IdempotentStorageManager
  {
    protected FSStorageAgent storageAgent;

    @NotNull
    protected String recoveryPath;

    /**
     * largest window for which there is recovery data across all physical operator instances.
     */
    protected transient long largestRecoveryWindow;

    /**
     * This is not null only for one physical instance.<br/>
     * It consists of operator ids which have been deleted but have some state that can be replayed.
     * Only one of the instances would be handling (modifying) the files that belong to this state.
     */
    protected Set<Integer> deletedOperators;

    /**
     * Sorted mapping from window id to all the operators that have state to replay for that window.
     */
    protected final transient TreeMultimap<Long, Integer> replayState;

    protected transient FileSystem fs;
    protected transient Path appPath;

    public FSIdempotentStorageManager()
    {
      replayState = TreeMultimap.create();
      deletedOperators = Sets.newHashSet();
      largestRecoveryWindow = Stateless.WINDOW_ID;
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
      Configuration configuration = new Configuration();
      appPath = new Path(recoveryPath + '/' + context.getValue(DAG.APPLICATION_ID));

      if (storageAgent == null) {
        storageAgent = new FSStorageAgent(appPath.toString(), configuration);
      }
      try {
        fs = FileSystem.newInstance(appPath.toUri(), configuration);

        if (fs.exists(appPath)) {
          FileStatus[] fileStatuses = fs.listStatus(appPath);

          for (FileStatus operatorDirStatus : fileStatuses) {
            int operatorId = Integer.parseInt(operatorDirStatus.getPath().getName());

            for (FileStatus status : fs.listStatus(operatorDirStatus.getPath())) {
              String fileName = status.getPath().getName();
              long windowId = Long.parseLong(fileName, 16);
              replayState.put(windowId, operatorId);
              if (windowId > largestRecoveryWindow) {
                largestRecoveryWindow = windowId;
              }
            }
          }
        }
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void save(Object object, int operatorId, long windowId) throws IOException
    {
      storageAgent.save(object, operatorId, windowId);
    }

    @Override
    public Object load(int operatorId, long windowId) throws IOException
    {
      Set<Integer> operators = replayState.get(windowId);
      if (operators == null || !operators.contains(operatorId)) {
        return null;
      }
      return storageAgent.load(operatorId, windowId);
    }

    @Override
    public void delete(int operatorId, long windowId) throws IOException
    {
      storageAgent.delete(operatorId, windowId);
    }

    @Override
    public Map<Integer, Object> load(long windowId) throws IOException
    {
      Set<Integer> operators = replayState.get(windowId);
      if (operators == null) {
        return null;
      }
      Map<Integer, Object> data = Maps.newHashMap();
      for (int operatorId : operators) {
        data.put(operatorId, load(operatorId, windowId));
      }
      return data;
    }

    @Override
    public long[] getWindowIds(int operatorId) throws IOException
    {
      Path operatorPath = new Path(appPath, String.valueOf(operatorId));
      if (!fs.exists(operatorPath) || fs.listStatus(operatorPath).length == 0) {
        return null;
      }
      return storageAgent.getWindowIds(operatorId);
    }

    /**
     * This deletes all the recovery files of window ids <= windowId.
     *
     * @param operatorId operator id.
     * @param windowId   the largest window id for which the states will be deleted.
     * @throws IOException
     */
    @Override
    public void deleteUpTo(int operatorId, long windowId) throws IOException
    {
      //deleting the replay state
      if (windowId <= largestRecoveryWindow && deletedOperators != null && !deletedOperators.isEmpty()) {
        Iterator<Long> windowsIterator = replayState.keySet().iterator();
        while (windowsIterator.hasNext()) {
          long lwindow = windowsIterator.next();
          if (lwindow > windowId) {
            break;
          }
          for (Integer loperator : replayState.removeAll(lwindow)) {

            if (deletedOperators.contains(loperator)) {
              storageAgent.delete(loperator, lwindow);

              Path loperatorPath = new Path(appPath, Integer.toString(loperator));
              if (fs.listStatus(loperatorPath).length == 0) {
                //The operator was deleted and it has nothing to replay.
                deletedOperators.remove(loperator);
                fs.delete(loperatorPath, true);
              }
            }
            else if (loperator == operatorId) {
              storageAgent.delete(loperator, lwindow);
            }
          }
        }
      }

      if (fs.listStatus(new Path(appPath, Integer.toString(operatorId))).length > 0) {
        long[] windowsAfterReplay = storageAgent.getWindowIds(operatorId);
        Arrays.sort(windowsAfterReplay);
        for (long lwindow : windowsAfterReplay) {
          if (lwindow <= windowId) {
            storageAgent.delete(operatorId, lwindow);
          }
        }
      }
    }

    @Override
    public long getLargestRecoveryWindow()
    {
      return largestRecoveryWindow;
    }

    @Override
    public void partitioned(Collection<IdempotentStorageManager> newManagers, Set<Integer> removedOperatorIds)
    {
      Preconditions.checkArgument(newManagers != null && !newManagers.isEmpty(), "there has to be one idempotent storage manager");
      FSIdempotentStorageManager deletedOperatorsManager = null;

      for (IdempotentStorageManager storageManager : newManagers) {

        FSIdempotentStorageManager lmanager = (FSIdempotentStorageManager) storageManager;
        lmanager.recoveryPath = this.recoveryPath;
        lmanager.storageAgent = this.storageAgent;

        if (lmanager.deletedOperators != null) {
          deletedOperatorsManager = lmanager;
        }
      }

      if (removedOperatorIds == null || removedOperatorIds.isEmpty()) {
        //Nothing to do
        return;
      }

      //If some operators were removed then there needs to be a manager which can clean there state when it is not needed.
      if (deletedOperatorsManager == null) {
        //None of the managers were handling deleted operators data.
        deletedOperatorsManager = (FSIdempotentStorageManager) newManagers.iterator().next();
        deletedOperatorsManager.deletedOperators = Sets.newHashSet();
      }

      deletedOperatorsManager.deletedOperators.addAll(removedOperatorIds);
    }

    @Override
    public void teardown()
    {
      try {
        fs.close();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public String getRecoveryPath()
    {
      return recoveryPath;
    }

    public void setRecoveryPath(String recoveryPath)
    {
      this.recoveryPath = recoveryPath;
    }

    @Override
    public FSIdempotentStorageManager newInstance()
    {
      return new FSIdempotentStorageManager();
    }
  }

  /**
   * This {@link IdempotentStorageManager} will never do recovery. This is a convenience class so that operators
   * can use the same logic for maintaining idempotency and avoiding idempotency.
   */
  public static class NoopIdempotentStorageManager implements IdempotentStorageManager
  {
    @Override
    public long getLargestRecoveryWindow()
    {
      return Stateless.WINDOW_ID;
    }

    @Override
    public Map<Integer, Object> load(long windowId) throws IOException
    {
      return null;
    }

    @Override
    public void partitioned(Collection<IdempotentStorageManager> newManagers, Set<Integer> removedOperatorIds)
    {
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }

    @Override
    public void save(Object object, int operatorId, long windowId) throws IOException
    {
    }

    @Override
    public Object load(int operatorId, long windowId) throws IOException
    {
      return null;
    }

    @Override
    public void delete(int operatorId, long windowId) throws IOException
    {
    }

    @Override
    public void deleteUpTo(int operatorId, long windowId) throws IOException
    {
    }

    @Override
    public long[] getWindowIds(int operatorId) throws IOException
    {
      return new long[0];
    }

    @Override
    public NoopIdempotentStorageManager newInstance()
    {
      return new NoopIdempotentStorageManager();
    }
  }
}
