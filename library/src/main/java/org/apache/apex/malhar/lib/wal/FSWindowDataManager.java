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
package org.apache.apex.malhar.lib.wal;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.FSStorageAgent;

/**
 * An {@link WindowDataManager} that uses FS to persist state.
 */
public class FSWindowDataManager implements WindowDataManager
{
  private static final String DEF_RECOVERY_PATH = "idempotentState";

  protected transient FSStorageAgent storageAgent;

  /**
   * Recovery path relative to app path where state is saved.
   */
  @NotNull
  private String recoveryPath;

  private boolean isRecoveryPathRelativeToAppPath = true;

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

  public FSWindowDataManager()
  {
    replayState = TreeMultimap.create();
    largestRecoveryWindow = Stateless.WINDOW_ID;
    recoveryPath = DEF_RECOVERY_PATH;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    Configuration configuration = new Configuration();
    if (isRecoveryPathRelativeToAppPath) {
      appPath = new Path(context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + recoveryPath);
    } else {
      appPath = new Path(recoveryPath);
    }

    try {
      storageAgent = new FSStorageAgent(appPath.toString(), configuration);

      fs = FileSystem.newInstance(appPath.toUri(), configuration);

      if (fs.exists(appPath)) {
        FileStatus[] fileStatuses = fs.listStatus(appPath);

        for (FileStatus operatorDirStatus : fileStatuses) {
          int operatorId = Integer.parseInt(operatorDirStatus.getPath().getName());

          for (FileStatus status : fs.listStatus(operatorDirStatus.getPath())) {
            String fileName = status.getPath().getName();
            if (fileName.endsWith(FSStorageAgent.TMP_FILE)) {
              continue;
            }
            long windowId = Long.parseLong(fileName, 16);
            replayState.put(windowId, operatorId);
            if (windowId > largestRecoveryWindow) {
              largestRecoveryWindow = windowId;
            }
          }
        }
      }
    } catch (IOException e) {
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

  @Override
  public long[] getWindowIds() throws IOException
  {
    SortedSet<Long> windowIds = replayState.keySet();
    long[] windowIdsArray = new long[windowIds.size()];

    int index = 0;

    for (Long windowId: windowIds) {
      windowIdsArray[index] = windowId;
      index++;
    }

    return windowIdsArray;
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
      Iterator<Map.Entry<Long, Collection<Integer>>> iterator = replayState.asMap().entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Long, Collection<Integer>> windowEntry = iterator.next();
        long lwindow = windowEntry.getKey();
        if (lwindow > windowId) {
          break;
        }
        for (Integer loperator : windowEntry.getValue()) {

          if (deletedOperators.contains(loperator)) {
            storageAgent.delete(loperator, lwindow);

            Path loperatorPath = new Path(appPath, Integer.toString(loperator));
            if (fs.listStatus(loperatorPath).length == 0) {
              //The operator was deleted and it has nothing to replay.
              deletedOperators.remove(loperator);
              fs.delete(loperatorPath, true);
            }
          } else if (loperator == operatorId) {
            storageAgent.delete(loperator, lwindow);
          }
        }
        iterator.remove();
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
  public void partitioned(Collection<WindowDataManager> newManagers, Set<Integer> removedOperatorIds)
  {
    Preconditions.checkArgument(newManagers != null && !newManagers.isEmpty(),
        "there has to be one idempotent storage manager");
    org.apache.apex.malhar.lib.wal.FSWindowDataManager deletedOperatorsManager = null;

    if (removedOperatorIds != null && !removedOperatorIds.isEmpty()) {
      if (this.deletedOperators == null) {
        this.deletedOperators = Sets.newHashSet();
      }
      this.deletedOperators.addAll(removedOperatorIds);
    }

    for (WindowDataManager storageManager : newManagers) {

      org.apache.apex.malhar.lib.wal.FSWindowDataManager lmanager = (org.apache.apex.malhar.lib.wal.FSWindowDataManager)storageManager;
      lmanager.recoveryPath = this.recoveryPath;
      lmanager.storageAgent = this.storageAgent;

      if (lmanager.deletedOperators != null) {
        deletedOperatorsManager = lmanager;
      }
      //only one physical instance can manage deleted operators so clearing this field for rest of the instances.
      if (lmanager != deletedOperatorsManager) {
        lmanager.deletedOperators = null;
      }
    }

    if (removedOperatorIds == null || removedOperatorIds.isEmpty()) {
      //Nothing to do
      return;
    }
    if (this.deletedOperators != null) {

      /*If some operators were removed then there needs to be a manager which can clean there state when it is not
      needed.*/
      if (deletedOperatorsManager == null) {
        //None of the managers were handling deleted operators data.
        deletedOperatorsManager = (org.apache.apex.malhar.lib.wal.FSWindowDataManager)newManagers.iterator().next();
        deletedOperatorsManager.deletedOperators = Sets.newHashSet();
      }

      deletedOperatorsManager.deletedOperators.addAll(removedOperatorIds);
    }
  }

  @Override
  public void teardown()
  {
    try {
      fs.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return recovery path
   */
  public String getRecoveryPath()
  {
    return recoveryPath;
  }

  /**
   * Sets the recovery path. If {@link #isRecoveryPathRelativeToAppPath} is true then this path is handled relative
   * to the application path; otherwise it is handled as an absolute path.
   *
   * @param recoveryPath recovery path
   */
  public void setRecoveryPath(String recoveryPath)
  {
    this.recoveryPath = recoveryPath;
  }

  /**
   * @return true if recovery path is relative to app path; false otherwise.
   */
  public boolean isRecoveryPathRelativeToAppPath()
  {
    return isRecoveryPathRelativeToAppPath;
  }

  /**
   * Specifies whether the recovery path is relative to application path.
   *
   * @param recoveryPathRelativeToAppPath true if recovery path is relative to application path; false otherwise.
   */
  public void setRecoveryPathRelativeToAppPath(boolean recoveryPathRelativeToAppPath)
  {
    isRecoveryPathRelativeToAppPath = recoveryPathRelativeToAppPath;
  }
}
