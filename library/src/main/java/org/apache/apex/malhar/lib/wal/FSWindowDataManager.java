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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.managed.IncrementalCheckpointManagerImpl;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;

import com.datatorrent.lib.io.fs.AbstractFileInputOperator;
import com.datatorrent.netlet.util.Slice;

/**
 * An {@link WindowDataManager} that uses FS to persist state every completed application window.<p/>
 *
 * FSWindowDataManager uses {@link FSWindowReplayWAL} to write to files. While saving an artifact corresponding
 * to a window, the window date manager saves:
 * <ol>
 *   <li>Window id</li>
 *   <li>Artifact</li>
 * </ol>
 * In order to ensure that all the entries corresponding to a window id are appended to the same wal part file, the
 * wal operates in batch mode. In batch mode, the rotation of a wal part is done only after a batch is complete.<br/>
 * <p/>
 *
 * <b>Replaying data of a completed window</b><br/>
 * Main support that {@link WindowDataManager} provides to input operators is to be able to replay windows which
 * were completely processed but not checkpointed. This is necessary for making input operators idempotent.<br/>
 * The {@link FileSystemWAL}, however, ignores any data which is not checkpointed after failure. Therefore,
 * {@link FSWindowDataManager} cannot rely solely on the state in wal after failures and so during recovery it modifies
 * the wal state by traversing through the wal files.<br/>
 * <br/>
 * {@link IncrementalCheckpointManagerImpl}, however, relies only on the checkpointed state and therefore sets
 * {@link #relyOnCheckpoints} to true. This is because {@link IncrementalCheckpointManagerImpl} only saves data per
 * checkpoint window.
 * <p/>
 *
 * <b>Purging of stale artifacts</b><br/>
 * When a window gets committed, it indicates that all the operators in the DAG have completely finished processing that
 * window. This means that the data of this window can be deleted as it will never be requested for replaying.
 * Operators can invoke {@link #committed(long)} callback of {@link FSWindowDataManager} to trigger deletion of stale
 * artifacts.<br/>
 * <p/>
 *
 * <b>Dynamic partitioning support provided</b><br/>
 * An operator can call {@link #partition(int, Set)} to get new instances of {@link FSWindowDataManager} during
 * re-partitioning. When operator partitions are removed, then one of the new instances will handle the state of
 * all deleted instances.<br/>
 * After re-partitioning, the largest completed window is the min of max completed windows across all partitions.</br>
 *
 * <p/>
 * At times, after re-partitioning, a physical operator may want to read the data saved by all the partitions for a
 * completed window id. For example,  {@link AbstractFileInputOperator}, needs to redistribute files based on the hash
 * of file-paths and its partition keys, so it reads artifacts saved by all partitions during replay of a completed
 * window. {@link #retrieveAllPartitions(long)} retrieves the artifacts of all partitions wrt a completed window.
 *
 *
 * @since 3.4.0
 */
public class FSWindowDataManager extends AbstractFSWindowStateManager implements WindowDataManager
{
  private static final String DEF_STATE_PATH = "idempotentState";

  public FSWindowDataManager()
  {
    super();
    setStatePath(DEF_STATE_PATH);
  }

  @Override
  public long getLargestCompletedWindow()
  {
    return largestCompletedWindow;
  }

  /**
   * Save writes 2 entries to the wal: <br/>
   * <ol>
   *   <li>window id</li>
   *   <li>artifact</li>
   * </ol>
   * Note: The wal is being used in batch mode so the part file will never be rotated between the 2 entries.<br/>
   * The wal part file may be rotated after both the entries, when
   * {@link FileSystemWAL.FileSystemWALWriter#rotateIfNecessary()} is triggered.
   *
   * @param object    state
   * @param windowId  window id
   * @throws IOException
   */
  @Override
  public void save(Object object, long windowId) throws IOException
  {
    closeReadersIfNecessary();
    FileSystemWAL.FileSystemWALWriter writer = wal.getWriter();

    appendWindowId(writer, windowId);
    writer.append(toSlice(object));
    wal.beforeCheckpoint(windowId);
    wal.windowWalParts.put(windowId, writer.getCurrentPointer().getPartNum());
    writer.rotateIfNecessary();
  }

  /**
   * The implementation assumes that artifacts are retrieved in increasing order of window ids. Typically it is used
   * to replay tuples of successive windows in input operators after failure.
   * @param windowId      window id
   * @return saved state for the window id.
   * @throws IOException
   */
  @Override
  public Object retrieve(long windowId) throws IOException
  {
    return retrieve(wal, windowId);
  }

  @Override
  public Map<Integer, Object> retrieveAllPartitions(long windowId) throws IOException
  {
    if (windowId > largestCompletedWindow) {
      return null;
    }
    Map<Integer, Object> artifacts = Maps.newHashMap();
    Object artifact = retrieve(wal, windowId);
    if (artifact != null) {
      artifacts.put(operatorId, artifact);
    }
    if (repartitioned) {
      for (Map.Entry<Integer, FSWindowReplayWAL> entry : readOnlyWals.entrySet()) {
        artifact = retrieve(entry.getValue(), windowId);
        if (artifact != null) {
          artifacts.put(entry.getKey(), artifact);
        }
      }
    }
    return artifacts;
  }

  private Object retrieve(FSWindowReplayWAL wal, long windowId) throws IOException
  {
    if (windowId > largestCompletedWindow || wal.walEndPointerAfterRecovery == null) {
      return null;
    }

    FileSystemWAL.FileSystemWALReader reader = wal.getReader();

    while (reader.getCurrentPointer() == null ||
        reader.getCurrentPointer().compareTo(wal.walEndPointerAfterRecovery) < 0) {
      long currentWindow;

      if (wal.retrievedWindow == null) {
        wal.retrievedWindow = readNext(reader);
        Preconditions.checkNotNull(wal.retrievedWindow);
      }
      currentWindow = Longs.fromByteArray(wal.retrievedWindow.toByteArray());

      if (windowId == currentWindow) {
        Slice data = readNext(reader);
        Preconditions.checkNotNull(data, "data is null");

        wal.windowWalParts.put(currentWindow, reader.getCurrentPointer().getPartNum());
        wal.retrievedWindow = readNext(reader); //null or next window

        return fromSlice(data);
      } else if (windowId < currentWindow) {
        //no artifact saved corresponding to that window and artifact is not read.
        return null;
      } else {
        //windowId > current window so we skip the data
        skipNext(reader);
        wal.windowWalParts.put(currentWindow, reader.getCurrentPointer().getPartNum());

        wal.retrievedWindow = readNext(reader); //null or next window
        if (wal.retrievedWindow == null) {
          //nothing else to read
          return null;
        }
      }
    }
    return null;
  }

  /**
   * Deletes artifacts for all windows less than equal to committed window id.<p/>
   *
   * {@link FSWindowDataManager} uses {@link FSWindowReplayWAL} to record data which writes to temp part files.
   * The temp part files are finalized only when they are rotated. So when a window is committed, artifacts for
   * windows <= committed window may still be in temporary files. These temporary files are needed for Wal recovery so
   * we do not alter them and we delete a part file completely (opposed to partial deletion) for efficiency.<br/>
   * Therefore, data of a window gets deleted only when it satisfies all the following criteria:
   * <ul>
   *   <li>window <= committed window id</li>
   *   <li>the part file of the artifact is rotated.</li>
   *   <li>the part file doesn't contain artifacts for windows greater than the artifact's window to avoid partial
   *   file deletion.</li>
   * </ul>
   *
   * In addition to this we also delete:
   * <ol>
   *   <li>Some stray temporary files are also deleted which correspond to completely deleted parts.</li>
   *   <li>Once the committed window > largest recovery window, we delete the files of partitions that were removed.</li>
   * </ol>
   *
   * @param committedWindowId   window id
   * @throws IOException
   */
  @Override
  public void committed(long committedWindowId) throws IOException
  {
    closeReadersIfNecessary();
    //find the largest window <= committed window id and the part file corresponding to it is finalized.
    Map.Entry<Long, Integer> largestEntryForDeletion = null;

    Iterator<Map.Entry<Long, Integer>> iterator = wal.windowWalParts.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<Long, Integer> entry = iterator.next();
      //only completely finalized part files are deleted.
      if (entry.getKey() <= committedWindowId && !wal.tempPartFiles.containsKey(entry.getValue())) {
        largestEntryForDeletion = entry;
        iterator.remove();
      }
      if (entry.getKey() > committedWindowId) {
        break;
      }
    }

    if (largestEntryForDeletion != null && !wal.windowWalParts.containsValue(
        largestEntryForDeletion.getValue()) /* no artifacts for higher window present*/) {

      int highestPartToDelete = largestEntryForDeletion.getValue();
      wal.getWriter().delete(new FileSystemWAL.FileSystemWALPointer(highestPartToDelete + 1, 0));

      //also delete any old stray temp files that correspond to parts < deleteTillPointer.partNum
      Iterator<Map.Entry<Integer, FSWindowReplayWAL.FileDescriptor>> fileIterator =
          wal.fileDescriptors.entries().iterator();
      while (fileIterator.hasNext()) {
        Map.Entry<Integer, FSWindowReplayWAL.FileDescriptor> entry = fileIterator.next();
        if (entry.getKey() <= highestPartToDelete && entry.getValue().isTmp()) {
          if (fileContext.util().exists(entry.getValue().getFilePath())) {
            fileContext.delete(entry.getValue().getFilePath(), true);
          }
        } else if (entry.getKey() > highestPartToDelete) {
          break;
        }
      }
    }

    //delete data of partitions that have been removed
    if (deletedOperators != null) {
      Iterator<Integer> operatorIter = deletedOperators.iterator();

      while (operatorIter.hasNext()) {
        int deletedOperatorId = operatorIter.next();
        FSWindowReplayWAL wal = readOnlyWals.get(deletedOperatorId);
        if (committedWindowId > largestCompletedWindow) {
          Path operatorDir = new Path(fullStatePath + Path.SEPARATOR + deletedOperatorId);

          if (fileContext.util().exists(operatorDir)) {
            fileContext.delete(operatorDir, true);
          }
          wal.teardown();
          operatorIter.remove();
          readOnlyWals.remove(deletedOperatorId);
        }
      }

      if (deletedOperators.isEmpty()) {
        deletedOperators = null;
      }
    }
  }

  @Override
  public List<WindowDataManager> partition(int newCount, Set<Integer> removedOperatorIds)
  {
    List<WindowDataManager> partitions = new ArrayList<>();
    List<FSWindowDataManager> fsWindowDataManagers = createPartitions(newCount, removedOperatorIds);
    partitions.addAll(fsWindowDataManagers);

    return partitions;
  }

  private static Logger LOG = LoggerFactory.getLogger(FSWindowDataManager.class);
}
