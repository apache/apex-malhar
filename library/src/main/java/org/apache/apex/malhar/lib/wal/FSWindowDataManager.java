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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.io.fs.AbstractFileInputOperator;
import org.apache.apex.malhar.lib.state.managed.IncrementalCheckpointManager;
import org.apache.apex.malhar.lib.util.KryoCloneUtils;
import org.apache.apex.malhar.lib.utils.FileContextUtils;
import org.apache.apex.malhar.lib.utils.serde.SerializationBuffer;
import org.apache.apex.malhar.lib.utils.serde.WindowedBlockStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.Stateless;
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
 * {@link IncrementalCheckpointManager}, however, relies only on the checkpointed state and therefore sets
 * {@link #relyOnCheckpoints} to true. This is because {@link IncrementalCheckpointManager} only saves data per
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
public class FSWindowDataManager implements WindowDataManager
{
  private static final String DEF_STATE_PATH = "idempotentState";
  private static final String WAL_FILE_NAME = "wal";

  /**
   * State path relative to app filePath where state is saved.
   */
  @NotNull
  private String statePath = DEF_STATE_PATH;

  private boolean isStatePathRelativeToAppPath = true;

  /**
   * This is not null only for one physical instance.<br/>
   * It consists of operator ids which have been deleted but have some state that can be replayed.
   * Only one of the instances would be handling (modifying) the files that belong to this state. <br/>
   * The value is assigned during partitioning.
   */
  private Set<Integer> deletedOperators;

  private boolean repartitioned;

  /**
   * Used when it is not necessary to replay every streaming/app window.
   * Used by {@link IncrementalCheckpointManager}
   */
  private boolean relyOnCheckpoints;

  private transient long largestCompletedWindow = Stateless.WINDOW_ID;

  private final FSWindowReplayWAL wal = new FSWindowReplayWAL();

  //operator id -> wals (sorted)
  private final transient Map<Integer, FSWindowReplayWAL> readOnlyWals = new HashMap<>();

  private transient String fullStatePath;
  private transient int operatorId;

  private final transient Kryo kryo = new Kryo();

  private transient FileContext fileContext;

  private transient SerializationBuffer serializationBuffer;

  public FSWindowDataManager()
  {
    kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    serializationBuffer = new SerializationBuffer(new WindowedBlockStream());
    operatorId = context.getId();

    if (isStatePathRelativeToAppPath) {
      fullStatePath = context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + statePath;
    } else {
      fullStatePath = statePath;
    }

    try {
      fileContext = FileContextUtils.getFileContext(fullStatePath);
      setupWals(context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void setupWals(long activationWindow) throws IOException
  {
    findFiles(wal, operatorId);
    configureWal(wal, operatorId, !relyOnCheckpoints);

    if (repartitioned) {
      createReadOnlyWals();
      for (Map.Entry<Integer, FSWindowReplayWAL> entry : readOnlyWals.entrySet()) {
        findFiles(entry.getValue(), entry.getKey());
        configureWal(entry.getValue(), entry.getKey(), true);
      }
    }

    //find largest completed window
    if (!relyOnCheckpoints) {
      long completedWindow = findLargestCompletedWindow(wal, null);
      //committed will not delete temp files so it is possible that when reading from files, a smaller window
      //than the activation window is found.
      if (completedWindow > activationWindow) {
        largestCompletedWindow = completedWindow;
      }
      if (wal.getReader().getCurrentPointer() != null) {
        wal.getWriter().setCurrentPointer(wal.getReader().getCurrentPointer().getCopy());
      }
    } else {
      wal.walEndPointerAfterRecovery = wal.getWriter().getCurrentPointer();
      largestCompletedWindow = wal.getLastCheckpointedWindow();
    }

    if (repartitioned && largestCompletedWindow > Stateless.WINDOW_ID) {
      //find the min of max window ids: a downstream will not finish a window until all the upstream have finished it.
      for (Map.Entry<Integer, FSWindowReplayWAL> entry : readOnlyWals.entrySet()) {

        long completedWindow = Stateless.WINDOW_ID;
        if (!relyOnCheckpoints) {
          long window = findLargestCompletedWindow(entry.getValue(), null);
          if (window > activationWindow) {
            completedWindow = window;
          }
        } else {
          completedWindow = findLargestCompletedWindow(entry.getValue(), activationWindow);
        }

        if (completedWindow < largestCompletedWindow) {
          largestCompletedWindow = completedWindow;
        }
      }
    }

    //reset readers
    wal.getReader().seek(wal.walStartPointer);
    for (FSWindowReplayWAL wal : readOnlyWals.values()) {
      wal.getReader().seek(wal.walStartPointer);
    }

    wal.setup();
    for (FSWindowReplayWAL wal : readOnlyWals.values()) {
      wal.setup();
    }

  }

  protected void createReadOnlyWals() throws IOException
  {
    Path statePath = new Path(fullStatePath);
    if (fileContext.util().exists(statePath)) {
      RemoteIterator<FileStatus> operatorsIter = fileContext.listStatus(statePath);
      while (operatorsIter.hasNext()) {
        FileStatus status = operatorsIter.next();
        int operatorId = Integer.parseInt(status.getPath().getName());

        if (operatorId != this.operatorId) {
          //create read-only wal for other partitions
          FSWindowReplayWAL wal = new FSWindowReplayWAL(true);
          readOnlyWals.put(operatorId, wal);
        }
      }
    }
  }

  private void configureWal(FSWindowReplayWAL wal, int operatorId, boolean updateWalState) throws IOException
  {
    String operatorDir = fullStatePath + Path.SEPARATOR + operatorId;
    wal.setFilePath(operatorDir + Path.SEPARATOR + WAL_FILE_NAME);
    wal.fileContext = fileContext;

    if (updateWalState) {
      if (!wal.fileDescriptors.isEmpty()) {
        SortedSet<Integer> sortedParts = wal.fileDescriptors.keySet();

        wal.walStartPointer = new FileSystemWAL.FileSystemWALPointer(sortedParts.first(), 0);

        FSWindowReplayWAL.FileDescriptor last = wal.fileDescriptors.get(sortedParts.last()).last();
        if (last.isTmp) {
          wal.tempPartFiles.put(last.part, last.filePath.toString());
        }
      }
    }
  }

  private void findFiles(FSWindowReplayWAL wal, int operatorId) throws IOException
  {
    String operatorDir = fullStatePath + Path.SEPARATOR + operatorId;
    Path operatorPath = new Path(operatorDir);
    if (fileContext.util().exists(operatorPath)) {
      RemoteIterator<FileStatus> walFilesIter = fileContext.listStatus(operatorPath);

      while (walFilesIter.hasNext()) {
        FileStatus fileStatus = walFilesIter.next();
        FSWindowReplayWAL.FileDescriptor descriptor = FSWindowReplayWAL.FileDescriptor.create(fileStatus.getPath());
        wal.fileDescriptors.put(descriptor.part, descriptor);
      }
    }
  }

  private long findLargestCompletedWindow(FSWindowReplayWAL wal, Long ceilingWindow) throws IOException
  {
    if (!wal.fileDescriptors.isEmpty()) {
      FileSystemWAL.FileSystemWALReader reader = wal.getReader();

      //to find the largest window, we only need to look at the last file.
      NavigableSet<Integer> descendingParts = new TreeSet<>(wal.fileDescriptors.keySet()).descendingSet();
      for (int part : descendingParts) {
        FSWindowReplayWAL.FileDescriptor last = wal.fileDescriptors.get(part).last();
        reader.seek(new FileSystemWAL.FileSystemWALPointer(last.part, 0));

        long endOffset = -1;

        long lastWindow = Stateless.WINDOW_ID;
        Slice slice = readNext(reader);

        while (slice != null) {
          boolean skipComplete = skipNext(reader); //skip the artifact because we need just the largest window id.
          if (!skipComplete) {
            //artifact not saved so this window was not finished.
            break;
          }
          long offset = reader.getCurrentPointer().getOffset();

          long window = Longs.fromByteArray(slice.toByteArray());
          if (ceilingWindow != null && window > ceilingWindow) {
            break;
          }
          endOffset = offset;
          lastWindow = window;
          slice = readNext(reader);  //either null or next window
        }

        if (endOffset != -1) {
          wal.walEndPointerAfterRecovery = new FileSystemWAL.FileSystemWALPointer(last.part, endOffset);
          wal.windowWalParts.put(lastWindow, wal.walEndPointerAfterRecovery.getPartNum());
          return lastWindow;
        }
      }
    }
    return Stateless.WINDOW_ID;
  }

  /**
   * Helper method that catches IOException while reading from wal to check if an entry was saved completely or not.
   * @param reader wal reader
   * @return wal entry
   */
  protected Slice readNext(FileSystemWAL.FileSystemWALReader reader)
  {
    try {
      return reader.next();
    } catch (IOException ex) {
      //exception while reading wal entry which can be because there may have been failure while persisting an
      //artifact so this window is not a finished window.
      try {
        reader.close();
      } catch (IOException ioe) {
        //closing the reader quietly.
      }
      return null;
    }
  }

  /**
   * Helper method that catches IOException while skipping an entry from wal to check if an entry was saved
   * completely or not.
   * @param reader wal reader
   * @return true if skip was successful; false otherwise.
   */
  private boolean skipNext(FileSystemWAL.FileSystemWALReader reader)
  {
    try {
      reader.skipNext();
      return true;
    } catch (IOException ex) {
      //exception while skipping wal entry which can be because there may have been failure while persisting an
      //artifact so this window is not a finished window.
      try {
        reader.close();
      } catch (IOException e) {
        //closing the reader quietly
      }
      return false;
    }
  }

  private void closeReaders() throws IOException
  {
    //close all reader stream and remove read-only wals
    wal.getReader().close();
    if (readOnlyWals.size() > 0) {
      Iterator<Map.Entry<Integer, FSWindowReplayWAL>> walIterator = readOnlyWals.entrySet().iterator();
      while (walIterator.hasNext()) {
        Map.Entry<Integer, FSWindowReplayWAL> entry = walIterator.next();
        entry.getValue().getReader().close();

        int operatorId = entry.getKey();
        if (deletedOperators == null || !deletedOperators.contains(operatorId)) {
          //the read only wal can be removed.
          walIterator.remove();
        }
      }
    }
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
    closeReaders();
    FileSystemWAL.FileSystemWALWriter writer = wal.getWriter();

    byte[] windowIdBytes = Longs.toByteArray(windowId);
    writer.append(new Slice(windowIdBytes));

    /**
     * writer.append() will copy the data to the file output stream.
     * So the data in the buffer is not needed any more, and it is safe to reset the serializationBuffer.
     *
     * And as the data in stream memory can be cleaned all at once. So don't need to separate data by different windows,
     * so beginWindow() and endWindow() don't need to be called
     */
    writer.append(toSlice(object));
    serializationBuffer.reset();

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
    closeReaders();
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
        if (entry.getKey() <= highestPartToDelete && entry.getValue().isTmp) {
          if (fileContext.util().exists(entry.getValue().filePath)) {
            fileContext.delete(entry.getValue().filePath, true);
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

  private Slice toSlice(Object object)
  {
    kryo.writeClassAndObject(serializationBuffer, object);
    return serializationBuffer.toSlice();
  }

  protected Object fromSlice(Slice slice)
  {
    Input input = new Input(slice.buffer, slice.offset, slice.length);
    Object object = kryo.readClassAndObject(input);
    input.close();
    return object;
  }

  public long getLargestCompletedWindow()
  {
    return largestCompletedWindow;
  }

  @Override
  public List<WindowDataManager> partition(int newCount, Set<Integer> removedOperatorIds)
  {
    repartitioned = true;

    KryoCloneUtils<FSWindowDataManager> cloneUtils = KryoCloneUtils.createCloneUtils(this);

    FSWindowDataManager[] windowDataManagers = cloneUtils.getClones(newCount);
    if (removedOperatorIds != null && !removedOperatorIds.isEmpty()) {
      windowDataManagers[0].deletedOperators = removedOperatorIds;
    }

    List<WindowDataManager> mangers = new ArrayList<>();
    mangers.addAll(Arrays.asList(windowDataManagers));
    return mangers;
  }

  @Override
  public void teardown()
  {
    wal.teardown();
    for (FSWindowReplayWAL wal : readOnlyWals.values()) {
      wal.teardown();
    }
  }

  protected void setRelyOnCheckpoints(boolean relyOnCheckpoints)
  {
    this.relyOnCheckpoints = relyOnCheckpoints;
  }

  /**
   * @return wal instance
   */
  protected FSWindowReplayWAL getWal()
  {
    return wal;
  }

  @VisibleForTesting
  public Set<Integer> getDeletedOperators()
  {
    if (deletedOperators == null) {
      return null;
    }
    return ImmutableSet.copyOf(deletedOperators);
  }

  /**
   * @return recovery filePath
   */
  public String getStatePath()
  {
    return statePath;
  }

  /**
   * Sets the state path. If {@link #isStatePathRelativeToAppPath} is true then this filePath is handled
   * relative
   * to the application filePath; otherwise it is handled as an absolute filePath.
   *
   * @param statePath recovery filePath
   */
  public void setStatePath(String statePath)
  {
    this.statePath = statePath;
  }

  /**
   * @return true if state path is relative to app path; false otherwise.
   */
  public boolean isStatePathRelativeToAppPath()
  {
    return isStatePathRelativeToAppPath;
  }

  /**
   * Specifies whether the state path is relative to application filePath.
   *
   * @param statePathRelativeToAppPath true if state path is relative to application path; false
   *                                      otherwise.
   */
  public void setStatePathRelativeToAppPath(boolean statePathRelativeToAppPath)
  {
    isStatePathRelativeToAppPath = statePathRelativeToAppPath;
  }

  private static Logger LOG = LoggerFactory.getLogger(FSWindowDataManager.class);
}
