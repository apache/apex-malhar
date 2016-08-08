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

import java.io.ByteArrayOutputStream;
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

import org.apache.apex.malhar.lib.state.managed.IncrementalCheckpointManager;
import org.apache.apex.malhar.lib.utils.FileContextUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.lib.util.KryoCloneUtils;
import com.datatorrent.netlet.util.Slice;

/**
 * An {@link WindowDataManager} that uses FS to persist state.
 *
 * @since 3.4.0
 */
public class FSWindowDataManager implements WindowDataManager
{
  private static final String DEF_RECOVERY_PATH = "idempotentState";
  private static final String WAL_FILE_NAME = "wal";

  /**
   * Recovery filePath relative to app filePath where state is saved.
   */
  @NotNull
  private String recoveryPath = DEF_RECOVERY_PATH;

  private boolean isRecoveryPathRelativeToAppPath = true;

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

  /**
   * largest window for which there is recovery data across all physical operator instances.
   */
  private transient long largestRecoveryWindow = Stateless.WINDOW_ID;

  private final FSWindowReplayWAL wal = new FSWindowReplayWAL();

  //operator id -> wals (sorted)
  private final transient Map<Integer, FSWindowReplayWAL> readOnlyWals = new HashMap<>();

  private transient String statePath;
  private transient int operatorId;

  private final transient Kryo kryo = new Kryo();

  private transient FileContext fileContext;

  public FSWindowDataManager()
  {
    kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    operatorId = context.getId();

    if (isRecoveryPathRelativeToAppPath) {
      statePath = context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + recoveryPath;
    } else {
      statePath = recoveryPath;
    }

    try {
      fileContext = FileContextUtils.getFileContext(statePath);
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

    //find largest recovery window
    if (!relyOnCheckpoints) {
      long recoveryWindow = findLargestRecoveryWindow(wal, null);
      //committed will not delete temp files so it is possible that when reading from files, a smaller window
      //than the activation window is found.
      if (recoveryWindow > activationWindow) {
        largestRecoveryWindow = recoveryWindow;
      }
      if (wal.getReader().getCurrentPointer() != null) {
        wal.getWriter().setCurrentPointer(wal.getReader().getCurrentPointer().getCopy());
      }
    } else {
      wal.walEndPointerAfterRecovery = wal.getWriter().getCurrentPointer();
      largestRecoveryWindow = wal.getLastCheckpointedWindow();
    }

    if (repartitioned && largestRecoveryWindow > Stateless.WINDOW_ID) {
      //find the min of max window ids: a downstream will not finish a window until all the upstream have finished it.
      for (Map.Entry<Integer, FSWindowReplayWAL> entry : readOnlyWals.entrySet()) {

        long recoveryWindow = Stateless.WINDOW_ID;
        if (!relyOnCheckpoints) {
          long window = findLargestRecoveryWindow(entry.getValue(), null);
          if (window > activationWindow) {
            recoveryWindow = window;
          }
        } else {
          recoveryWindow = findLargestRecoveryWindow(entry.getValue(), activationWindow);
        }

        if (recoveryWindow < largestRecoveryWindow) {
          largestRecoveryWindow = recoveryWindow;
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
    RemoteIterator<FileStatus> operatorsIter = fileContext.listStatus(new Path(statePath));
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

  private void configureWal(FSWindowReplayWAL wal, int operatorId, boolean updateWalState) throws IOException
  {
    String operatorDir = statePath + Path.SEPARATOR + operatorId;
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
    String operatorDir = statePath + Path.SEPARATOR + operatorId;
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

  private long findLargestRecoveryWindow(FSWindowReplayWAL wal, Long ceilingWindow) throws IOException
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
          boolean skipComplete = skipNext(reader);
          if (!skipComplete) {
            //artifact not saved so this window was not finished.
            break;
          }
          Slice windowSlice = slice;
          long offset = reader.getCurrentPointer().getOffset();
          slice = readNext(reader);  //either null, deleted or next window

          if (slice == null || !slice.equals(DELETED)) {
            //delete entry not found so window was not deleted
            long window = Longs.fromByteArray(windowSlice.toByteArray());

            if (ceilingWindow != null && window > ceilingWindow) {
              break;
            }
            endOffset = offset;
            lastWindow = window;
          } else {
            slice = readNext(reader);
          }
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
  private Slice readNext(FileSystemWAL.FileSystemWALReader reader)
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

  @Override
  public void save(Object object, long windowId) throws IOException
  {
    closeReaders();
    FileSystemWAL.FileSystemWALWriter writer = wal.getWriter();
    writer.batchCompleted(); //finish previous batch and rotate if needed

    byte[] windowIdBytes = Longs.toByteArray(windowId);
    writer.append(new Slice(windowIdBytes));
    writer.append(toSlice(object));
    wal.beforeCheckpoint(windowId);
    wal.windowWalParts.put(windowId, writer.getCurrentPointer().getPartNum());
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
    if (windowId > largestRecoveryWindow) {
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
    if (windowId > largestRecoveryWindow || wal.walEndPointerAfterRecovery == null) {
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

        Slice nextSlice = readNext(reader); //null, deleted or next window

        if (nextSlice == null) {
          // the current window is not deleted
          wal.retrievedWindow = null;
          return fromSlice(data);
        } else if (nextSlice.equals(DELETED)) {
          //current window is deleted;
          wal.retrievedWindow = null;
          //window end is after deleted
          return null;
        } else {
          //next window
          wal.retrievedWindow = nextSlice;
          return fromSlice(data);
        }
      } else if (windowId < currentWindow) {
        //no artifact saved corresponding to that window and artifact is not read.
        return null;
      } else {
        //windowId > current window so we skip the data
        skipNext(reader);
        wal.windowWalParts.put(currentWindow, reader.getCurrentPointer().getPartNum());

        Slice nextSlice = readNext(reader); //null, deleted or next window

        if (nextSlice == null) {
          //nothing else to read
          wal.retrievedWindow = null;
          return null;
        } else if (nextSlice.equals(DELETED)) {
          wal.retrievedWindow = null;
        } else {
          wal.retrievedWindow = nextSlice;
        }
      }
    }
    return null;
  }

  public Map<Long, Object> retrieveAllWindows() throws IOException
  {
    Map<Long, Object> artifactPerWindow = new HashMap<>();
    FileSystemWAL.FileSystemWALReader reader = wal.getReader();
    reader.seek(wal.walStartPointer);

    Slice windowSlice = readNext(reader);
    while (reader.getCurrentPointer().compareTo(wal.walEndPointerAfterRecovery) < 0 && windowSlice != null) {
      long window = Longs.fromByteArray(windowSlice.toByteArray());
      Object data = fromSlice(readNext(reader));
      windowSlice = readNext(reader); //null, deleted or next window
      if (windowSlice == DELETED) {
        windowSlice = readNext(reader);
      } else {
        artifactPerWindow.put(window, data);
      }
    }
    reader.seek(wal.walStartPointer);
    return artifactPerWindow;
  }

  @Override
  public void deleteLastWindow() throws IOException
  {
    wal.getWriter().append(DELETED);
    wal.getWriter().flush();
  }

  @Override
  public void committed(long windowId) throws IOException
  {
    closeReaders();
    Map.Entry<Long, Integer> largestEntryForDeletion = null;
    Iterator<Map.Entry<Long, Integer>> iterator = wal.windowWalParts.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, Integer> entry = iterator.next();
      //only completely finalized part files are deleted.
      if (entry.getKey() <= windowId && !wal.tempPartFiles.containsKey(entry.getValue())) {
        largestEntryForDeletion = entry;
        iterator.remove();
      }
      if (entry.getKey() > windowId) {
        break;
      }
    }
    if (largestEntryForDeletion != null) {

      int highestPartToDelete = largestEntryForDeletion.getValue();
      wal.getWriter().delete(new FileSystemWAL.FileSystemWALPointer(highestPartToDelete + 1, 0));

      //also delete any old stray temp files
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
    wal.committed(windowId);

    //delete data of partitions that have been removed
    if (deletedOperators != null) {
      Iterator<Integer> operatorIter = deletedOperators.iterator();

      while (operatorIter.hasNext()) {
        int deletedOperatorId = operatorIter.next();
        FSWindowReplayWAL wal = readOnlyWals.get(deletedOperatorId);
        if (windowId > largestRecoveryWindow) {
          Path operatorDir = new Path(statePath + Path.SEPARATOR + deletedOperatorId);

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
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    kryo.writeClassAndObject(output, object);
    output.close();
    byte[] bytes = baos.toByteArray();

    return new Slice(bytes);
  }

  private Object fromSlice(Slice slice)
  {
    Input input = new Input(slice.buffer, slice.offset, slice.length);
    Object object = kryo.readClassAndObject(input);
    input.close();
    return object;
  }

  @Override
  public long getLargestRecoveryWindow()
  {
    return largestRecoveryWindow;
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
  public String getRecoveryPath()
  {
    return recoveryPath;
  }

  /**
   * Sets the recovery filePath. If {@link #isRecoveryPathRelativeToAppPath} is true then this filePath is handled
   * relative
   * to the application filePath; otherwise it is handled as an absolute filePath.
   *
   * @param recoveryPath recovery filePath
   */
  public void setRecoveryPath(String recoveryPath)
  {
    this.recoveryPath = recoveryPath;
  }

  /**
   * @return true if recovery filePath is relative to app filePath; false otherwise.
   */
  public boolean isRecoveryPathRelativeToAppPath()
  {
    return isRecoveryPathRelativeToAppPath;
  }

  /**
   * Specifies whether the recovery filePath is relative to application filePath.
   *
   * @param recoveryPathRelativeToAppPath true if recovery filePath is relative to application filePath; false
   *                                      otherwise.
   */
  public void setRecoveryPathRelativeToAppPath(boolean recoveryPathRelativeToAppPath)
  {
    isRecoveryPathRelativeToAppPath = recoveryPathRelativeToAppPath;
  }

  private static Slice DELETED = new Slice("DEL".getBytes());

  private static Logger LOG = LoggerFactory.getLogger(FSWindowDataManager.class);
}
